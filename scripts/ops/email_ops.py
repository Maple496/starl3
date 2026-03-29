
import os
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.header import Header
from email.utils import formataddr
from email import encoders
from pathlib import Path
from core.pipeline_engine import PipelineEngine
from core.constants import BASE_DIR, DATA_DIR


def _get_email_config(ctx):
    """获取邮件配置（从上下文中）"""
    if "_email_config" not in ctx:
        ctx["_email_config"] = {}
    return ctx["_email_config"]


def _get_email_content(ctx):
    """获取邮件内容（从上下文中）"""
    if "_email_content" not in ctx:
        ctx["_email_content"] = {
            "subject": "",
            "body": "",
            "body_type": "text",
            "attachments": []
        }
    return ctx["_email_content"]

def op_config_email(ctx, params):
    """配置邮箱 SMTP 参数"""
    config = {
        "host": params.get("host", "smtp.qq.com"),
        "port": int(params.get("port", 465)),
        "username": params.get("username", ""),
        "password": params.get("password", ""),
        "use_ssl": params.get("use_ssl", True),
        "sender_name": params.get("sender_name", "")
    }
    ctx["_email_config"] = config
    return {"status": "configured", "config": {k: v if k != "password" else "***" for k, v in config.items()}}

def op_set_email_content(ctx, params):
    """设置邮件内容"""
    content = _get_email_content(ctx)
    content["subject"] = params.get("subject", "")
    content["body"] = params.get("body", "")
    content["body_type"] = params.get("body_type", "text")
    return {"subject": content["subject"], "body_type": content["body_type"]}

def op_add_attachment(ctx, params):
    """添加附件 - 支持从上下文自动获取"""
    email_content = _get_email_content(ctx)
    file_path = params.get("file_path", "")
    
    # 如果没有指定文件路径，尝试从上下文获取
    if not file_path:
        data_ctx = ctx.get("_data_context")
        if data_ctx and hasattr(data_ctx, 'files') and data_ctx.files:
            added_count = 0
            for file_info in data_ctx.files:
                path = file_info.get("abs_path") or file_info.get("path")
                if path and os.path.exists(path):
                    email_content["attachments"].append(path)
                    added_count += 1
            return {
                "status": "added_from_context",
                "count": added_count,
                "total_attachments": len(email_content["attachments"])
            }
        return {"status": "no_file", "message": "未指定文件路径且上下文中无文件"}
    
    # 处理单文件路径
    if isinstance(file_path, list):
        added_count = 0
        for fp in file_path:
            full_path = os.path.join(DATA_DIR, fp)
            if os.path.exists(full_path):
                email_content["attachments"].append(full_path)
                added_count += 1
        return {"status": "added", "count": added_count, "total_attachments": len(email_content["attachments"])}
    else:
        full_path = os.path.join(DATA_DIR, file_path)
        if os.path.exists(full_path):
            email_content["attachments"].append(full_path)
            return {"status": "added", "file": file_path, "total_attachments": len(email_content["attachments"])}
        else:
            return {"status": "error", "message": f"文件不存在: {file_path}"}

def op_clear_attachments(ctx, params):
    """清空附件列表"""
    email_content = _get_email_content(ctx)
    email_content["attachments"] = []
    return {"status": "cleared"}

def op_send_email(ctx, params):
    """发送邮件"""
    email_config = _get_email_config(ctx)
    email_content = _get_email_content(ctx)
    
    if not email_config.get("host") or not email_config.get("username"):
        return {"status": "error", "message": "邮箱未配置，请先调用 config_email"}
    
    # 获取收件人
    to_addresses = params.get("to", [])
    if isinstance(to_addresses, str):
        to_addresses = [addr.strip() for addr in to_addresses.split(",") if addr.strip()]
    
    if not to_addresses:
        return {"status": "error", "message": "收件人地址不能为空"}
    
    cc_addresses = params.get("cc", [])
    if isinstance(cc_addresses, str):
        cc_addresses = [addr.strip() for addr in cc_addresses.split(",") if addr.strip()]
    
    bcc_addresses = params.get("bcc", [])
    if isinstance(bcc_addresses, str):
        bcc_addresses = [addr.strip() for addr in bcc_addresses.split(",") if addr.strip()]
    
    try:
        # 创建邮件对象
        msg = MIMEMultipart()
        sender = email_config.get("username", "")
        sender_name = email_config.get("sender_name", "")
        
        if sender_name:
            # 使用 Header 编码中文显示名
            msg['From'] = formataddr((Header(sender_name, 'utf-8').encode(), sender))
        else:
            msg['From'] = sender
        msg['To'] = ', '.join(to_addresses)
        if cc_addresses:
            msg['Cc'] = ', '.join(cc_addresses)
        # Subject 使用 Header 编码中文
        subject = email_content.get("subject", "")
        msg['Subject'] = Header(subject, 'utf-8')
        
        # 添加正文
        body = email_content.get("body", "")
        body_type = email_content.get("body_type", "text")
        
        if body_type == "html":
            msg.attach(MIMEText(body, 'html', 'utf-8'))
        else:
            msg.attach(MIMEText(body, 'plain', 'utf-8'))
        
        # 添加附件
        for file_path in email_content.get("attachments", []):
            if os.path.exists(file_path):
                with open(file_path, 'rb') as f:
                    attachment = MIMEBase('application', 'octet-stream')
                    attachment.set_payload(f.read())
                encoders.encode_base64(attachment)
                filename = Path(file_path).name
                # 修复：使用正确的方式设置 Content-Disposition 头
                attachment.add_header('Content-Disposition', f'attachment; filename="{filename}"')
                msg.attach(attachment)
        
        # 连接 SMTP 服务器并发送
        all_recipients = to_addresses + cc_addresses + bcc_addresses
        
        if email_config.get("use_ssl", True):
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(email_config["host"], email_config["port"], context=context) as server:
                server.login(email_config["username"], email_config["password"])
                server.sendmail(sender, all_recipients, msg.as_string())
        else:
            with smtplib.SMTP(email_config["host"], email_config["port"]) as server:
                server.starttls()
                server.login(email_config["username"], email_config["password"])
                server.sendmail(sender, all_recipients, msg.as_string())
        
        result = {
            "status": "success",
            "to": to_addresses,
            "cc": cc_addresses,
            "bcc": bcc_addresses,
            "subject": email_content.get("subject", ""),
            "attachments_count": len(email_content.get("attachments", []))
        }
        
        # 发送成功后清空附件（可选）
        if params.get("clear_after_send", True):
            email_content["attachments"] = []
        
        return result
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

def op_send_simple_email(ctx, params):
    """一键发送邮件（简化版，包含所有参数）"""
    # 配置
    config_result = op_config_email(ctx, {
        "host": params.get("smtp_host", "smtp.qq.com"),
        "port": params.get("smtp_port", 465),
        "username": params.get("username", ""),
        "password": params.get("password", ""),
        "use_ssl": params.get("use_ssl", True),
        "sender_name": params.get("sender_name", "")
    })
    
    if config_result.get("status") == "error":
        return config_result
    
    # 设置内容
    op_set_email_content(ctx, {
        "subject": params.get("subject", ""),
        "body": params.get("body", ""),
        "body_type": params.get("body_type", "text")
    })
    
    # 添加附件
    attachments = params.get("attachments", [])
    if isinstance(attachments, str):
        attachments = [a.strip() for a in attachments.split(",") if a.strip()]
    
    for att in attachments:
        op_add_attachment(ctx, {"file_path": att})
    
    # 发送
    return op_send_email(ctx, {
        "to": params.get("to", []),
        "cc": params.get("cc", []),
        "bcc": params.get("bcc", []),
        "clear_after_send": params.get("clear_after_send", True)
    })

OP_MAP = {
    "config_email": op_config_email,
    "set_email_content": op_set_email_content,
    "add_attachment": op_add_attachment,
    "clear_attachments": op_clear_attachments,
    "send_email": op_send_email,
    "send_simple_email": op_send_simple_email,
}

def run(config_path=None):
    PipelineEngine.main(OP_MAP, cfg=config_path, init_ctx=lambda: {"base_dir": DATA_DIR})

if __name__ == '__main__':
    run()
