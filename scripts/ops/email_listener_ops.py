"""
邮件监听模块
提供邮箱监控功能，当收到满足条件的邮件时触发执行
"""

import os
import json
import time
import imaplib
import email
import threading
from typing import Dict, List, Optional, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict

from core.pipeline_engine import PipelineEngine
from core.constants import BASE_DIR, DATA_DIR
from core.logger import get_logger
from core.registry import op

logger = get_logger("email_listener_ops")

# ==================== 全局邮件监听状态 ====================

class EmailListenerState:
    """邮件监听状态管理（单例模式）"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init()
        return cls._instance
    
    def _init(self):
        self.is_running = False
        self.connections: Dict[str, 'EmailConnection'] = {}
        self.monitors: List['EmailMonitor'] = []
        self.thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
    
    def start(self):
        """启动所有监听"""
        if self.is_running:
            logger.info("邮件监听已在运行")
            return
        
        self.is_running = True
        self.stop_event.clear()
        
        # 启动所有监控器
        for monitor in self.monitors:
            monitor.start()
        
        logger.info(f"邮件监听已启动，共 {len(self.monitors)} 个监控任务")
    
    def stop(self):
        """停止所有监听"""
        if not self.is_running:
            return
        
        self.is_running = False
        self.stop_event.set()
        
        # 停止所有监控器
        for monitor in self.monitors:
            monitor.stop()
        
        # 关闭所有连接
        for conn in self.connections.values():
            conn.close()
        
        logger.info("邮件监听已停止")
    
    def add_monitor(self, monitor: 'EmailMonitor'):
        """添加监控器"""
        self.monitors.append(monitor)
    
    def wait(self, timeout: Optional[float] = None):
        """等待运行"""
        if timeout:
            self.stop_event.wait(timeout)
        else:
            while self.is_running:
                time.sleep(1)


email_listener_state = EmailListenerState()


# ==================== 邮件连接管理 ====================

class EmailConnection:
    """邮箱连接封装"""
    
    def __init__(self, config_name: str, imap_server: str, imap_port: int,
                 username: str, password: str, use_ssl: bool = True):
        self.config_name = config_name
        self.imap_server = imap_server
        self.imap_port = imap_port
        self.username = username
        self.password = password
        self.use_ssl = use_ssl
        self.conn: Optional[imaplib.IMAP4] = None
        self.last_connect_time = None
        self.is_connected = False
    
    def connect(self) -> bool:
        """连接邮箱"""
        try:
            if self.use_ssl:
                self.conn = imaplib.IMAP4_SSL(self.imap_server, self.imap_port)
            else:
                self.conn = imaplib.IMAP4(self.imap_server, self.imap_port)
            
            self.conn.login(self.username, self.password)
            self.is_connected = True
            self.last_connect_time = datetime.now()
            logger.info(f"邮箱连接成功: {self.username}")
            return True
            
        except Exception as e:
            logger.error(f"邮箱连接失败: {e}")
            self.is_connected = False
            return False
    
    def ensure_connected(self) -> bool:
        """确保连接可用"""
        if not self.is_connected or not self.conn:
            return self.connect()
        
        # 检查连接是否还存活（超过30分钟重连）
        if self.last_connect_time and \
           (datetime.now() - self.last_connect_time).seconds > 1800:
            try:
                self.conn.noop()
            except:
                logger.info("邮箱连接超时，重新连接")
                return self.connect()
        
        return True
    
    def search_emails(self, criteria: str = "UNSEEN") -> List[str]:
        """搜索邮件"""
        if not self.ensure_connected():
            return []
        
        try:
            self.conn.select("INBOX")
            _, data = self.conn.search(None, criteria)
            email_ids = data[0].split()
            return [eid.decode() for eid in email_ids]
        except Exception as e:
            logger.error(f"搜索邮件失败: {e}")
            return []
    
    def fetch_email(self, email_id: str) -> Optional[Dict]:
        """获取邮件详情"""
        if not self.ensure_connected():
            return None
        
        try:
            _, msg_data = self.conn.fetch(email_id, "(RFC822)")
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)
            
            # 解析邮件内容
            result = {
                "id": email_id,
                "subject": self._decode_header(msg.get("Subject", "")),
                "from": self._decode_header(msg.get("From", "")),
                "to": self._decode_header(msg.get("To", "")),
                "date": msg.get("Date", ""),
                "body_text": "",
                "body_html": "",
                "attachments": []
            }
            
            # 提取正文
            if msg.is_multipart():
                for part in msg.walk():
                    content_type = part.get_content_type()
                    content_disposition = str(part.get("Content-Disposition", ""))
                    
                    if "attachment" in content_disposition:
                        # 附件
                        filename = part.get_filename()
                        if filename:
                            result["attachments"].append({
                                "filename": filename,
                                "content": part.get_payload(decode=True)
                            })
                    elif content_type == "text/plain":
                        result["body_text"] = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                    elif content_type == "text/html":
                        result["body_html"] = part.get_payload(decode=True).decode("utf-8", errors="ignore")
            else:
                content_type = msg.get_content_type()
                if content_type == "text/plain":
                    result["body_text"] = msg.get_payload(decode=True).decode("utf-8", errors="ignore")
                elif content_type == "text/html":
                    result["body_html"] = msg.get_payload(decode=True).decode("utf-8", errors="ignore")
            
            return result
            
        except Exception as e:
            logger.error(f"获取邮件失败: {e}")
            return None
    
    def mark_as_seen(self, email_id: str):
        """标记为已读"""
        if self.ensure_connected():
            try:
                self.conn.store(email_id, "+FLAGS", "\\Seen")
            except Exception as e:
                logger.error(f"标记已读失败: {e}")
    
    def _decode_header(self, header: str) -> str:
        """解码邮件头"""
        try:
            decoded_parts = email.header.decode_header(header)
            result = ""
            for part, charset in decoded_parts:
                if isinstance(part, bytes):
                    result += part.decode(charset or "utf-8", errors="ignore")
                else:
                    result += part
            return result
        except:
            return header
    
    def close(self):
        """关闭连接"""
        if self.conn:
            try:
                self.conn.close()
                self.conn.logout()
            except:
                pass
        self.is_connected = False


# ==================== 邮件监控器 ====================

class EmailMonitor:
    """邮件监控器"""
    
    def __init__(self, connection: EmailConnection, config_file: str, 
                 base_dir: str, filters: Dict, check_interval: int = 60,
                 mark_as_seen: bool = True, max_emails_per_check: int = 10):
        self.connection = connection
        self.config_file = config_file
        self.base_dir = base_dir
        self.filters = filters
        self.check_interval = check_interval
        self.mark_as_seen = mark_as_seen
        self.max_emails_per_check = max_emails_per_check
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self.processed_ids: set = set()  # 防重复处理
    
    def start(self):
        """启动监控"""
        self.is_running = True
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
        logger.info(f"邮件监控启动: {self.connection.username} -> {self.config_file}")
    
    def stop(self):
        """停止监控"""
        self.is_running = False
    
    def _monitor_loop(self):
        """监控循环"""
        while self.is_running and email_listener_state.is_running:
            try:
                self._check_emails()
            except Exception as e:
                logger.error(f"邮件检查出错: {e}")
            
            # 等待下一次检查
            for _ in range(self.check_interval):
                if not self.is_running or not email_listener_state.is_running:
                    break
                time.sleep(1)
    
    def _check_emails(self):
        """检查新邮件"""
        # 搜索未读邮件
        email_ids = self.connection.search_emails("UNSEEN")
        
        if not email_ids:
            return
        
        logger.info(f"发现 {len(email_ids)} 封新邮件")
        
        # 限制每次处理数量
        email_ids = email_ids[:self.max_emails_per_check]
        
        for email_id in email_ids:
            # 防重复
            if email_id in self.processed_ids:
                continue
            
            # 获取邮件详情
            email_data = self.connection.fetch_email(email_id)
            if not email_data:
                continue
            
            # 检查过滤条件
            if not self._match_filters(email_data):
                continue
            
            logger.info(f"【触发】邮件匹配: {email_data.get('subject', '无主题')}")
            
            # 在新线程中执行 pipeline
            threading.Thread(
                target=self._run_pipeline,
                args=(email_data,),
                daemon=True
            ).start()
            
            self.processed_ids.add(email_id)
            
            # 标记为已读
            if self.mark_as_seen:
                self.connection.mark_as_seen(email_id)
    
    def _match_filters(self, email_data: Dict) -> bool:
        """检查邮件是否满足过滤条件"""
        if not self.filters:
            return True
        
        # 检查发件人
        if "from_contains" in self.filters:
            if self.filters["from_contains"] not in email_data.get("from", ""):
                return False
        
        if "from_regex" in self.filters:
            import re
            if not re.search(self.filters["from_regex"], email_data.get("from", "")):
                return False
        
        # 检查主题
        if "subject_contains" in self.filters:
            if self.filters["subject_contains"] not in email_data.get("subject", ""):
                return False
        
        if "subject_regex" in self.filters:
            import re
            if not re.search(self.filters["subject_regex"], email_data.get("subject", "")):
                return False
        
        # 检查内容
        if "body_contains" in self.filters:
            body = email_data.get("body_text", "") + email_data.get("body_html", "")
            if self.filters["body_contains"] not in body:
                return False
        
        if "body_regex" in self.filters:
            import re
            body = email_data.get("body_text", "") + email_data.get("body_html", "")
            if not re.search(self.filters["body_regex"], body):
                return False
        
        return True
    
    def _run_pipeline(self, email_data: Dict):
        """执行 pipeline"""
        try:
            from main_starl3 import run_pipeline
            
            # 构建触发上下文
            trigger_ctx = {
                "trigger_type": "email",
                "trigger_time": datetime.now().isoformat(),
                "email_id": email_data.get("id"),
                "email_subject": email_data.get("subject"),
                "email_from": email_data.get("from"),
                "email_to": email_data.get("to"),
                "email_date": email_data.get("date"),
                "email_body_text": email_data.get("body_text"),
                "email_body_html": email_data.get("body_html"),
                "email_attachments": [a["filename"] for a in email_data.get("attachments", [])]
            }
            
            logger.info(f"【执行】开始运行: {self.config_file}")
            result = run_pipeline(self.config_file, self.base_dir, trigger_ctx=trigger_ctx)
            logger.info(f"【完成】{self.config_file} 执行成功")
            
        except Exception as e:
            logger.error(f"【失败】{self.config_file} 执行出错: {e}")


# ==================== 邮件监听操作 ====================

@op("email_listen_start", category="email_listener", description="启动邮箱监听")
def op_email_listen_start(ctx, params):
    """
    启动邮箱监听，当收到满足条件的邮件时触发执行
    
    参数:
        # 邮箱配置
        imap_server: str — IMAP 服务器地址，如 "imap.qq.com"
        imap_port: int — IMAP 端口，默认 993 (SSL) 或 143
        username: str — 邮箱账号
        password: str — 邮箱密码或授权码
        use_ssl: bool — 是否使用 SSL，默认 true
        
        # 触发配置
        config_file: str — 收到邮件时执行的 pipeline 配置文件（必填）
        
        # 过滤条件
        filters: dict — 邮件过滤条件
            - from_contains: str — 发件人包含
            - from_regex: str — 发件人正则匹配
            - subject_contains: str — 主题包含
            - subject_regex: str — 主题正则匹配
            - body_contains: str — 内容包含
            - body_regex: str — 内容正则匹配
            
        # 其他选项
        check_interval: int — 检查间隔（秒），默认 60
        mark_as_seen: bool — 处理后是否标记为已读，默认 true
        max_emails_per_check: int — 每次最多处理邮件数，默认 10
        
        # 运行控制
        timeout: int/float — 监听超时时间（秒），0 表示永久运行
        
    注意:
        这是一个阻塞操作，会保持程序运行直到超时或手动停止
        按 Ctrl+C 可以停止
        
    触发上下文变量:
        - trigger_type: "email"
        - email_id: 邮件ID
        - email_subject: 邮件主题
        - email_from: 发件人
        - email_to: 收件人
        - email_date: 发送时间
        - email_body_text: 纯文本内容
        - email_body_html: HTML内容
        - email_attachments: 附件文件名列表
    """
    imap_server = params.get("imap_server")
    imap_port = params.get("imap_port", 993)
    username = params.get("username")
    password = params.get("password")
    use_ssl = params.get("use_ssl", True)
    config_file = params.get("config_file")
    
    if not all([imap_server, username, password, config_file]):
        raise ValueError("imap_server, username, password, config_file 参数必填")
    
    base_dir = ctx.get("base_dir", DATA_DIR)
    config_file = os.path.abspath(os.path.join(base_dir, config_file))
    
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"配置文件不存在: {config_file}")
    
    # 创建连接
    config_name = f"{username}_{imap_server}"
    if config_name in email_listener_state.connections:
        conn = email_listener_state.connections[config_name]
    else:
        conn = EmailConnection(
            config_name=config_name,
            imap_server=imap_server,
            imap_port=imap_port,
            username=username,
            password=password,
            use_ssl=use_ssl
        )
        email_listener_state.connections[config_name] = conn
    
    # 测试连接
    if not conn.connect():
        raise RuntimeError("邮箱连接失败，请检查配置")
    
    # 创建监控器
    monitor = EmailMonitor(
        connection=conn,
        config_file=config_file,
        base_dir=base_dir,
        filters=params.get("filters", {}),
        check_interval=params.get("check_interval", 60),
        mark_as_seen=params.get("mark_as_seen", True),
        max_emails_per_check=params.get("max_emails_per_check", 10)
    )
    
    email_listener_state.add_monitor(monitor)
    
    logger.info("=" * 50)
    logger.info(f"邮件监听配置完成")
    logger.info(f"邮箱: {username}")
    logger.info(f"服务器: {imap_server}:{imap_port}")
    logger.info(f"过滤条件: {params.get('filters', {})}")
    logger.info(f"检查间隔: {params.get('check_interval', 60)} 秒")
    logger.info(f"触发脚本: {config_file}")
    logger.info("=" * 50)
    
    # 启动监听
    email_listener_state.start()
    
    # 运行
    timeout = params.get("timeout", 0)
    try:
        if timeout and timeout > 0:
            logger.info(f"将在 {timeout} 秒后自动停止")
            email_listener_state.wait(timeout)
        else:
            logger.info("按 Ctrl+C 停止监听")
            email_listener_state.wait()
    except KeyboardInterrupt:
        logger.info("收到停止信号")
    finally:
        email_listener_state.stop()
    
    return {
        "status": "stopped",
        "email": username,
        "filters": params.get("filters", {})
    }


@op("email_listen_stop", category="email_listener", description="停止邮箱监听")
def op_email_listen_stop(ctx, params):
    """停止邮件监听"""
    email_listener_state.stop()
    return {"status": "stopped"}


@op("email_check_once", category="email_listener", description="单次检查邮件")
def op_email_check_once(ctx, params):
    """
    单次检查邮件，不启动常驻监听
    
    参数同 email_listen_start，但不阻塞运行
    返回匹配的邮件列表
    """
    imap_server = params.get("imap_server")
    imap_port = params.get("imap_port", 993)
    username = params.get("username")
    password = params.get("password")
    use_ssl = params.get("use_ssl", True)
    
    filters = params.get("filters", {})
    max_emails = params.get("max_emails", 10)
    
    if not all([imap_server, username, password]):
        raise ValueError("imap_server, username, password 参数必填")
    
    # 创建临时连接
    conn = EmailConnection(
        config_name="temp",
        imap_server=imap_server,
        imap_port=imap_port,
        username=username,
        password=password,
        use_ssl=use_ssl
    )
    
    if not conn.connect():
        raise RuntimeError("邮箱连接失败")
    
    try:
        # 搜索邮件
        email_ids = conn.search_emails("UNSEEN")
        
        results = []
        for email_id in email_ids[:max_emails]:
            email_data = conn.fetch_email(email_id)
            if not email_data:
                continue
            
            # 检查过滤条件
            monitor = EmailMonitor(conn, "", "", filters)
            if monitor._match_filters(email_data):
                results.append({
                    "id": email_data.get("id"),
                    "subject": email_data.get("subject"),
                    "from": email_data.get("from"),
                    "date": email_data.get("date"),
                    "body_text": email_data.get("body_text", "")[:500],  # 截断
                    "attachments": [a["filename"] for a in email_data.get("attachments", [])]
                })
                
                # 标记为已读
                if params.get("mark_as_seen", True):
                    conn.mark_as_seen(email_id)
        
        return {
            "total_unread": len(email_ids),
            "matched": len(results),
            "emails": results
        }
        
    finally:
        conn.close()


# ==================== 便捷操作 ====================

@op("email_watch_simple", category="email_listener", description="简单邮件监听（便捷封装）")
def op_email_watch_simple(ctx, params):
    """
    简化的邮件监听，一行配置实现
    
    参数:
        email: str — 邮箱账号（必填）
        password: str — 密码/授权码（必填）
        server: str — IMAP服务器（必填），如 "imap.qq.com"
        on_subject: str — 主题包含（可选）
        run: str — 触发时执行的脚本（必填）
        check_interval: int — 检查间隔，默认 60 秒
        
    示例:
        {
            "email": "xxx@qq.com",
            "password": "授权码",
            "server": "imap.qq.com",
            "on_subject": "订单",
            "run": "configs/process_order.json"
        }
    """
    filters = {}
    if params.get("on_subject"):
        filters["subject_contains"] = params["on_subject"]
    if params.get("from"):
        filters["from_contains"] = params["from"]
    
    return op_email_listen_start(ctx, {
        "imap_server": params.get("server"),
        "username": params.get("email"),
        "password": params.get("password"),
        "config_file": params.get("run"),
        "filters": filters,
        "check_interval": params.get("check_interval", 60),
        "timeout": params.get("timeout", 0)
    })


# ==================== OP_MAP ====================

OP_MAP = {
    "email_listen_start": op_email_listen_start,
    "email_listen_stop": op_email_listen_stop,
    "email_check_once": op_email_check_once,
    "email_watch_simple": op_email_watch_simple,
}


def run(config_path=None):
    """模块测试入口"""
    PipelineEngine.main(OP_MAP, cfg=config_path, init_ctx=lambda: {"base_dir": DATA_DIR})


if __name__ == '__main__':
    run()
