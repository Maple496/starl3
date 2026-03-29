import os
import re
import shutil
import platform
import subprocess
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

from core.pipeline_engine import PipelineEngine
from core.safe_eval import SafeEvaluator, SafeEvalError
from core.constants import BASE_DIR, DATA_DIR
from core.path_utils import ensure_dir_exists
from core.registry import op
from ops.plug_lib.csvtohtml1 import csv_to_html
from ops.plug_lib.csvtohtml2 import csv_to_html as csv_to_html2

# 创建模块级别的 logger
from core.logger import get_logger
logger = get_logger("file_ops")


def _resolve_path(path: str, ctx: dict) -> str:
    """解析路径中的变量引用，如 ${paths.source_dir}
    
    Args:
        path: 原始路径，可能包含变量引用
        ctx: 上下文字典
        
    Returns:
        解析后的实际路径
    """
    if not path or not isinstance(path, str):
        return path
    
    def replace_var(match):
        var_path = match.group(1)
        parts = var_path.split('.')
        current = ctx
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return match.group(0)  # 变量不存在，保持原样
        return str(current) if current is not None else match.group(0)
    
    return re.sub(r'\$\{([^}]+)\}', replace_var, path)


def _match(n: str, i: Dict, conditions: List[Dict]) -> bool:
    """匹配文件条件
    
    Args:
        n: 文件名
        i: 文件信息字典
        conditions: 条件列表
        
    Returns:
        是否匹配所有条件
    """
    for c in conditions:
        field, op, value = c.get("field"), c.get("operator"), c.get("value")
        
        # 获取字段值
        field_value = {
            "name": n,
            "ext": i.get("extension", ""),
            "modify_time": i.get("modify_time", ""),
            "size": i.get("file_size", 0),
            "type": i.get("type", "")
        }.get(field)
        
        if field_value is None:
            continue
        
        # 正则匹配
        if op == "~":
            if not re.search(str(value), str(field_value)):
                return False
            continue
        
        # 时间字段特殊处理
        if field == "modify_time":
            try:
                import time
                field_value = datetime.strptime(field_value, "%Y-%m-%d %H:%M:%S").timestamp()
                value_str = str(value)
                if "+" in value_str:
                    value = time.time() + int(value_str.split("+")[1])
                elif "-" in value_str:
                    value = time.time() - int(value_str.split("-")[1])
                else:
                    value = float(value)
            except (ValueError, TypeError) as e:
                # 时间解析失败，视为不匹配
                return False
        
        # 使用安全比较替代 eval
        try:
            # 标准化操作符
            if op == "=":
                op = "=="
            
            # 构建安全的比较表达式
            if isinstance(field_value, str):
                # 字符串比较
                if op == "==":
                    result = field_value == str(value)
                elif op == "!=":
                    result = field_value != str(value)
                elif op == "<":
                    result = field_value < str(value)
                elif op == "<=":
                    result = field_value <= str(value)
                elif op == ">":
                    result = field_value > str(value)
                elif op == ">=":
                    result = field_value >= str(value)
                else:
                    return False
            else:
                # 数值比较
                try:
                    value_num = float(value)
                except (ValueError, TypeError):
                    value_num = value
                    
                if op == "==":
                    result = field_value == value_num
                elif op == "!=":
                    result = field_value != value_num
                elif op == "<":
                    result = field_value < value_num
                elif op == "<=":
                    result = field_value <= value_num
                elif op == ">":
                    result = field_value > value_num
                elif op == ">=":
                    result = field_value >= value_num
                else:
                    return False
            
            if not result:
                return False
                
        except Exception as e:
            # 比较失败，视为不匹配
            return False
    
    return True

def build_item(path: str) -> Dict[str, Any]:
    """构建文件/目录信息字典
    
    Args:
        path: 文件或目录路径
        
    Returns:
        包含文件信息的字典
        
    Raises:
        FileNotFoundError: 路径不存在
        PermissionError: 无权限访问
    """
    try:
        st = os.stat(path)
        mt = datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        
        if os.path.isfile(path):
            return {
                "file_size": st.st_size / 1024,
                "modify_time": mt,
                "extension": Path(path).suffix[1:],
                "type": "file"
            }
        return {"modify_time": mt, "type": "folder"}
        
    except FileNotFoundError:
        raise FileNotFoundError(f"路径不存在: {path}")
    except PermissionError:
        raise PermissionError(f"无权限访问: {path}")

def refresh_items(base_path, items):
    new_items = {}
    for name, info in items.items():
        p = os.path.join(base_path, name)
        if os.path.exists(p):
            new_items[name] = build_item(p)
    return new_items

@op("scan_directory", category="file", description="扫描目录")
def scan_directory(ctx, p):
    """扫描目录
    
    Args:
        ctx: 执行上下文
        p: 参数字典，包含 folder_path
        
    Returns:
        包含 base_path 和 items 的字典
    """
    folder_path = p.get("folder_path")
    if not folder_path:
        raise ValueError("folder_path 参数不能为空")
    
    # 解析路径变量（如 ${paths.source_dir}）
    folder_path = _resolve_path(folder_path, ctx)
    
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"目录不存在: {folder_path}")
    
    if not os.path.isdir(folder_path):
        raise NotADirectoryError(f"不是目录: {folder_path}")
    
    try:
        items = {
            i: build_item(os.path.join(folder_path, i))
            for i in os.listdir(folder_path)
        }
        return {"base_path": folder_path, "items": items}
    except PermissionError as e:
        raise PermissionError(f"无法访问目录 {folder_path}: {e}")

@op("filter_files", category="file", description="过滤文件")
def filter_files(ctx, p):
    """过滤文件
    
    Args:
        ctx: 执行上下文
        p: 参数字典，包含 conditions
        
    Returns:
        过滤后的文件列表
    """
    last_result = ctx.get("last_result", {})
    base_path = last_result.get("base_path") or ctx.get("base_dir", "")
    items = last_result.get("items", {})
    conditions = p.get("conditions", [])
    
    if not isinstance(conditions, list):
        raise ValueError("conditions 必须是列表")
    
    try:
        filtered = {
            k: v for k, v in items.items()
            if _match(k, v, conditions)
        }
        return {"base_path": base_path, "items": filtered}
    except Exception as e:
        raise RuntimeError(f"过滤文件时出错: {e}")

def batch_delete(ctx, p):
    """批量删除文件/目录
    
    Args:
        ctx: 执行上下文
        p: 参数字典
        
    Returns:
        删除结果
    """
    last_result = ctx.get("last_result", {})
    base_path = last_result.get("base_path", "")
    items = last_result.get("items", {})
    
    deleted = {}
    errors = []
    
    for name, info in items.items():
        path = os.path.join(base_path, name)
        try:
            if info.get("type") == "folder":
                shutil.rmtree(path)
            else:
                os.remove(path)
            deleted[name] = "deleted"
        except FileNotFoundError:
            deleted[name] = "not_found"
        except PermissionError as e:
            errors.append(f"无权限删除 {name}: {e}")
            deleted[name] = "permission_denied"
        except Exception as e:
            errors.append(f"删除 {name} 失败: {e}")
            deleted[name] = "error"
    
    if errors:
        return {
            "base_path": base_path,
            "items": deleted,
            "errors": errors
        }
    
    return {"base_path": base_path, "items": deleted}

def batch_rename(ctx, p):
    """批量重命名文件
    
    Args:
        ctx: 执行上下文
        p: 参数字典，包含 prefix
        
    Returns:
        重命名结果
    """
    last_result = ctx.get("last_result", {})
    base_path = last_result.get("base_path", "")
    items = last_result.get("items", {})
    prefix = p.get("prefix", "file")
    
    renamed = {}
    errors = []
    
    for idx, (old_name, info) in enumerate(items.items(), 1):
        ext = f".{info.get('extension')}" if info.get("extension") else ""
        
        # 查找可用的新文件名
        counter = 0
        while True:
            suffix = f"_{counter}" if counter > 0 else ""
            new_name = f"{prefix}_{idx}{suffix}{ext}"
            new_path = os.path.join(base_path, new_name)
            
            if not os.path.exists(new_path):
                break
            counter += 1
        
        old_path = os.path.join(base_path, old_name)
        
        try:
            os.rename(old_path, new_path)
            renamed[new_name] = info
        except FileNotFoundError:
            errors.append(f"源文件不存在: {old_name}")
        except PermissionError as e:
            errors.append(f"无权限重命名 {old_name}: {e}")
        except Exception as e:
            errors.append(f"重命名 {old_name} 失败: {e}")
    
    result = {"base_path": base_path, "items": renamed}
    if errors:
        result["errors"] = errors
    
    return result

def limit_items(ctx, p):
    d = ctx.get("last_result", {})
    count = p.get("count")
    items = dict(list(d.get("items", {}).items())[:count]) if count else d.get("items", {})
    return {"base_path": d.get("base_path"), "items": items}

def batch_copy(ctx, p):
    """批量复制文件/目录
    
    Args:
        ctx: 执行上下文
        p: 参数字典，包含 dest_path
        
    Returns:
        复制结果
    """
    last_result = ctx.get("last_result", {})
    base_path = last_result.get("base_path", "")
    items = last_result.get("items", {})
    dest_path = p.get("dest_path")
    
    if not dest_path:
        raise ValueError("dest_path 参数不能为空")
    
    # 使用核心工具函数创建目录
    try:
        ensure_dir_exists(os.path.join(dest_path, ".placeholder"))
    except Exception as e:
        raise RuntimeError(f"无法创建目标目录 {dest_path}: {e}")
    
    copied = {}
    errors = []
    
    for name, info in items.items():
        src = os.path.join(base_path, name)
        dst = os.path.join(dest_path, name)
        
        try:
            if info.get("type") == "folder":
                shutil.copytree(src, dst, dirs_exist_ok=True)
            else:
                shutil.copy2(src, dst)
            copied[name] = info
        except FileNotFoundError:
            errors.append(f"源文件不存在: {name}")
        except PermissionError as e:
            errors.append(f"无权限复制 {name}: {e}")
        except Exception as e:
            errors.append(f"复制 {name} 失败: {e}")
    
    # 刷新目标目录的文件信息
    try:
        new_items = refresh_items(dest_path, copied)
    except Exception as e:
        new_items = copied
    
    result = {"base_path": dest_path, "items": new_items}
    if errors:
        result["errors"] = errors
    
    return result

@op("print_result", category="file", description="打印结果")
def print_result(ctx, p):
    d = p.get("content") or ctx.get("last_result", {})
    print(f">>> [Pipeline Step Result]:\n{json.dumps(d, indent=2, ensure_ascii=False)}")
    return d

def open_program(ctx, p):
    d = ctx.get("last_result", {})
    path = p.get("path") or (d.get("base_path") if isinstance(d,dict) else str(d))
    if os.path.exists(path):
        opener = getattr(os, 'startfile', None)
        if opener:
            opener(path)
        else:
            subprocess.Popen(["open" if platform.system() == "Darwin" else "xdg-open", path])
    return path

def plug_in(ctx, p):
    d = ctx.get("last_result", {})
    base_path, items = d.get("base_path", ""), d.get("items", {})
    out_dir = Path(base_path) / "output"
    out_dir.mkdir(exist_ok=True)
    typ = p.get("type", "csvtohtml")
    if typ == "csvtohtml":
        html_items = {}
        for name, info in items.items():
            if info.get("type") == "file" and name.lower().endswith(".csv"):
                src, dst = Path(base_path)/name, out_dir/(Path(name).stem+".html")
                csv_to_html(src, dst)
                html_items[dst.name] = {"type":"file", "path": str(dst)}
        ctx["last_result"] = {"base_path": str(out_dir), "items": html_items}
        return ctx["last_result"]
    elif typ == "csvtohtml2":
        html_items = {}
        for name, info in items.items():
            if info.get("type") == "file" and name.lower().endswith(".csv"):
                src, dst = Path(base_path)/name, out_dir/(Path(name).stem+".html")
                csv_to_html2(src, dst)
                html_items[dst.name] = {"type":"file", "path": str(dst)}
        ctx["last_result"] = {"base_path": str(out_dir), "items": html_items}
        return ctx["last_result"]
    return d

OP_MAP = {
    "scan_directory": scan_directory,
    "filter_files": filter_files,
    "batch_delete": batch_delete,
    "batch_rename": batch_rename,
    "limit_items": limit_items,
    "batch_copy": batch_copy,
    "print_result": print_result,
    "open_program": open_program,
    "plug_in": plug_in,
}

def run(config_path=None):
    PipelineEngine.main(OP_MAP, cfg=config_path, init_ctx=lambda: {"base_dir": DATA_DIR})

if __name__ == '__main__':
    run()