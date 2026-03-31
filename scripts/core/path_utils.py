"""
StarL3 路径安全工具模块
"""

import os
import re
from pathlib import Path


class PathSecurityError(Exception):
    """路径安全错误"""
    pass


def safe_join(base_dir: str, *paths: str) -> str:
    """安全地拼接路径，防止路径遍历攻击"""
    base_dir = os.path.abspath(base_dir)
    final_path = os.path.abspath(os.path.join(base_dir, *paths))
    
    base_resolved = Path(base_dir).resolve()
    final_resolved = Path(final_path).resolve()
    
    try:
        final_resolved.relative_to(base_resolved)
    except ValueError:
        raise PathSecurityError(f"路径超出允许范围: '{paths}'")
    
    return final_path


def ensure_dir_exists(path: str) -> None:
    """确保目录存在"""
    dir_path = os.path.dirname(path)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)


def _resolve_path(path: str, ctx) -> str:
    """解析路径中的变量引用，如 ${paths.source_file}"""
    if not path or not isinstance(path, str):
        return path
    
    def replace_var(match):
        var_path = match.group(1)
        parts = var_path.split('.')
        current = ctx
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            elif hasattr(current, '__getitem__') and part in current:
                current = current[part]
            else:
                return match.group(0)
        return str(current) if current is not None else match.group(0)
    
    return re.sub(r'\$\{([^}]+)\}', replace_var, path)
