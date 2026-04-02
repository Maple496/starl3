"""
StarL3 共享工具模块
集中存放跨模块重复的工具函数
"""

import os
import re
from pathlib import Path
from typing import Any, Dict


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


def resolve_path(path: str, ctx: dict) -> str:
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


def detect_encoding(file_path: str, sample_size: int = 100000) -> str:
    """使用 chardet 预检测文件编码"""
    try:
        import chardet
        with open(file_path, 'rb') as f:
            raw_data = f.read(sample_size)
            result = chardet.detect(raw_data)
            encoding = result.get('encoding', 'utf-8')
            confidence = result.get('confidence', 0)
            
            if confidence > 0.7:
                encoding_map = {
                    'GB2312': 'gb18030',
                    'GBK': 'gb18030',
                    'UTF-8-SIG': 'utf-8-sig',
                    'UTF-16-LE': 'utf-16',
                    'UTF-16-BE': 'utf-16',
                }
                return encoding_map.get(encoding.upper(), encoding)
    except Exception:
        pass
    return 'utf-8'


def clean_newlines(df):
    """清理字符串列中的换行符"""
    str_cols = df.select_dtypes(include=["object"]).columns
    if len(str_cols) == 0:
        return df
    
    replacements = {
        col: df[col].str.replace(r'[\r\n]+', ' ', regex=True)
        for col in str_cols
    }
    return df.assign(**replacements)


__all__ = [
    'detect_encoding', 'clean_newlines', 'resolve_path', 'safe_join',
    'ensure_dir_exists', 'PathSecurityError'
]
