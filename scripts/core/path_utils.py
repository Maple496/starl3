"""
StarL3 路径安全工具模块
提供安全的路径操作函数，防止路径遍历攻击
"""

import os
import re
from pathlib import Path
from typing import Optional


class PathSecurityError(Exception):
    """路径安全错误"""
    pass


def safe_join(base_dir: str, *paths: str) -> str:
    """
    安全地拼接路径，防止路径遍历攻击
    
    Args:
        base_dir: 基础目录（必须是绝对路径）
        *paths: 要拼接的路径组件
        
    Returns:
        安全的绝对路径
        
    Raises:
        PathSecurityError: 如果结果路径超出基础目录范围
        
    Example:
        >>> safe_join("/home/user", "data", "file.txt")
        "/home/user/data/file.txt"
        >>> safe_join("/home/user", "../../../etc/passwd")
        PathSecurityError: 路径超出允许范围
    """
    # 确保基础目录是绝对路径
    base_dir = os.path.abspath(base_dir)
    
    # 拼接路径
    joined = os.path.join(base_dir, *paths)
    
    # 解析为绝对路径（处理 .. 和 .）
    final_path = os.path.abspath(joined)
    
    # 安全检查：确保最终路径在基础目录内
    # 使用 Path 的 resolve 方法来处理符号链接
    try:
        base_resolved = Path(base_dir).resolve()
        final_resolved = Path(final_path).resolve()
        
        # 检查 final_resolved 是否在 base_resolved 之下
        try:
            final_resolved.relative_to(base_resolved)
        except ValueError:
            raise PathSecurityError(
                f"路径安全错误: '{paths}' 解析为 '{final_path}'，"
                f"超出了基础目录 '{base_dir}' 的范围"
            )
    except (OSError, RuntimeError) as e:
        raise PathSecurityError(f"路径解析错误: {e}")
    
    return final_path


def safe_path_join(base_dir: str, relative_path: str) -> str:
    """
    安全地拼接单一路径（兼容旧接口）
    
    Args:
        base_dir: 基础目录
        relative_path: 相对路径
        
    Returns:
        安全的绝对路径
    """
    return safe_join(base_dir, relative_path)


def validate_path_in_scope(path: str, base_dir: str) -> bool:
    """
    验证路径是否在指定范围内
    
    Args:
        path: 要验证的路径
        base_dir: 允许的基础目录
        
    Returns:
        True 如果在范围内，False 否则
    """
    try:
        safe_join(base_dir, os.path.relpath(path, base_dir))
        return True
    except PathSecurityError:
        return False


def ensure_dir_exists(path: str) -> None:
    """
    确保目录存在，如果不存在则创建
    
    Args:
        path: 文件路径（将创建其父目录）
    """
    dir_path = os.path.dirname(path)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)



def _resolve_path(path: str, ctx: dict) -> str:
    """
    解析路径中的变量引用，如 ${paths.source_file}
    
    Args:
        path: 原始路径，可能包含变量引用，如 "${paths.output_dir}/result.csv"
        ctx: 上下文字典，包含变量值
        
    Returns:
        解析后的实际路径
        
    Example:
        >>> ctx = {'paths': {'output_dir': '/data/output'}}
        >>> _resolve_path('${paths.output_dir}/result.csv', ctx)
        '/data/output/result.csv'
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
