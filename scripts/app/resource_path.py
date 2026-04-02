"""
资源路径工具模块
解决 PyInstaller 打包后的资源路径问题
"""
import os
import sys


def get_resource_path(relative_path):
    """
    获取资源的绝对路径
    兼容开发环境和 PyInstaller 打包环境
    
    Args:
        relative_path: 相对于项目根目录的路径
        
    Returns:
        绝对路径
    """
    if hasattr(sys, '_MEIPASS'):
        # PyInstaller 打包环境：资源在临时解压目录
        return os.path.join(sys._MEIPASS, relative_path)
    else:
        # 开发环境：资源在脚本目录
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(base_dir, relative_path)


def get_static_path(sub_path=''):
    """
    获取静态文件路径
    
    Args:
        sub_path: 子路径，如 'web', 'editors/pipeline.html'
        
    Returns:
        完整路径
    """
    return get_resource_path(os.path.join('frontend', 'static', sub_path))
