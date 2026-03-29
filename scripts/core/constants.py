"""
StarL3 全局常量定义
解决循环导入问题的核心模块
"""

import sys
import os

# 基础目录 - 优先从 sys._MEIPASS 获取（PyInstaller 打包后）
def get_base_dir() -> str:
    """获取应用基础目录
    
    优先级:
    1. PyInstaller 打包后的临时目录 (sys._MEIPASS)
    2. 当前脚本所在目录
    3. 当前工作目录
    """
    if getattr(sys, 'frozen', False):
        # PyInstaller 打包后的可执行文件
        if hasattr(sys, '_MEIPASS'):
            return sys._MEIPASS
        return os.path.dirname(sys.executable)
    else:
        # 开发环境
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# 全局常量
BASE_DIR = get_base_dir()

# 常用路径
CONFIGS_DIR = os.path.join(BASE_DIR, "configs")
OPS_DIR = os.path.join(BASE_DIR, "ops")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
PROJECTS_DIR = os.path.join(BASE_DIR, "projects")


# 便捷函数
def resolve_path(relative_path: str) -> str:
    """将相对路径解析为绝对路径"""
    return os.path.join(BASE_DIR, relative_path)


def ensure_dir(path: str) -> str:
    """确保目录存在，如果不存在则创建"""
    if not os.path.isabs(path):
        path = os.path.join(BASE_DIR, path)
    os.makedirs(os.path.dirname(path) or path, exist_ok=True)
    return path
