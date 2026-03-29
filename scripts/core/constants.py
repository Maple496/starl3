"""
StarL3 全局常量定义
解决循环导入问题的核心模块

路径规划：
- APP_DIR: 程序所在目录（只读，不放数据）
- DATA_DIR: 用户数据目录（可配置，可迁移）
- TEMP_DIR: 临时文件目录（不迁移，自动清理）
"""

import sys
import os
import tempfile
from pathlib import Path


def get_app_dir() -> str:
    """获取程序所在目录（只读，不放数据）
    
    优先级:
    1. PyInstaller 打包后的临时目录 (sys._MEIPASS)
    2. 当前脚本所在目录
    3. 当前工作目录
    """
    if getattr(sys, 'frozen', False):
        if hasattr(sys, '_MEIPASS'):
            return sys._MEIPASS
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_data_dir(app_name: str = "starl3") -> Path:
    """获取用户数据目录（可配置，可迁移）
    
    优先级：
    1. 环境变量 STARL3_DATA_DIR
    2. 系统标准用户数据目录
    
    Returns:
        Path: 用户数据目录路径
    """
    # 优先从环境变量读取
    if env_dir := os.environ.get("STARL3_DATA_DIR"):
        data_dir = Path(env_dir)
    else:
        # 系统标准目录
        system = sys.platform
        if system == "win32":
            # Windows: C:\Users\用户名\AppData\Local\starl3
            base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData/Local"))
        elif system == "darwin":
            # macOS: ~/Library/Application Support/starl3
            base = Path.home() / "Library/Application Support"
        else:
            # Linux: ~/.local/share/starl3
            xdg_data = os.environ.get("XDG_DATA_HOME")
            base = Path(xdg_data) if xdg_data else Path.home() / ".local/share"
        
        data_dir = base / app_name
    
    # 自动创建目录
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def get_temp_dir(app_name: str = "starl3") -> Path:
    """获取应用临时目录（不迁移，可定期清理）
    
    Returns:
        Path: 临时目录路径
    """
    temp_dir = Path(tempfile.gettempdir()) / app_name
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


def get_log_dir(app_name: str = "starl3") -> Path:
    """获取日志目录
    
    Returns:
        Path: 日志目录路径（位于数据目录下）
    """
    log_dir = get_data_dir(app_name) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def get_config_dir(app_name: str = "starl3") -> Path:
    """获取配置目录
    
    Returns:
        Path: 配置目录路径（位于数据目录下）
    """
    config_dir = get_data_dir(app_name) / "configs"
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir


def get_dynamic_config_dir(app_name: str = "starl3") -> Path:
    """获取动态配置目录（运行时生成的配置）
    
    Returns:
        Path: 动态配置目录路径
    """
    dyn_dir = get_data_dir(app_name) / "dynamic_configs"
    dyn_dir.mkdir(parents=True, exist_ok=True)
    return dyn_dir


# ==================== 兼容性常量 ====================
# 旧代码使用 BASE_DIR，新代码应使用 APP_DIR 或 DATA_DIR

APP_DIR = get_app_dir()  # 程序目录（只读）
DATA_DIR = str(get_data_dir())  # 用户数据目录（可迁移）
TEMP_DIR = str(get_temp_dir())  # 临时目录（不迁移）

# 兼容旧代码
BASE_DIR = APP_DIR

# 常用路径（基于数据目录，可迁移）
CONFIGS_DIR = str(get_config_dir())
DYNAMIC_CONFIGS_DIR = str(get_dynamic_config_dir())
LOGS_DIR = str(get_log_dir())

# 程序目录（基于程序安装位置，只读）
OPS_DIR = os.path.join(APP_DIR, "ops")

# 旧 OUTPUT_DIR 改为基于数据目录
OUTPUT_DIR = os.path.join(DATA_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ==================== 便捷函数 ====================

def resolve_app_path(relative_path: str) -> str:
    """将相对路径解析为程序目录下的绝对路径（只读资源）"""
    return os.path.join(APP_DIR, relative_path)


def resolve_data_path(relative_path: str) -> str:
    """将相对路径解析为用户数据目录下的绝对路径（可迁移）"""
    full_path = Path(DATA_DIR) / relative_path
    full_path.parent.mkdir(parents=True, exist_ok=True)
    return str(full_path)


def resolve_temp_path(relative_path: str) -> str:
    """将相对路径解析为临时目录下的绝对路径（不迁移）"""
    full_path = Path(TEMP_DIR) / relative_path
    full_path.parent.mkdir(parents=True, exist_ok=True)
    return str(full_path)


def ensure_data_dir(path: str) -> str:
    """确保数据目录存在，返回完整路径
    
    Args:
        path: 相对路径（相对于数据目录）或绝对路径
    
    Returns:
        str: 完整路径
    """
    if os.path.isabs(path):
        full_path = Path(path)
    else:
        full_path = Path(DATA_DIR) / path
    
    full_path.parent.mkdir(parents=True, exist_ok=True)
    return str(full_path)


# 兼容旧函数
resolve_path = resolve_data_path
ensure_dir = ensure_data_dir
