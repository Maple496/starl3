# config.py
"""编辑器配置管理模块

提供配置加载、验证和运行时设置管理
"""
import json
import os
import sys
from typing import Dict, List, Any, Callable, Optional, Set

# 项目根目录（config.py 的上级目录）
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 支持的列数据类型
VALID_DTYPES = {"str", "int", "float", "bool", "json", "enum"}

# 默认配置
def find_python_exe():
    """自动查找可用的 Python 解释器"""
    # 1. 优先使用当前运行的 Python
    if sys.executable and os.path.exists(sys.executable):
        return sys.executable
    
    # 2. 检查虚拟环境
    venv_python = os.path.join(BASE_DIR, ".venv", "Scripts", "python.exe")
    if os.path.exists(venv_python):
        return venv_python
    
    # 3. 尝试找系统 python
    for cmd in ["python", "python3", "py"]:
        try:
            import subprocess
            result = subprocess.run([cmd, "--version"], capture_output=True, text=True)
            if result.returncode == 0:
                return cmd
        except:
            pass
    
    return "python"


def find_starl3_runner():
    """查找 StarL3 运行器"""
    # BASE_DIR 是 scripts/ 目录
    # 1. 优先查找可执行文件 (scripts 同级目录)
    exe_path = os.path.join(os.path.dirname(BASE_DIR), "main_starl3.exe")
    if os.path.exists(exe_path):
        return exe_path, None
    
    # 2. 查找 Python 脚本 (BASE_DIR 就是 scripts/)
    py_path = os.path.join(BASE_DIR, "main_starl3.py")
    if os.path.exists(py_path):
        return None, py_path
    
    # 3. 搜索上级目录
    parent_dir = os.path.dirname(BASE_DIR)
    for root, dirs, files in os.walk(parent_dir):
        if "main_starl3.exe" in files:
            return os.path.join(root, "main_starl3.exe"), None
        if "main_starl3.py" in files:
            return None, os.path.join(root, "main_starl3.py")
        # 限制搜索深度
        if root.count(os.sep) > parent_dir.count(os.sep) + 2:
            break
    
    return None, None


# 自动查找运行器
_AUTO_EXE, _AUTO_PY = find_starl3_runner()
_AUTO_PYTHON = find_python_exe()

DEFAULT_PROFILE = {
    "title": "JSON Config Editor",
    "config_path": "configs/attemper_ops_config.json",
    "exe": _AUTO_EXE or os.path.join(os.path.dirname(BASE_DIR), "main_starl3.exe"),
    "py": _AUTO_PY or os.path.join(BASE_DIR, "main_starl3.py"),
    "python_exe": _AUTO_PYTHON,
    "run_args": lambda t, p: [t],
    "columns": [
        {"name": "step_id", "dtype": "str", "width": "80px", "label": "Step ID", "hidden": False, "default": ""},
        {"name": "step_order", "dtype": "int", "width": "70px", "label": "Step Order", "hidden": False, "default": "10", "auto_increment": 10},
        {"name": "op_type", "dtype": "str", "width": "90px", "label": "Op Type", "hidden": False, "default": "log"},
        {"name": "params_json", "dtype": "json", "width": "auto", "label": "Params JSON", "hidden": False, "default": "{}"},
        {"name": "enabled", "dtype": "enum", "width": "60px", "label": "Enabled", "hidden": True, "default": "Y", "enum_values": ["Y", "N"]},
        {"name": "note", "dtype": "str", "width": "150px", "label": "Note", "hidden": False, "default": ""}
    ],
    "default_rows": [["", "10", "log", "{}", "Y", ""]],
    "sort_col": "step_order",
    "toggle_col": "enabled"
}

# 所有可用配置
PROFILES: Dict[str, Dict[str, Any]] = {
    "quickELT": DEFAULT_PROFILE.copy()
}


def get_profile(key: str) -> Dict[str, Any]:
    """获取指定配置，如果不存在返回默认配置"""
    return PROFILES.get(key, DEFAULT_PROFILE.copy())


def parse_args(args: List[str]) -> tuple:
    """解析命令行参数
    
    Returns:
        (profile_key, config_path) 元组
    """
    profile_key = "quickELT"
    config_path = None
    
    for arg in args:
        if arg in PROFILES:
            profile_key = arg
        elif os.path.isfile(arg) and arg.lower().endswith('.json'):
            config_path = arg.replace('\\', '/')
    
    return profile_key, config_path


# 解析命令行参数
PROFILE_KEY, CLI_CONFIG_PATH = parse_args(sys.argv[1:])
ACTIVE_PROFILE = get_profile(PROFILE_KEY)

# 运行时设置
RUN_SETTINGS = {
    "config_path": CLI_CONFIG_PATH or ACTIVE_PROFILE["config_path"],
    "exe": ACTIVE_PROFILE["exe"],
    "py": ACTIVE_PROFILE["py"],
    "python_exe": ACTIVE_PROFILE["python_exe"]
}

# 列相关配置
COL_NAMES: List[str] = [c["name"] for c in ACTIVE_PROFILE["columns"]]
JSON_COLS: Set[int] = {i for i, c in enumerate(ACTIVE_PROFILE["columns"]) if c["dtype"] == "json"}
HIDDEN_COLS: List[str] = [c["name"] for c in ACTIVE_PROFILE["columns"] if c.get("hidden")]

# 加载 default_config.json
DEFAULT_CONFIG: Dict[str, Any] = {}
PUBLIC_OPS: List[str] = []


def load_default_config() -> Dict[str, Any]:
    """加载默认操作配置文件"""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "default_config.json")
    if not os.path.isfile(config_path):
        return {}
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"[警告] 加载 default_config.json 失败: {e}")
        return {}


# 加载配置
default_config_data = load_default_config()
if default_config_data:
    DEFAULT_CONFIG = default_config_data
    PUBLIC_OPS = list(DEFAULT_CONFIG.get("public_ops", {}).keys())


# 配置验证
class ConfigError(ValueError):
    """配置错误异常"""
    pass


def validate_column(col: Dict[str, Any], index: int) -> None:
    """验证单列配置
    
    Args:
        col: 列配置字典
        index: 列索引
        
    Raises:
        ConfigError: 配置无效时抛出
    """
    if not isinstance(col, dict):
        raise ConfigError(f"第 {index} 列必须是字典类型")
    
    if "name" not in col:
        raise ConfigError(f"第 {index} 列缺少 'name' 字段")
    
    if "dtype" not in col:
        raise ConfigError(f"列 '{col['name']}' 缺少 'dtype' 字段")
    
    if col["dtype"] not in VALID_DTYPES:
        raise ConfigError(f"列 '{col['name']}' 使用了无效的 dtype: {col['dtype']}")
    
    if col["dtype"] == "enum" and "enum_values" not in col:
        raise ConfigError(f"enum 类型列 '{col['name']}' 缺少 'enum_values'")


def validate_profile(profile: Dict[str, Any]) -> bool:
    """验证配置文件是否完整
    
    Args:
        profile: 配置字典
        
    Returns:
        True 如果配置有效
        
    Raises:
        ConfigError: 配置无效时抛出
    """
    required_keys = ["columns", "sort_col", "toggle_col"]
    for key in required_keys:
        if key not in profile:
            raise ConfigError(f"Profile 缺少必要配置: {key}")
    
    if not isinstance(profile["columns"], list):
        raise ConfigError("'columns' 必须是列表")
    
    if len(profile["columns"]) == 0:
        raise ConfigError("'columns' 不能为空列表")
    
    col_names = [c["name"] for c in profile["columns"]]
    
    if profile["sort_col"] not in col_names:
        raise ConfigError(f"sort_col '{profile['sort_col']}' 不在 columns 中")
    
    if profile["toggle_col"] not in col_names:
        raise ConfigError(f"toggle_col '{profile['toggle_col']}' 不在 columns 中")
    
    # 验证每列配置
    for i, col in enumerate(profile["columns"]):
        validate_column(col, i)
    
    return True


# 启动时验证
def init_config() -> None:
    """初始化配置，验证并处理错误"""
    try:
        validate_profile(ACTIVE_PROFILE)
    except ConfigError as e:
        print(f"[错误] 配置验证失败: {e}")
        sys.exit(1)


# 模块加载时自动初始化
init_config()
