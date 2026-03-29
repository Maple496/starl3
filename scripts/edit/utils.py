# utils.py
"""数据处理工具模块

提供数据清洗、文件操作和JSON处理功能
"""
import json
import os
import re
from tkinter import Tk, filedialog
from typing import List, Dict, Any, Optional, Union, Tuple

from .config import ACTIVE_PROFILE, RUN_SETTINGS, BASE_DIR, JSON_COLS


class JSONError(ValueError):
    """JSON 处理错误"""
    pass


def fix_json_str(s: Union[str, Any]) -> str:
    """尝试修复和规范化 JSON 字符串
    
    修复策略（按优先级）：
    1. 原样解析
    2. 移除尾随逗号后解析
    3. 无法修复则返回原字符串
    
    Args:
        s: 输入字符串
        
    Returns:
        修复后的 JSON 字符串
    """
    if not isinstance(s, str):
        return str(s)
    
    s = s.strip().replace('\r\n', '\n').replace('\r', '\n')
    if not s:
        return "{}"
    
    # 清理不可见字符
    s = re.sub(r'[\u00A0\u2000-\u200B\u3000\uFEFF]', ' ', s)
    
    # 尝试多种修复策略
    strategies = [
        lambda x: x,  # 原样尝试
        lambda x: re.sub(r',\s*([}\]])', r'\1', x),  # 移除尾随逗号
    ]
    
    for strategy in strategies:
        try:
            cleaned = strategy(s)
            parsed = json.loads(cleaned)
            return json.dumps(parsed, ensure_ascii=False, separators=(',', ':'))
        except json.JSONDecodeError:
            continue
    
    # 无法修复，返回原字符串
    return s.strip()


def strict_json_parse(s: str) -> Any:
    """严格的 JSON 解析，不进行自动修复
    
    Args:
        s: 要解析的字符串
        
    Returns:
        解析后的 Python 对象
        
    Raises:
        ValueError: 如果输入不是字符串
        json.JSONDecodeError: 如果解析失败
    """
    if not isinstance(s, str):
        raise ValueError(f"输入必须是字符串，得到 {type(s)}")
    
    s = s.strip()
    if not s:
        return {}
    
    # 只进行最基本的空白字符清理
    s = re.sub(r'[\u00A0\u2000-\u200B\u3000\uFEFF]', ' ', s)
    
    return json.loads(s)


def validate_json_str(s: str) -> Tuple[bool, Optional[str]]:
    """验证字符串是否为有效的 JSON
    
    Args:
        s: 要验证的字符串
        
    Returns:
        (is_valid, error_msg) 元组
    """
    try:
        strict_json_parse(s)
        return True, None
    except (json.JSONDecodeError, ValueError) as e:
        return False, str(e)


def _normalize_row(origin_r: Union[Dict, List], cols: List[Dict]) -> List:
    """将行数据标准化为列表格式，并补齐缺失列
    
    Args:
        origin_r: 原始行数据（列表或字典）
        cols: 列配置列表
        
    Returns:
        标准化后的行列表
    """
    if isinstance(origin_r, dict):
        r = [origin_r.get(c["name"], c.get("default", "")) for c in cols]
    else:
        r = list(origin_r)
        # 补齐不够的列
        for i, c in enumerate(cols):
            if i >= len(r):
                r.append(c.get("default", ""))
            elif r[i] in ["", None] and "default" in c:
                r[i] = c.get("default", "")
    return r


def _process_json_cols(r: List, json_cols: set) -> List:
    """处理 JSON 类型的列，验证并格式化
    
    Args:
        r: 行数据
        json_cols: JSON 列索引集合
        
    Returns:
        处理后的行数据
    """
    for i in json_cols:
        if i < len(r):
            val = r[i]
            if isinstance(val, dict):
                r[i] = json.dumps(val, ensure_ascii=False, separators=(',', ':'))
            else:
                is_valid, _ = validate_json_str(str(val))
                if is_valid:
                    r[i] = fix_json_str(str(val))
                else:
                    r[i] = str(val)  # 无效 JSON 保留原值
    return r


def _process_path_cols(r: List, json_cols: set) -> List:
    """处理路径列，将反斜杠替换为正斜杠
    
    Args:
        r: 行数据
        json_cols: JSON 列索引集合（跳过这些列）
        
    Returns:
        处理后的行数据
    """
    for i in range(len(r)):
        if i not in json_cols and isinstance(r[i], str):
            r[i] = r[i].replace('\\', '/')
    return r


def clean_data(data: Union[Dict[str, Any], List[List]]) -> List[List]:
    """清理和规范化数据
    
    Args:
        data: 包含 rows 的字典或直接是 rows 列表
        
    Returns:
        清理后的行列表
    """
    rows = data.get('rows', []) if isinstance(data, dict) else data
    cols = ACTIVE_PROFILE["columns"]
    
    res = []
    for origin_r in rows:
        r = _normalize_row(origin_r, cols)
        r = _process_json_cols(r, JSON_COLS)
        r = _process_path_cols(r, JSON_COLS)
        res.append(r)
    return res


def load_file(path: str) -> Dict[str, List]:
    """加载 JSON 文件
    
    Args:
        path: 文件路径
        
    Returns:
        包含 'rows' 键的字典
        
    Raises:
        FileNotFoundError: 文件不存在
        ValueError: JSON 解析错误
        RuntimeError: 其他加载错误
    """
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return {"rows": data.get("rows", data) if isinstance(data, dict) else data}
    except FileNotFoundError:
        raise FileNotFoundError(f"配置文件不存在: {path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON 解析错误 ({path}): {e}")
    except Exception as e:
        raise RuntimeError(f"加载文件失败 ({path}): {e}")


def ensure_config_exists() -> None:
    """确保配置文件存在，不存在则创建默认配置
    
    Raises:
        RuntimeError: 创建默认配置失败时抛出
    """
    cp = RUN_SETTINGS["config_path"]
    if not os.path.isabs(cp):
        cp = os.path.join(BASE_DIR, cp)
    if not os.path.exists(cp):
        try:
            os.makedirs(os.path.dirname(cp) or '.', exist_ok=True)
            default_rows = [[v.replace('\\', '/') if isinstance(v, str) else v for v in r] 
                          for r in ACTIVE_PROFILE["default_rows"]]
            with open(cp, 'w', encoding='utf-8') as f:
                json.dump(default_rows, f, ensure_ascii=False, indent=2)
        except Exception as e:
            raise RuntimeError(f"创建默认配置文件失败 ({cp}): {e}")


def generate_temp_file(profile_name: str, data: Any) -> str:
    """生成临时文件
    
    Args:
        profile_name: 配置文件名前缀
        data: 要写入的数据
        
    Returns:
        临时文件路径
    """
    temp_path = os.path.join(BASE_DIR, f"{profile_name}_temp.json")
    with open(temp_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return temp_path


def get_run_cmd(temp_path: Optional[str] = None) -> Tuple[Optional[List[str]], str]:
    """获取运行命令
    
    Args:
        temp_path: 临时配置文件路径
        
    Returns:
        (命令列表, 状态消息) 元组
    """
    exe_path = os.path.join(BASE_DIR, RUN_SETTINGS["exe"])
    py_path = os.path.join(BASE_DIR, RUN_SETTINGS["py"])
    py_exe = RUN_SETTINGS["python_exe"].strip()
    cmd_args = [temp_path] if temp_path else []
    
    if os.path.isfile(exe_path):
        return [exe_path] + cmd_args, "EXE已启动"
    if os.path.isfile(py_exe) and os.path.isfile(py_path):
        return [py_exe, py_path] + cmd_args, "Python已启动"
    if not os.path.isfile(py_exe):
        return None, f"Python解释器无效: {py_exe}"
    if not os.path.isfile(py_path):
        return None, f"Python脚本不存在: {py_path}"
    return None, "找不到执行文件"


def get_cmd_str(temp_path: Optional[str] = None) -> Optional[str]:
    """获取运行命令字符串
    
    Args:
        temp_path: 临时配置文件路径
        
    Returns:
        命令字符串或 None
    """
    exe_path = os.path.join(BASE_DIR, RUN_SETTINGS["exe"])
    py_path = os.path.join(BASE_DIR, RUN_SETTINGS["py"])
    py_exe = RUN_SETTINGS["python_exe"].strip()
    arg = f' "{temp_path}"' if temp_path else ""
    
    if os.path.isfile(exe_path):
        return f'"{exe_path}"{arg}'
    if os.path.isfile(py_exe) and os.path.isfile(py_path):
        return f'"{py_exe}" "{py_path}"{arg}'
    return None


BROWSE_MODES = {
    "folder": {"func": filedialog.askdirectory, "kwargs": {}},
    "file_py": {"func": filedialog.askopenfilename, 
                "kwargs": {"filetypes": [("Python/EXE", "*.py *.exe"), ("All", "*.*")]}},
    "file_json": {"func": filedialog.askopenfilename,
                  "kwargs": {"filetypes": [("JSON", "*.json"), ("All", "*.*")]}},
    "file_exe": {"func": filedialog.askopenfilename,
                 "kwargs": {"filetypes": [("EXE", "*.exe"), ("All", "*.*")]}},
    "save_json": {"func": filedialog.asksaveasfilename,
                  "kwargs": {"defaultextension": ".json",
                           "filetypes": [("JSON", "*.json"), ("All", "*.*")]}}
}


def browse_path(mode: str, initial_dir: str = "") -> str:
    """浏览文件或目录
    
    Args:
        mode: 浏览模式，支持 folder/file_py/file_json/file_exe/save_json
        initial_dir: 初始目录
        
    Returns:
        选择的文件/目录路径，取消则返回空字符串
    """
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    try:
        initial_dir = initial_dir or BASE_DIR
        config = BROWSE_MODES.get(mode)
        
        if not config:
            return ""
        
        kwargs = {"initialdir": initial_dir, **config["kwargs"]}
        result = config["func"](**kwargs)
        
        return result.replace('\\', '/') if result else ""
    finally:
        root.destroy()
