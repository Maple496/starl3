"""
键盘鼠标操作模块
提供自动化键鼠操作功能，支持配置化执行操作序列
"""

import os
import json
import time
import threading
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum
from datetime import datetime

from core.pipeline_engine import PipelineEngine
from core.constants import BASE_DIR
from core.logger import get_logger
from core.registry import op
from core.path_utils import safe_join, ensure_dir_exists

logger = get_logger("input_ops")

# 尝试导入 pyautogui，如果没有安装则给出提示
try:
    import pyautogui
    # 设置安全模式，防止鼠标失控时无法退出
    pyautogui.FAILSAFE = True
    pyautogui.PAUSE = 0.1
    HAS_PYAUTOGUI = True
except ImportError:
    HAS_PYAUTOGUI = False
    logger.warning("pyautogui 未安装，键鼠操作功能不可用。请运行: pip install pyautogui")

# 尝试导入 pynput 用于录制
try:
    from pynput import mouse, keyboard
    from pynput.mouse import Button as MouseButton
    from pynput.keyboard import Key
    HAS_PYNPUT = True
except ImportError:
    HAS_PYNPUT = False
    logger.warning("pynput 未安装，录制功能不可用。请运行: pip install pynput")


class ActionType(Enum):
    """操作类型枚举"""
    MOUSE_CLICK = "mouse_click"       # 鼠标点击
    MOUSE_MOVE = "mouse_move"         # 鼠标移动
    MOUSE_SCROLL = "mouse_scroll"     # 鼠标滚轮
    MOUSE_DRAG = "mouse_drag"         # 鼠标拖拽
    KEY_PRESS = "key_press"           # 按键
    TYPE_TEXT = "type_text"           # 输入文本
    HOTKEY = "hotkey"                 # 组合键
    WAIT = "wait"                     # 等待
    SCREENSHOT = "screenshot"         # 截图
    GET_POSITION = "get_position"     # 获取鼠标位置
    FIND_CLICK = "find_click"         # 查找图片并点击


@dataclass
class InputAction:
    """键鼠操作数据类"""
    action_type: str
    params: Dict[str, Any]
    description: str = ""  # 操作描述，用于日志记录
    
    def to_dict(self) -> Dict:
        return {
            "action_type": self.action_type,
            "params": self.params,
            "description": self.description
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "InputAction":
        return cls(
            action_type=data["action_type"],
            params=data.get("params", {}),
            description=data.get("description", "")
        )


@dataclass
class InputConfig:
    """键鼠操作配置"""
    name: str
    actions: List[InputAction]
    description: str = ""
    repeat: int = 1  # 重复执行次数
    interval: float = 0.0  # 每次重复间隔（秒）
    created_at: str = ""  # 创建时间
    
    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "description": self.description,
            "repeat": self.repeat,
            "interval": self.interval,
            "created_at": self.created_at or datetime.now().isoformat(),
            "actions": [a.to_dict() for a in self.actions]
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "InputConfig":
        return cls(
            name=data["name"],
            description=data.get("description", ""),
            repeat=data.get("repeat", 1),
            interval=data.get("interval", 0.0),
            created_at=data.get("created_at", ""),
            actions=[InputAction.from_dict(a) for a in data.get("actions", [])]
        )


def _ensure_pyautogui():
    """确保 pyautogui 已安装"""
    if not HAS_PYAUTOGUI:
        raise RuntimeError("pyautogui 未安装，请运行: pip install pyautogui")


def _ensure_pynput():
    """确保 pynput 已安装"""
    if not HAS_PYNPUT:
        raise RuntimeError("pynput 未安装，请运行: pip install pynput")


def _get_config_path(ctx: dict, config_name: str) -> str:
    """获取配置文件路径"""
    configs_dir = os.path.join(ctx.get("base_dir", BASE_DIR), "dynamic_configs", "input")
    os.makedirs(configs_dir, exist_ok=True)
    return os.path.join(configs_dir, f"{config_name}.json")


# ==================== 配置管理操作 ====================

@op("create_input_config", category="input", description="创建键鼠操作配置")
def op_create_input_config(ctx, params):
    """
    创建并保存键鼠操作配置
    
    参数:
        config_name: str — 配置名称（必填）
        actions: list — 操作列表（必填）
        description: str — 配置描述（可选）
        repeat: int — 重复执行次数，默认 1
        interval: float — 每次重复间隔（秒），默认 0
        save_to: str — 保存结果到 ctx 的路径，如 "input_configs.my_config"
        
    操作列表格式:
        [
            {
                "action_type": "mouse_click",
                "params": {"x": 100, "y": 200, "button": "left", "clicks": 1},
                "description": "点击登录按钮"
            },
            {
                "action_type": "type_text",
                "params": {"text": "hello world", "interval": 0.01},
                "description": "输入文本"
            }
        ]
    
    返回:
        创建的配置字典
    """
    config_name = params.get("config_name")
    if not config_name:
        raise ValueError("config_name 参数不能为空")
    
    actions_data = params.get("actions", [])
    if not actions_data:
        raise ValueError("actions 参数不能为空")
    
    # 构建配置对象
    config = InputConfig(
        name=config_name,
        description=params.get("description", ""),
        repeat=params.get("repeat", 1),
        interval=params.get("interval", 0.0),
        actions=[InputAction.from_dict(a) for a in actions_data]
    )
    
    # 保存到文件
    config_path = _get_config_path(ctx, config_name)
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config.to_dict(), f, indent=2, ensure_ascii=False)
    
    logger.info(f"键鼠配置已保存: {config_path}")
    
    # 保存到 ctx
    save_to = params.get("save_to")
    if save_to:
        parts = save_to.split('.')
        current = ctx
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = config.to_dict()
    
    return config.to_dict()


@op("load_input_config", category="input", description="加载键鼠操作配置")
def op_load_input_config(ctx, params):
    """
    从文件加载键鼠操作配置
    
    参数:
        config_name: str — 配置名称（可选，与 from_ctx 二选一）
        from_ctx: str — 从 ctx 中读取配置，如 "input_configs.my_config"
        
    返回:
        加载的配置字典
    """
    from_ctx = params.get("from_ctx")
    
    if from_ctx:
        # 从上下文中加载
        parts = from_ctx.split('.')
        current = ctx
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                raise ValueError(f"上下文路径不存在: {from_ctx}")
        return current
    
    config_name = params.get("config_name")
    if not config_name:
        raise ValueError("config_name 或 from_ctx 参数必须提供")
    
    # 从文件加载
    config_path = _get_config_path(ctx, config_name)
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    logger.info(f"键鼠配置已加载: {config_path}")
    return data


@op("list_input_configs", category="input", description="列出所有键鼠配置")
def op_list_input_configs(ctx, params):
    """
    列出所有已保存的键鼠操作配置
    
    返回:
        配置信息列表
    """
    configs_dir = os.path.join(ctx.get("base_dir", BASE_DIR), "dynamic_configs", "input")
    if not os.path.exists(configs_dir):
        return []
    
    configs = []
    for f in os.listdir(configs_dir):
        if f.endswith('.json'):
            config_name = f[:-5]  # 去掉 .json 后缀
            config_path = os.path.join(configs_dir, f)
            try:
                with open(config_path, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    configs.append({
                        "name": config_name,
                        "description": data.get("description", ""),
                        "created_at": data.get("created_at", ""),
                        "action_count": len(data.get("actions", []))
                    })
            except Exception as e:
                logger.warning(f"读取配置失败 {f}: {e}")
    
    return configs


@op("delete_input_config", category="input", description="删除键鼠配置")
def op_delete_input_config(ctx, params):
    """
    删除键鼠操作配置
    
    参数:
        config_name: str — 配置名称（必填）
        
    返回:
        是否成功删除
    """
    config_name = params.get("config_name")
    if not config_name:
        raise ValueError("config_name 参数不能为空")
    
    config_path = _get_config_path(ctx, config_name)
    if os.path.exists(config_path):
        os.remove(config_path)
        logger.info(f"键鼠配置已删除: {config_path}")
        return True
    
    return False


# ==================== 单步操作 ====================

@op("mouse_click", category="input", description="鼠标点击")
def op_mouse_click(ctx, params):
    """
    执行鼠标点击操作
    
    参数:
        x: int — X坐标（可选，不传则当前位置）
        y: int — Y坐标（可选，不传则当前位置）
        button: str — 按钮：'left'/'right'/'middle'，默认 'left'
        clicks: int — 点击次数，默认 1
        interval: float — 多次点击间隔（秒），默认 0.0
    
    返回:
        点击的坐标 {"x": x, "y": y}
    """
    _ensure_pyautogui()
    
    x = params.get("x")
    y = params.get("y")
    button = params.get("button", "left")
    clicks = params.get("clicks", 1)
    interval = params.get("interval", 0.0)
    
    if x is not None and y is not None:
        pyautogui.click(x, y, clicks=clicks, interval=interval, button=button)
        logger.debug(f"鼠标点击: ({x}, {y}), 按钮: {button}, 次数: {clicks}")
        return {"x": x, "y": y}
    else:
        pyautogui.click(clicks=clicks, interval=interval, button=button)
        current_pos = pyautogui.position()
        logger.debug(f"鼠标点击当前位置: {current_pos}, 按钮: {button}, 次数: {clicks}")
        return {"x": current_pos.x, "y": current_pos.y}


@op("mouse_move", category="input", description="鼠标移动")
def op_mouse_move(ctx, params):
    """
    移动鼠标到指定位置
    
    参数:
        x: int — X坐标（必填）
        y: int — Y坐标（必填）
        duration: float — 移动持续时间（秒），默认 0.25
    
    返回:
        目标坐标 {"x": x, "y": y}
    """
    _ensure_pyautogui()
    
    x = params.get("x")
    y = params.get("y")
    if x is None or y is None:
        raise ValueError("x 和 y 参数必须提供")
    
    duration = params.get("duration", 0.25)
    pyautogui.moveTo(x, y, duration=duration)
    logger.debug(f"鼠标移动: ({x}, {y}), 耗时: {duration}s")
    
    return {"x": x, "y": y}


@op("mouse_scroll", category="input", description="鼠标滚轮")
def op_mouse_scroll(ctx, params):
    """
    滚动鼠标滚轮
    
    参数:
        clicks: int — 滚动格数，正值向上，负值向下，默认 3
        x: int — 滚动位置的X坐标（可选）
        y: int — 滚动位置的Y坐标（可选）
    
    返回:
        滚动的格数 {"clicks": clicks}
    """
    _ensure_pyautogui()
    
    clicks = params.get("clicks", 3)
    x = params.get("x")
    y = params.get("y")
    
    # 如果指定了坐标，先移动鼠标
    if x is not None and y is not None:
        pyautogui.moveTo(x, y)
    
    pyautogui.scroll(clicks)
    logger.debug(f"鼠标滚轮滚动: {clicks}")
    
    return {"clicks": clicks}


@op("mouse_drag", category="input", description="鼠标拖拽")
def op_mouse_drag(ctx, params):
    """
    拖拽鼠标
    
    参数:
        x: int — 目标X坐标（必填）
        y: int — 目标Y坐标（必填）
        duration: float — 拖拽持续时间（秒），默认 0.5
        button: str — 按钮：'left'/'right'/'middle'，默认 'left'
    
    返回:
        起始和目标坐标 {"from": {"x": x1, "y": y1}, "to": {"x": x2, "y": y2}}
    """
    _ensure_pyautogui()
    
    x = params.get("x")
    y = params.get("y")
    if x is None or y is None:
        raise ValueError("x 和 y 参数必须提供")
    
    duration = params.get("duration", 0.5)
    button = params.get("button", "left")
    
    start_pos = pyautogui.position()
    pyautogui.dragTo(x, y, duration=duration, button=button)
    logger.debug(f"鼠标拖拽: ({start_pos.x}, {start_pos.y}) -> ({x}, {y})")
    
    return {
        "from": {"x": start_pos.x, "y": start_pos.y},
        "to": {"x": x, "y": y}
    }


@op("key_press", category="input", description="按键")
def op_key_press(ctx, params):
    """
    按下并释放一个按键
    
    参数:
        key: str — 按键名（必填），如 'enter', 'esc', 'tab', 'space' 等
        presses: int — 按下次数，默认 1
        interval: float — 多次按键间隔（秒），默认 0.0
    
    返回:
        按键信息 {"key": key, "presses": presses}
    """
    _ensure_pyautogui()
    
    key = params.get("key")
    if not key:
        raise ValueError("key 参数不能为空")
    
    presses = params.get("presses", 1)
    interval = params.get("interval", 0.0)
    
    pyautogui.press(key, presses=presses, interval=interval)
    logger.debug(f"按键: {key}, 次数: {presses}")
    
    return {"key": key, "presses": presses}


@op("type_text", category="input", description="输入文本")
def op_type_text(ctx, params):
    """
    输入文本字符串
    
    参数:
        text: str — 要输入的文本（可选，不传则使用 last_result）
        interval: float — 每个字符输入间隔（秒），默认 0.0
    
    返回:
        输入的文本长度 {"text": text, "length": length}
    """
    _ensure_pyautogui()
    
    text = params.get("text")
    if text is None:
        text = str(ctx.get("last_result", ""))
    
    interval = params.get("interval", 0.0)
    
    pyautogui.typewrite(text, interval=interval)
    logger.debug(f"输入文本: {text[:50]}{'...' if len(text) > 50 else ''}")
    
    return {"text": text, "length": len(text)}


@op("hotkey", category="input", description="组合键")
def op_hotkey(ctx, params):
    """
    按下组合键
    
    参数:
        keys: list — 按键列表（必填），如 ["ctrl", "c"], ["alt", "tab"] 等
    
    返回:
        按下的组合键 {"keys": keys}
    """
    _ensure_pyautogui()
    
    keys = params.get("keys", [])
    if not keys:
        raise ValueError("keys 参数不能为空")
    
    pyautogui.hotkey(*keys)
    logger.debug(f"组合键: {'+'.join(keys)}")
    
    return {"keys": keys}


@op("wait", category="input", description="等待")
def op_wait(ctx, params):
    """
    等待指定秒数
    
    参数:
        seconds: float — 等待秒数（必填）
    
    返回:
        等待的秒数 {"seconds": seconds}
    """
    seconds = params.get("seconds")
    if seconds is None:
        raise ValueError("seconds 参数不能为空")
    
    time.sleep(seconds)
    logger.debug(f"等待: {seconds}s")
    
    return {"seconds": seconds}


@op("screenshot", category="input", description="截图")
def op_screenshot(ctx, params):
    """
    截取屏幕或指定区域
    
    参数:
        file: str — 保存路径（可选），如 "screenshots/capture.png"
        region: list — 区域 [left, top, width, height]（可选）
    
    返回:
        截图信息 {"file": file, "size": [width, height]}
    """
    _ensure_pyautogui()
    
    region = params.get("region")
    screenshot = pyautogui.screenshot(region=region)
    
    file_path = params.get("file")
    if file_path:
        # 解析路径
        if os.path.isabs(file_path):
            fp = file_path
        else:
            fp = os.path.join(ctx.get("base_dir", BASE_DIR), file_path)
        
        ensure_dir_exists(fp)
        screenshot.save(fp)
        logger.info(f"截图已保存: {fp}")
        return {"file": fp, "size": [screenshot.width, screenshot.height]}
    
    return {"size": [screenshot.width, screenshot.height]}


@op("get_mouse_position", category="input", description="获取鼠标位置")
def op_get_mouse_position(ctx, params):
    """
    获取当前鼠标位置
    
    返回:
        鼠标坐标 {"x": x, "y": y}
    """
    _ensure_pyautogui()
    
    pos = pyautogui.position()
    logger.debug(f"鼠标位置: ({pos.x}, {pos.y})")
    
    return {"x": pos.x, "y": pos.y}


@op("get_screen_size", category="input", description="获取屏幕尺寸")
def op_get_screen_size(ctx, params):
    """
    获取屏幕尺寸
    
    返回:
        屏幕尺寸 {"width": width, "height": height}
    """
    _ensure_pyautogui()
    
    size = pyautogui.size()
    return {"width": size.width, "height": size.height}


@op("find_and_click", category="input", description="查找图片并点击")
def op_find_and_click(ctx, params):
    """
    在屏幕上查找图片并点击
    
    参数:
        image: str — 图片路径（必填）
        confidence: float — 匹配置信度，默认 0.9（需要 opencv-python）
        grayscale: bool — 是否灰度匹配，默认 False
        clicks: int — 点击次数，默认 1
        button: str — 按钮，默认 'left'
    
    返回:
        点击坐标 {"x": x, "y": y, "found": True/False}
    """
    _ensure_pyautogui()
    
    image = params.get("image")
    if not image:
        raise ValueError("image 参数不能为空")
    
    # 解析图片路径
    if not os.path.isabs(image):
        image = os.path.join(ctx.get("base_dir", BASE_DIR), image)
    
    if not os.path.exists(image):
        raise FileNotFoundError(f"图片不存在: {image}")
    
    confidence = params.get("confidence", 0.9)
    grayscale = params.get("grayscale", False)
    clicks = params.get("clicks", 1)
    button = params.get("button", "left")
    
    try:
        location = pyautogui.locateOnScreen(
            image, 
            confidence=confidence, 
            grayscale=grayscale
        )
        
        if location:
            center = pyautogui.center(location)
            pyautogui.click(center.x, center.y, clicks=clicks, button=button)
            logger.info(f"找到图片并点击: {image} at ({center.x}, {center.y})")
            return {"x": center.x, "y": center.y, "found": True}
        else:
            logger.warning(f"未找到图片: {image}")
            return {"found": False}
            
    except Exception as e:
        logger.error(f"查找图片失败: {e}")
        return {"found": False, "error": str(e)}


# ==================== 批量执行操作 ====================

@op("execute_input_actions", category="input", description="执行键鼠操作序列")
def op_execute_input_actions(ctx, params):
    """
    执行键鼠操作序列（支持从配置加载或直接使用 actions 参数）
    
    参数:
        config_name: str — 配置名称（可选，与 actions 二选一）
        actions: list — 操作列表（可选，与 config_name 二选一）
        repeat: int — 重复执行次数，默认 1
        interval: float — 每次重复间隔（秒），默认 0
        save_results: bool — 是否保存每步结果，默认 False
        
    返回:
        执行结果列表
    """
    _ensure_pyautogui()
    
    # 获取操作列表
    if params.get("config_name"):
        config_data = op_load_input_config(ctx, {"config_name": params["config_name"]})
        config = InputConfig.from_dict(config_data)
        actions = config.actions
        repeat = params.get("repeat", config.repeat)
        interval = params.get("interval", config.interval)
    else:
        actions_data = params.get("actions", [])
        if not actions_data:
            raise ValueError("config_name 或 actions 参数必须提供")
        actions = [InputAction.from_dict(a) for a in actions_data]
        repeat = params.get("repeat", 1)
        interval = params.get("interval", 0.0)
    
    save_results = params.get("save_results", False)
    results = []
    
    # 执行操作
    action_map = {
        "mouse_click": op_mouse_click,
        "mouse_move": op_mouse_move,
        "mouse_scroll": op_mouse_scroll,
        "mouse_drag": op_mouse_drag,
        "key_press": op_key_press,
        "type_text": op_type_text,
        "hotkey": op_hotkey,
        "wait": op_wait,
        "screenshot": op_screenshot,
        "get_position": op_get_mouse_position,
        "get_mouse_position": op_get_mouse_position,
        "find_click": op_find_and_click,
        "find_and_click": op_find_and_click,
    }
    
    for r in range(repeat):
        logger.info(f"执行第 {r + 1}/{repeat} 轮操作")
        
        for i, action in enumerate(actions):
            try:
                handler = action_map.get(action.action_type)
                if not handler:
                    raise ValueError(f"未知的操作类型: {action.action_type}")
                
                if action.description:
                    logger.info(f"[{i+1}/{len(actions)}] {action.description}")
                
                result = handler(ctx, action.params)
                
                if save_results:
                    results.append({
                        "step": i + 1,
                        "action": action.action_type,
                        "description": action.description,
                        "result": result,
                        "success": True
                    })
                
            except Exception as e:
                logger.error(f"操作执行失败 [{action.action_type}]: {e}")
                if save_results:
                    results.append({
                        "step": i + 1,
                        "action": action.action_type,
                        "description": action.description,
                        "error": str(e),
                        "success": False
                    })
                raise  # 终止执行
        
        # 重复间隔
        if r < repeat - 1 and interval > 0:
            time.sleep(interval)
    
    logger.info(f"键鼠操作序列执行完成，共 {len(actions)} 步 x {repeat} 轮")
    
    return {
        "total_actions": len(actions),
        "repeat": repeat,
        "results": results if save_results else None
    }


# ==================== 录制功能 ====================

class InputRecorder:
    """键鼠录制器"""
    
    def __init__(self, min_move_distance: int = 5):
        self.actions: List[Dict] = []
        self.min_move_distance = min_move_distance  # 最小移动距离，小于此值不记录
        self.record_mouse_move = True  # 是否记录鼠标移动
        self.last_pos = None
        self.is_recording = False
        self.mouse_listener = None
        self.keyboard_listener = None
        self.last_time = None
        
    def _get_button_name(self, button) -> str:
        """获取鼠标按钮名称"""
        button_map = {
            MouseButton.left: "left",
            MouseButton.right: "right",
            MouseButton.middle: "middle"
        }
        return button_map.get(button, "left")
    
    def _get_key_name(self, key) -> str:
        """获取按键名称"""
        if isinstance(key, Key):
            return key.name if hasattr(key, 'name') else str(key)
        return key.char if hasattr(key, 'char') else str(key)
    
    def _calculate_distance(self, x1, y1, x2, y2) -> float:
        """计算两点距离"""
        return ((x2 - x1) ** 2 + (y2 - y1) ** 2) ** 0.5
    
    def _add_wait_action(self):
        """添加等待动作（根据时间差）"""
        current_time = time.time()
        if self.last_time is not None:
            elapsed = current_time - self.last_time
            if elapsed > 0.5:  # 如果间隔超过0.5秒，添加等待
                self.actions.append({
                    "action_type": "wait",
                    "params": {"seconds": round(elapsed, 2)},
                    "description": f"等待 {round(elapsed, 2)} 秒"
                })
        self.last_time = current_time
    
    def on_move(self, x, y):
        """鼠标移动事件"""
        if not self.is_recording:
            return
        
        # 如果不记录鼠标移动，直接返回
        if not self.record_mouse_move:
            self.last_pos = (x, y)
            return
            
        # 检查最小移动距离
        if self.last_pos is not None:
            distance = self._calculate_distance(self.last_pos[0], self.last_pos[1], x, y)
            if distance < self.min_move_distance:
                return
        
        self._add_wait_action()
        self.actions.append({
            "action_type": "mouse_move",
            "params": {"x": x, "y": y, "duration": 0.25},
            "description": f"鼠标移动到 ({x}, {y})"
        })
        self.last_pos = (x, y)
    
    def on_click(self, x, y, button, pressed):
        """鼠标点击事件"""
        if not self.is_recording or not pressed:
            return
        
        self._add_wait_action()
        button_name = self._get_button_name(button)
        self.actions.append({
            "action_type": "mouse_click",
            "params": {"x": x, "y": y, "button": button_name, "clicks": 1},
            "description": f"{button_name}键点击 ({x}, {y})"
        })
        self.last_pos = (x, y)
    
    def on_scroll(self, x, y, dx, dy):
        """鼠标滚轮事件"""
        if not self.is_recording:
            return
        
        self._add_wait_action()
        self.actions.append({
            "action_type": "mouse_scroll",
            "params": {"clicks": dy, "x": x, "y": y},
            "description": f"滚轮滚动 {dy} ({x}, {y})"
        })
        self.last_pos = (x, y)
    
    def on_press(self, key):
        """键盘按下事件"""
        if not self.is_recording:
            return
        
        # 检查是否是停止键（Esc）
        if key == Key.esc:
            self.stop()
            return
        
        self._add_wait_action()
        key_name = self._get_key_name(key)
        
        # 特殊处理一些常用组合键
        if key in [Key.ctrl_l, Key.ctrl_r, Key.alt_l, Key.alt_r, 
                   Key.shift_l, Key.shift_r, Key.cmd]:
            # 修饰键单独按下时，作为普通按键处理
            self.actions.append({
                "action_type": "key_press",
                "params": {"key": key_name},
                "description": f"按下 {key_name}"
            })
        else:
            self.actions.append({
                "action_type": "key_press",
                "params": {"key": key_name},
                "description": f"按下 {key_name}"
            })
    
    def start(self):
        """开始录制"""
        self.actions = []
        self.is_recording = True
        self.last_pos = None
        self.last_time = time.time()
        
        # 创建监听器
        self.mouse_listener = mouse.Listener(
            on_move=self.on_move,
            on_click=self.on_click,
            on_scroll=self.on_scroll
        )
        self.keyboard_listener = keyboard.Listener(
            on_press=self.on_press
        )
        
        self.mouse_listener.start()
        self.keyboard_listener.start()
        logger.info("录制开始... 按 Esc 键停止录制")
    
    def stop(self):
        """停止录制"""
        self.is_recording = False
        
        if self.mouse_listener:
            self.mouse_listener.stop()
        if self.keyboard_listener:
            self.keyboard_listener.stop()
        
        logger.info(f"录制停止，共录制 {len(self.actions)} 个操作")
        return self.actions
    
    def wait_for_stop(self, timeout: Optional[float] = None):
        """等待录制停止"""
        if timeout:
            start_time = time.time()
            while self.is_recording and (time.time() - start_time) < timeout:
                time.sleep(0.1)
            if self.is_recording:
                self.stop()
        else:
            # 无限等待，直到用户按 Esc
            while self.is_recording:
                time.sleep(0.1)
        
        return self.actions


@op("record_actions", category="input", description="录制键鼠操作")
def op_record_actions(ctx, params):
    """
    录制键鼠操作并保存为配置
    
    使用 pynput 库录制键鼠操作，支持多种采样频率模式
    
    参数:
        config_name: str — 保存的配置名称（必填）
        timeout: int/float — 录制超时时间（秒），默认 60，0 表示无限等待
        sampling_mode: str — 采样模式，可选:
            - "clicks_only" (默认): 只记录点击、滚轮、按键，不记录鼠标移动轨迹
            - "low": 低频记录移动，距离阈值 50 像素
            - "medium": 中频记录移动，距离阈值 20 像素  
            - "high": 高频记录移动，距离阈值 5 像素
            - "ultra": 超高频记录移动，距离阈值 1 像素
        save_to: str — 保存结果到 ctx 的路径，如 "input_configs.recorded"
        
    控制:
        按 Esc 键停止录制
        
    返回:
        录制的配置信息
    """
    _ensure_pyautogui()
    _ensure_pynput()
    
    config_name = params.get("config_name")
    if not config_name:
        raise ValueError("config_name 参数不能为空")
    
    timeout = params.get("timeout", 60)
    if timeout == 0:
        timeout = None
    
    # 采样模式对应的移动阈值
    sampling_mode = params.get("sampling_mode", "clicks_only")
    mode_thresholds = {
        "clicks_only": None,  # None 表示不记录移动
        "low": 50,            # 50像素阈值，低频
        "medium": 20,         # 20像素阈值，中频
        "high": 5,            # 5像素阈值，高频
        "ultra": 1            # 1像素阈值，超高频
    }
    
    if sampling_mode not in mode_thresholds:
        raise ValueError(f"无效的 sampling_mode: {sampling_mode}，可选: {list(mode_thresholds.keys())}")
    
    min_move_distance = mode_thresholds[sampling_mode]
    record_mouse_move = min_move_distance is not None
    
    logger.info("=" * 50)
    logger.info(f"开始录制键鼠操作: {config_name}")
    logger.info(f"采样模式: {sampling_mode}")
    if record_mouse_move:
        logger.info(f"移动阈值: {min_move_distance} 像素")
    else:
        logger.info("鼠标移动: 不记录（仅记录点击/滚轮/按键）")
    logger.info(f"超时时间: {timeout if timeout else '无限'} 秒")
    logger.info("提示: 按 Esc 键停止录制")
    logger.info("=" * 50)
    
    # 创建录制器
    recorder = InputRecorder(
        min_move_distance=min_move_distance if record_mouse_move else 999999
    )
    recorder.record_mouse_move = record_mouse_move
    recorder.start()
    
    try:
        # 等待录制停止
        actions = recorder.wait_for_stop(timeout=timeout)
    except KeyboardInterrupt:
        logger.info("用户中断录制")
        actions = recorder.stop()
    
    # 创建配置
    config = InputConfig(
        name=config_name,
        description=f"录制于 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [模式: {sampling_mode}]",
        repeat=1,
        interval=0.0,
        actions=[InputAction.from_dict(a) for a in actions]
    )
    
    # 保存到文件
    config_path = _get_config_path(ctx, config_name)
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config.to_dict(), f, indent=2, ensure_ascii=False)
    
    logger.info(f"录制完成！已保存 {len(actions)} 个操作到: {config_path}")
    
    # 保存到 ctx
    save_to = params.get("save_to")
    if save_to:
        parts = save_to.split('.')
        current = ctx
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = config.to_dict()
    
    return {
        "config_name": config_name,
        "action_count": len(actions),
        "sampling_mode": sampling_mode,
        "config_path": config_path,
        "actions": actions
    }


@op("run_recorded_config", category="input", description="运行录制的配置")
def op_run_recorded_config(ctx, params):
    """
    运行已录制的键鼠配置（便捷方法）
    
    参数:
        config_name: str — 配置名称（必填）
        repeat: int — 重复执行次数，默认 1
        interval: float — 每次重复间隔（秒），默认 0
        delay_before: float — 执行前延迟（秒），默认 3（给用户时间准备）
        save_results: bool — 是否保存每步结果，默认 False
        
    返回:
        执行结果
    """
    config_name = params.get("config_name")
    if not config_name:
        raise ValueError("config_name 参数不能为空")
    
    # 执行前延迟
    delay_before = params.get("delay_before", 3)
    if delay_before > 0:
        logger.info(f"{delay_before} 秒后开始执行...")
        time.sleep(delay_before)
    
    # 调用执行方法
    return op_execute_input_actions(ctx, {
        "config_name": config_name,
        "repeat": params.get("repeat", 1),
        "interval": params.get("interval", 0.0),
        "save_results": params.get("save_results", False)
    })


# ==================== OP_MAP 和入口 ====================

OP_MAP = {
    "create_input_config": op_create_input_config,
    "load_input_config": op_load_input_config,
    "list_input_configs": op_list_input_configs,
    "delete_input_config": op_delete_input_config,
    "execute_input_actions": op_execute_input_actions,
    "run_recorded_config": op_run_recorded_config,
    "record_actions": op_record_actions,
    "mouse_click": op_mouse_click,
    "mouse_move": op_mouse_move,
    "mouse_scroll": op_mouse_scroll,
    "mouse_drag": op_mouse_drag,
    "key_press": op_key_press,
    "type_text": op_type_text,
    "hotkey": op_hotkey,
    "wait": op_wait,
    "screenshot": op_screenshot,
    "get_mouse_position": op_get_mouse_position,
    "get_screen_size": op_get_screen_size,
    "find_and_click": op_find_and_click,
}


def run(config_path=None):
    """模块测试入口"""
    PipelineEngine.main(OP_MAP, cfg=config_path, init_ctx=lambda: {"base_dir": BASE_DIR})


if __name__ == '__main__':
    run()
