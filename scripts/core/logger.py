"""
StarL3 结构化日志系统
支持日志级别、彩色输出、文件记录、JSON格式
"""

import json
import sys
import time
import traceback
from datetime import datetime
from enum import IntEnum
from pathlib import Path
from typing import Any, Dict, Optional, Callable


class LogLevel(IntEnum):
    """日志级别"""
    DEBUG = 10
    INFO = 20
    SUCCESS = 25
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


class Logger:
    """结构化日志记录器"""
    
    #  ANSI 颜色代码
    COLORS = {
        LogLevel.DEBUG: "\033[36m",      # 青色
        LogLevel.INFO: "\033[34m",       # 蓝色
        LogLevel.SUCCESS: "\033[32m",    # 绿色
        LogLevel.WARNING: "\033[33m",    # 黄色
        LogLevel.ERROR: "\033[31m",      # 红色
        LogLevel.CRITICAL: "\033[35m",   # 紫色
        "RESET": "\033[0m",
        "BOLD": "\033[1m",
        "DIM": "\033[2m",
    }
    
    def __init__(
        self,
        name: str = "StarL3",
        level: LogLevel = LogLevel.INFO,
        use_color: bool = True,
        log_file: Optional[str] = None,
        json_format: bool = False
    ):
        self.name = name
        self.level = level
        self.use_color = use_color and sys.platform != "win32"  # Windows 默认不支持 ANSI
        self.json_format = json_format
        self.log_file = log_file
        self._step_id: Optional[str] = None
        self._pipeline_name: Optional[str] = None
        
        # 如果指定了日志文件，确保目录存在
        if log_file:
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    
    def set_step(self, step_id: Optional[str]):
        """设置当前步骤ID"""
        self._step_id = step_id
    
    def set_pipeline(self, name: Optional[str]):
        """设置当前 pipeline 名称"""
        self._pipeline_name = name
    
    def _should_log(self, level: LogLevel) -> bool:
        """检查是否应该记录该级别的日志"""
        return level >= self.level
    
    def _format_message(
        self,
        level: LogLevel,
        message: str,
        extra: Optional[Dict[str, Any]] = None,
        exc_info: Optional[Exception] = None
    ) -> str:
        """格式化日志消息"""
        if self.json_format:
            # JSON 格式
            log_entry = {
                "level": level.name,
                "logger": self.name,
                "message": message,
            }
            if self._pipeline_name:
                log_entry["pipeline"] = self._pipeline_name
            if self._step_id:
                log_entry["step_id"] = self._step_id
            if extra:
                log_entry["extra"] = extra
            if exc_info:
                log_entry["exception"] = {
                    "type": type(exc_info).__name__,
                    "message": str(exc_info),
                    "traceback": traceback.format_exception(
                        type(exc_info), exc_info, exc_info.__traceback__
                    )
                }
            return json.dumps(log_entry, ensure_ascii=False, default=str)
        else:
            # 文本格式 - 无时间戳
            parts = []
            
            if self._pipeline_name:
                parts.append(f"[{self._pipeline_name}]")
            if self._step_id:
                parts.append(f"[{self._step_id}]")
            
            # 如果有消息内容，添加消息
            if message:
                parts.append(message)
            
            # 将 extra 信息直接追加（不括起来）
            if extra:
                extra_str = " ".join([f"{v}" for k, v in extra.items()])
                if extra_str:
                    parts.append(extra_str)
            
            return " ".join(parts)
    
    def _colorize(self, text: str, level: LogLevel) -> str:
        """添加颜色"""
        if not self.use_color:
            return text
        color = self.COLORS.get(level, "")
        reset = self.COLORS["RESET"]
        return f"{color}{text}{reset}"
    
    def _write(self, text: str, level: LogLevel):
        """写入日志"""
        colored_text = self._colorize(text, level)
        
        # 输出到控制台
        if level >= LogLevel.ERROR:
            print(colored_text, file=sys.stderr)
        else:
            print(colored_text)
        
        # 写入文件（无颜色）
        if self.log_file:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(text + "\n")
    
    def log(
        self,
        level: LogLevel,
        message: str,
        extra: Optional[Dict[str, Any]] = None,
        exc_info: Optional[Exception] = None
    ):
        """记录日志"""
        if not self._should_log(level):
            return
        
        formatted = self._format_message(level, message, extra, exc_info)
        self._write(formatted, level)
    
    # 快捷方法
    def debug(self, message: str, **kwargs):
        self.log(LogLevel.DEBUG, message, **kwargs)
    
    def info(self, message: str, **kwargs):
        self.log(LogLevel.INFO, message, **kwargs)
    
    def success(self, message: str, **kwargs):
        self.log(LogLevel.SUCCESS, message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        self.log(LogLevel.WARNING, message, **kwargs)
    
    def error(self, message: str, **kwargs):
        self.log(LogLevel.ERROR, message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        self.log(LogLevel.CRITICAL, message, **kwargs)
    
    def step_start(self, step_id: str, op_type: str):
        """记录步骤开始 - 不输出"""
        self.set_step(step_id)
        # 静默开始，不输出日志
    
    def step_end(self, step_id: str, success: bool = True, result_summary: str = ""):
        """记录步骤结束 - 简洁格式"""
        self.set_step(step_id)
        if success:
            # 只输出耗时信息，不输出"步骤执行完成"文字
            if result_summary:
                self.info("", extra={"": result_summary})
        else:
            self.error("", extra={"": result_summary})
    
    def step_error(self, step_id: str, error: Exception, context: Optional[Dict] = None):
        """记录步骤错误"""
        self.set_step(step_id)
        extra = context or {}
        extra["error_type"] = type(error).__name__
        self.error(
            f"步骤执行出错: {error}",
            extra=extra,
            exc_info=error
        )


# 全局日志记录器实例
_default_logger: Optional[Logger] = None


def get_logger(
    name: str = "StarL3",
    level: Optional[LogLevel] = None,
    log_file: Optional[str] = None,
    json_format: bool = False
) -> Logger:
    """获取或创建日志记录器"""
    global _default_logger
    
    # 从环境变量读取日志级别
    if level is None:
        level_map = {
            "DEBUG": LogLevel.DEBUG,
            "INFO": LogLevel.INFO,
            "WARNING": LogLevel.WARNING,
            "ERROR": LogLevel.ERROR,
            "CRITICAL": LogLevel.CRITICAL,
        }
        env_level = os.environ.get("STARL3_LOG_LEVEL", "INFO").upper()
        level = level_map.get(env_level, LogLevel.INFO)
    
    # 从环境变量读取日志文件
    if log_file is None:
        log_file = os.environ.get("STARL3_LOG_FILE")
    
    # 从环境变量读取是否使用 JSON 格式
    if not json_format:
        json_format = os.environ.get("STARL3_LOG_JSON", "false").lower() == "true"
    
    if _default_logger is None:
        _default_logger = Logger(
            name=name,
            level=level,
            log_file=log_file,
            json_format=json_format
        )
    
    return _default_logger


def reset_logger():
    """重置日志记录器（用于测试）"""
    global _default_logger
    _default_logger = None


# 便捷函数
def debug(msg: str, **kwargs):
    get_logger().debug(msg, **kwargs)


def info(msg: str, **kwargs):
    get_logger().info(msg, **kwargs)


def success(msg: str, **kwargs):
    get_logger().success(msg, **kwargs)


def warning(msg: str, **kwargs):
    get_logger().warning(msg, **kwargs)


def error(msg: str, **kwargs):
    get_logger().error(msg, **kwargs)


def critical(msg: str, **kwargs):
    get_logger().critical(msg, **kwargs)


# 上下文管理器用于临时设置步骤
class StepContext:
    """步骤上下文管理器"""
    
    def __init__(self, logger: Logger, step_id: str, op_type: str):
        self.logger = logger
        self.step_id = step_id
        self.op_type = op_type
        self.start_time: Optional[float] = None
    
    def __enter__(self):
        self.start_time = time.time()
        self.logger.step_start(self.step_id, self.op_type)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.time() - self.start_time if self.start_time else 0
        
        if exc_val is not None:
            self.logger.step_error(self.step_id, exc_val, {"elapsed": f"{elapsed:.3f}s"})
        else:
            self.logger.step_end(
                self.step_id,
                success=True,
                result_summary=f"耗时: {elapsed:.3f}s"
            )
        
        self.logger.set_step(None)
        return False  # 不抑制异常


import os
