"""
StarL3 结构化日志系统
"""

import json
import sys
import time
import os
from enum import IntEnum
from pathlib import Path
from typing import Any, Dict, Optional


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
    
    COLORS = {
        LogLevel.DEBUG: "\033[36m",
        LogLevel.INFO: "\033[34m",
        LogLevel.SUCCESS: "\033[32m",
        LogLevel.WARNING: "\033[33m",
        LogLevel.ERROR: "\033[31m",
        LogLevel.CRITICAL: "\033[35m",
        "RESET": "\033[0m",
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
        self.use_color = use_color and sys.platform != "win32"
        self.json_format = json_format
        self.log_file = log_file
        self._step_id: Optional[str] = None
        self._pipeline_name: Optional[str] = None
        
        if log_file:
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    
    def set_step(self, step_id: Optional[str]):
        self._step_id = step_id
    
    def set_pipeline(self, name: Optional[str]):
        self._pipeline_name = name
    
    def _format(self, level: LogLevel, message: str, extra: Optional[Dict] = None) -> str:
        """格式化日志消息"""
        if self.json_format:
            entry = {"level": level.name, "message": message}
            if self._pipeline_name:
                entry["pipeline"] = self._pipeline_name
            if self._step_id:
                entry["step_id"] = self._step_id
            if extra:
                entry["extra"] = extra
            return json.dumps(entry, ensure_ascii=False, default=str)
        
        parts = []
        if self._pipeline_name:
            parts.append(f"[{self._pipeline_name}]")
        if self._step_id:
            parts.append(f"[{self._step_id}]")
        parts.append(message)
        if extra:
            parts.extend(str(v) for v in extra.values())
        return " ".join(parts)
    
    def _write(self, text: str, level: LogLevel):
        colored = f"{self.COLORS.get(level, '')}{text}{self.COLORS['RESET']}" if self.use_color else text
        stream = sys.stderr if level >= LogLevel.ERROR else sys.stdout
        print(colored, file=stream)
        
        if self.log_file:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(text + "\n")
    
    def log(self, level: LogLevel, message: str, extra: Optional[Dict] = None):
        """记录日志"""
        if level < self.level:
            return
        self._write(self._format(level, message, extra), level)
    
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
    
    def step_error(self, step_id: str, error: Exception, context: Optional[Dict] = None):
        """记录步骤错误"""
        self.set_step(step_id)
        extra = context or {}
        extra["error_type"] = type(error).__name__
        self.error(f"步骤执行出错: {error}", extra=extra)


_default_logger: Optional[Logger] = None


def get_logger(name: str = "StarL3", level: Optional[LogLevel] = None) -> Logger:
    """获取或创建日志记录器"""
    global _default_logger
    
    if level is None:
        level_map = {"DEBUG": LogLevel.DEBUG, "INFO": LogLevel.INFO, 
                     "WARNING": LogLevel.WARNING, "ERROR": LogLevel.ERROR}
        env_level = os.environ.get("STARL3_LOG_LEVEL", "INFO").upper()
        level = level_map.get(env_level, LogLevel.INFO)
    
    if _default_logger is None:
        _default_logger = Logger(name=name, level=level)
    
    return _default_logger


class StepContext:
    """步骤上下文管理器"""
    
    def __init__(self, logger: Logger, step_id: str, op_type: str):
        self.logger = logger
        self.step_id = step_id
        self.op_type = op_type
        self.start_time: Optional[float] = None
    
    def __enter__(self):
        self.start_time = time.time()
        self.logger.set_step(self.step_id)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.time() - self.start_time if self.start_time else 0
        
        if exc_val is not None:
            self.logger.step_error(self.step_id, exc_val, {"elapsed": f"{elapsed:.3f}s"})
        else:
            self.logger.info(f"耗时: {elapsed:.3f}s")
        
        self.logger.set_step(None)
        return False
