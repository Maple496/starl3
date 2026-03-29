"""
调度监听模块
提供常驻监听、定时任务、文件监控等触发执行功能
"""

import os
import json
import time
import threading
import subprocess
from typing import Dict, List, Callable, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict

from core.pipeline_engine import PipelineEngine
from core.constants import BASE_DIR
from core.logger import get_logger
from core.registry import op
from core.path_utils import ensure_dir_exists

logger = get_logger("scheduler_ops")

# 可选依赖
try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent, FileDeletedEvent
    HAS_WATCHDOG = True
except ImportError:
    HAS_WATCHDOG = False
    logger.warning("watchdog 未安装，文件监控功能不可用。请运行: pip install watchdog")

try:
    import schedule
    HAS_SCHEDULE = True
except ImportError:
    HAS_SCHEDULE = False
    logger.warning("schedule 未安装，定时任务功能不可用。请运行: pip install schedule")


# ==================== 全局调度器状态 ====================

class SchedulerState:
    """调度器状态管理（单例模式）"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init()
        return cls._instance
    
    def _init(self):
        self.is_running = False
        self.observer: Optional[Observer] = None
        self.watches: Dict[str, Dict] = {}  # 监控任务 {path: {handler, config}}
        self.scheduled_jobs: List[Dict] = []  # 定时任务
        self.thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
    
    def start(self):
        """启动调度器"""
        if self.is_running:
            logger.info("调度器已在运行")
            return
        
        self.is_running = True
        self.stop_event.clear()
        
        # 启动文件监控
        if HAS_WATCHDOG and self.watches:
            self.observer = Observer()
            for path, watch_info in self.watches.items():
                self._add_watch(path, watch_info)
            self.observer.start()
            logger.info(f"文件监控已启动，共 {len(self.watches)} 个监控点")
        
        # 启动定时任务循环
        if HAS_SCHEDULE and self.scheduled_jobs:
            self.thread = threading.Thread(target=self._schedule_loop, daemon=True)
            self.thread.start()
            logger.info(f"定时任务已启动，共 {len(self.scheduled_jobs)} 个任务")
    
    def stop(self):
        """停止调度器"""
        if not self.is_running:
            return
        
        self.is_running = False
        self.stop_event.set()
        
        if self.observer:
            self.observer.stop()
            self.observer.join()
            self.observer = None
        
        if self.thread:
            self.thread.join(timeout=5)
            self.thread = None
        
        logger.info("调度器已停止")
    
    def _add_watch(self, path: str, watch_info: Dict):
        """添加文件监控"""
        if not HAS_WATCHDOG or not self.observer:
            return
        
        handler = watch_info.get("handler")
        if handler:
            self.observer.schedule(handler, path, recursive=watch_info.get("recursive", False))
    
    def _schedule_loop(self):
        """定时任务循环"""
        while self.is_running and not self.stop_event.is_set():
            if HAS_SCHEDULE:
                schedule.run_pending()
            time.sleep(1)


# 全局状态实例
scheduler_state = SchedulerState()


# ==================== 文件监控处理器 ====================

class PipelineTriggerHandler(FileSystemEventHandler):
    """文件变化触发 pipeline 执行"""
    
    def __init__(self, config_file: str, trigger_on: List[str], base_dir: str):
        self.config_file = config_file
        self.trigger_on = trigger_on  # ["created", "modified", "deleted"]
        self.base_dir = base_dir
        self.last_trigger = {}  # 防抖动
        self.cooldown = 2  # 秒
    
    def on_created(self, event):
        if "created" in self.trigger_on and not event.is_directory:
            self._trigger(event, "created")
    
    def on_modified(self, event):
        if "modified" in self.trigger_on and not event.is_directory:
            self._trigger(event, "modified")
    
    def on_deleted(self, event):
        if "deleted" in self.trigger_on and not event.is_directory:
            self._trigger(event, "deleted")
    
    def _trigger(self, event, action: str):
        """触发执行"""
        now = time.time()
        file_path = event.src_path
        
        # 防抖动检查
        if file_path in self.last_trigger:
            if now - self.last_trigger[file_path] < self.cooldown:
                return
        
        self.last_trigger[file_path] = now
        
        logger.info(f"【触发】文件{action}: {file_path}")
        
        # 在新线程中执行 pipeline，避免阻塞监控
        threading.Thread(
            target=self._run_pipeline,
            args=(file_path, action),
            daemon=True
        ).start()
    
    def _run_pipeline(self, file_path: str, action: str):
        """执行 pipeline"""
        try:
            # 导入 main_starl3 的 run_pipeline
            from main_starl3 import run_pipeline
            
            # 构建上下文
            ctx = {
                "base_dir": self.base_dir,
                "trigger_file": file_path,
                "trigger_action": action,
                "trigger_time": datetime.now().isoformat()
            }
            
            logger.info(f"【执行】开始运行: {self.config_file}")
            result = run_pipeline(self.config_file, self.base_dir, trigger_ctx=ctx)
            logger.info(f"【完成】{self.config_file} 执行成功")
            
        except Exception as e:
            logger.error(f"【失败】{self.config_file} 执行出错: {e}")


# ==================== 调度器操作 ====================

@op("watch_directory", category="scheduler", description="监控目录变化并触发执行")
def op_watch_directory(ctx, params):
    """
    监控目录，当文件变化时触发执行指定 pipeline
    
    参数:
        watch_path: str — 要监控的目录路径（必填）
        config_file: str — 触发时执行的 pipeline 配置文件（必填）
        trigger_on: list — 触发事件类型，默认 ["created", "modified"]
            可选: "created"(创建), "modified"(修改), "deleted"(删除)
        recursive: bool — 是否递归监控子目录，默认 false
        pattern: str — 文件过滤模式，如 "*.csv"，默认 "*"（所有文件）
        
    注意:
        需要常驻运行才能持续监控，配合 scheduler_run 使用
    """
    if not HAS_WATCHDOG:
        raise RuntimeError("watchdog 未安装，请运行: pip install watchdog")
    
    watch_path = params.get("watch_path")
    config_file = params.get("config_file")
    
    if not watch_path or not config_file:
        raise ValueError("watch_path 和 config_file 参数必填")
    
    watch_path = os.path.abspath(os.path.join(ctx.get("base_dir", BASE_DIR), watch_path))
    config_file = os.path.abspath(os.path.join(ctx.get("base_dir", BASE_DIR), config_file))
    
    if not os.path.exists(watch_path):
        raise FileNotFoundError(f"监控目录不存在: {watch_path}")
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"配置文件不存在: {config_file}")
    
    trigger_on = params.get("trigger_on", ["created", "modified"])
    recursive = params.get("recursive", False)
    
    # 创建处理器
    handler = PipelineTriggerHandler(
        config_file=config_file,
        trigger_on=trigger_on,
        base_dir=ctx.get("base_dir", BASE_DIR)
    )
    
    # 注册到全局状态
    scheduler_state.watches[watch_path] = {
        "handler": handler,
        "config_file": config_file,
        "trigger_on": trigger_on,
        "recursive": recursive
    }
    
    logger.info(f"【注册监控】{watch_path} -> 执行 {config_file}")
    logger.info(f"  触发条件: {', '.join(trigger_on)}")
    logger.info(f"  递归监控: {'是' if recursive else '否'}")
    
    return {
        "watch_path": watch_path,
        "config_file": config_file,
        "trigger_on": trigger_on,
        "status": "registered"
    }


@op("schedule_cron", category="scheduler", description="添加定时任务")
def op_schedule_cron(ctx, params):
    """
    添加定时执行的任务
    
    参数:
        config_file: str — 要执行的 pipeline 配置文件（必填）
        schedule_type: str — 调度类型（必填）
            - "interval": 固定间隔
            - "daily": 每天指定时间
            - "weekly": 每周指定时间
            - "once": 只执行一次
        
        # interval 类型参数
        interval_seconds: int — 间隔秒数
        interval_minutes: int — 间隔分钟
        interval_hours: int — 间隔小时
        
        # daily/weekly 类型参数
        at_time: str — 执行时间，格式 "HH:MM"
        day_of_week: str — 周几（weekly类型），如 "monday", "friday"
        
        # once 类型参数
        run_at: str — 执行时间，格式 "YYYY-MM-DD HH:MM:SS"
        
    注意:
        需要常驻运行才能持续调度，配合 scheduler_run 使用
    """
    if not HAS_SCHEDULE:
        raise RuntimeError("schedule 未安装，请运行: pip install schedule")
    
    config_file = params.get("config_file")
    schedule_type = params.get("schedule_type")
    
    if not config_file or not schedule_type:
        raise ValueError("config_file 和 schedule_type 参数必填")
    
    base_dir = ctx.get("base_dir", BASE_DIR)
    config_file = os.path.abspath(os.path.join(base_dir, config_file))
    
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"配置文件不存在: {config_file}")
    
    job_id = f"{schedule_type}_{config_file}_{time.time()}"
    
    def job_func():
        """任务执行函数"""
        try:
            from main_starl3 import run_pipeline
            logger.info(f"【定时执行】{config_file}")
            run_pipeline(config_file, base_dir)
        except Exception as e:
            logger.error(f"【定时执行失败】{config_file}: {e}")
    
    # 根据类型创建调度
    if schedule_type == "interval":
        seconds = params.get("interval_seconds", 0)
        minutes = params.get("interval_minutes", 0)
        hours = params.get("interval_hours", 0)
        
        total_seconds = seconds + minutes * 60 + hours * 3600
        if total_seconds <= 0:
            raise ValueError("interval 类型需要设置 interval_seconds/minutes/hours")
        
        schedule.every(total_seconds).seconds.do(job_func)
        desc = f"每 {total_seconds} 秒"
        
    elif schedule_type == "daily":
        at_time = params.get("at_time")
        if not at_time:
            raise ValueError("daily 类型需要 at_time 参数")
        schedule.every().day.at(at_time).do(job_func)
        desc = f"每天 {at_time}"
        
    elif schedule_type == "weekly":
        at_time = params.get("at_time")
        day = params.get("day_of_week", "monday").lower()
        if not at_time:
            raise ValueError("weekly 类型需要 at_time 参数")
        
        day_map = {
            "monday": schedule.every().monday,
            "tuesday": schedule.every().tuesday,
            "wednesday": schedule.every().wednesday,
            "thursday": schedule.every().thursday,
            "friday": schedule.every().friday,
            "saturday": schedule.every().saturday,
            "sunday": schedule.every().sunday,
        }
        if day not in day_map:
            raise ValueError(f"无效的 day_of_week: {day}")
        
        day_map[day].at(at_time).do(job_func)
        desc = f"每周{day} {at_time}"
        
    else:
        raise ValueError(f"无效的 schedule_type: {schedule_type}")
    
    job_info = {
        "job_id": job_id,
        "config_file": config_file,
        "schedule_type": schedule_type,
        "description": desc,
        "params": params
    }
    scheduler_state.scheduled_jobs.append(job_info)
    
    logger.info(f"【添加定时任务】{desc} -> 执行 {config_file}")
    
    return job_info


@op("scheduler_run", category="scheduler", description="启动调度器（阻塞运行）")
def op_scheduler_run(ctx, params):
    """
    启动调度器，保持程序常驻运行
    
    参数:
        timeout: int/float — 运行超时时间（秒），0或不传表示永久运行
        
    注意:
        这是一个阻塞操作，会保持程序运行直到超时或手动停止
        按 Ctrl+C 可以停止
    """
    timeout = params.get("timeout", 0)
    
    logger.info("=" * 50)
    logger.info("调度器启动")
    logger.info(f"监控任务: {len(scheduler_state.watches)} 个")
    logger.info(f"定时任务: {len(scheduler_state.scheduled_jobs)} 个")
    logger.info("按 Ctrl+C 停止")
    logger.info("=" * 50)
    
    scheduler_state.start()
    
    try:
        if timeout and timeout > 0:
            logger.info(f"将在 {timeout} 秒后自动停止")
            time.sleep(timeout)
        else:
            # 永久运行
            while scheduler_state.is_running:
                time.sleep(1)
    except KeyboardInterrupt:
        logger.info("收到停止信号")
    finally:
        scheduler_state.stop()
    
    return {
        "status": "stopped",
        "watches_count": len(scheduler_state.watches),
        "jobs_count": len(scheduler_state.scheduled_jobs)
    }


@op("scheduler_stop", category="scheduler", description="停止调度器")
def op_scheduler_stop(ctx, params):
    """
    停止运行中的调度器
    
    通常不需要手动调用，scheduler_run 会在程序退出时自动清理
    """
    scheduler_state.stop()
    return {"status": "stopped"}


@op("list_scheduled_jobs", category="scheduler", description="列出所有定时任务")
def op_list_scheduled_jobs(ctx, params):
    """列出已注册的所有定时任务和监控任务"""
    return {
        "watches": [
            {
                "path": path,
                "config_file": info["config_file"],
                "trigger_on": info["trigger_on"]
            }
            for path, info in scheduler_state.watches.items()
        ],
        "scheduled_jobs": scheduler_state.scheduled_jobs,
        "is_running": scheduler_state.is_running
    }


@op("clear_scheduled_jobs", category="scheduler", description="清除所有定时任务")
def op_clear_scheduled_jobs(ctx, params):
    """清除所有任务（谨慎使用）"""
    if HAS_SCHEDULE:
        schedule.clear()
    scheduler_state.watches.clear()
    scheduler_state.scheduled_jobs.clear()
    logger.info("所有任务已清除")
    return {"status": "cleared"}


# ==================== 便捷操作 ====================

@op("run_on_file_arrival", category="scheduler", description="文件到达即处理（便捷封装）")
def op_run_on_file_arrival(ctx, params):
    """
    监控目录，有新文件到达立即处理
    
    参数:
        watch_path: str — 监控目录（必填）
        config_file: str — 处理脚本（必填）
        pattern: str — 文件过滤，如 "*.csv"，默认 "*"
        
    这是一个便捷封装，等同于:
        watch_directory + scheduler_run
    """
    # 注册监控
    op_watch_directory(ctx, {
        "watch_path": params.get("watch_path"),
        "config_file": params.get("config_file"),
        "trigger_on": ["created"],
        "pattern": params.get("pattern", "*")
    })
    
    # 启动调度器（阻塞）
    return op_scheduler_run(ctx, {"timeout": params.get("timeout", 0)})


@op("run_periodically", category="scheduler", description="周期性执行（便捷封装）")
def op_run_periodically(ctx, params):
    """
    周期性执行指定脚本
    
    参数:
        config_file: str — 要执行的脚本（必填）
        interval_minutes: int — 间隔分钟数（必填）
        
    这是一个便捷封装，等同于:
        schedule_cron(interval) + scheduler_run
    """
    # 添加定时任务
    op_schedule_cron(ctx, {
        "config_file": params.get("config_file"),
        "schedule_type": "interval",
        "interval_minutes": params.get("interval_minutes", 60)
    })
    
    # 启动调度器
    return op_scheduler_run(ctx, {"timeout": params.get("timeout", 0)})


# ==================== OP_MAP ====================

OP_MAP = {
    "watch_directory": op_watch_directory,
    "schedule_cron": op_schedule_cron,
    "scheduler_run": op_scheduler_run,
    "scheduler_stop": op_scheduler_stop,
    "list_scheduled_jobs": op_list_scheduled_jobs,
    "clear_scheduled_jobs": op_clear_scheduled_jobs,
    "run_on_file_arrival": op_run_on_file_arrival,
    "run_periodically": op_run_periodically,
}


def run(config_path=None):
    """模块测试入口"""
    PipelineEngine.main(OP_MAP, cfg=config_path, init_ctx=lambda: {"base_dir": BASE_DIR})


if __name__ == '__main__':
    run()
