"""
StarL3 任务管理器
管理多个 Pipeline 任务的并发执行
"""

import threading
import uuid
import time
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict, Callable
from datetime import datetime


class TaskStatus(Enum):
    """任务状态"""
    PENDING = "pending"      # 等待中
    RUNNING = "running"      # 运行中
    PAUSED = "paused"        # 已暂停
    STOPPING = "stopping"    # 正在停止
    COMPLETED = "completed"  # 已完成
    ERROR = "error"          # 出错


@dataclass
class Task:
    """任务对象"""
    id: str
    name: str
    config_path: str
    status: TaskStatus = TaskStatus.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    current_step: str = ""
    progress: float = 0.0  # 0-100
    error_msg: str = ""
    
    # 线程控制
    _thread: Optional[threading.Thread] = field(default=None, repr=False)
    _pause_event: threading.Event = field(default_factory=threading.Event, repr=False)
    _stop_event: threading.Event = field(default_factory=threading.Event, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    
    # 日志捕获
    _logs: list = field(default_factory=list, repr=False)
    _log_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    
    def __post_init__(self):
        self._pause_event.set()  # 默认不暂停
    
    def add_log(self, message: str):
        """添加日志行"""
        with self._log_lock:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self._logs.append(f"[{timestamp}] {message}")
    
    def get_logs(self) -> list:
        """获取所有日志"""
        with self._log_lock:
            return self._logs.copy()
    
    def to_dict(self) -> dict:
        """转换为字典（用于JSON序列化）"""
        return {
            "id": self.id,
            "name": self.name,
            "config_path": self.config_path,
            "status": self.status.value,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "current_step": self.current_step,
            "progress": round(self.progress, 2),
            "error_msg": self.error_msg,
        }


class TaskManager:
    """任务管理器"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """单例模式"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        
        self._tasks: Dict[str, Task] = {}
        self._tasks_lock = threading.Lock()
        self._step_callback: Optional[Callable[[str, str, float], None]] = None
    
    def set_step_callback(self, callback: Callable[[str, str, float], None]):
        """设置步骤进度回调函数 callback(task_id, step_name, progress)"""
        self._step_callback = callback
    
    def start_task(self, config_path: str) -> str:
        """
        启动新任务
        
        Args:
            config_path: 配置文件路径
            
        Returns:
            task_id: 任务ID
        """
        import os
        
        # 生成任务ID
        task_id = str(uuid.uuid4())[:8]
        task_name = os.path.splitext(os.path.basename(config_path))[0]
        
        # 创建任务对象
        task = Task(
            id=task_id,
            name=task_name,
            config_path=config_path,
            start_time=datetime.now(),
        )
        
        # 包装执行函数
        def run_wrapper():
            try:
                task.status = TaskStatus.RUNNING
                
                # 运行 pipeline
                result = run_pipeline_with_hooks(
                    config_path=config_path,
                    task=task,
                    progress_callback=self._step_callback,
                )
                
                if task.status != TaskStatus.ERROR and task.status != TaskStatus.STOPPING:
                    task.status = TaskStatus.COMPLETED
                    task.progress = 100.0
                    
            except InterruptedError:
                task.status = TaskStatus.STOPPING
                task.error_msg = "任务被用户停止"
            except Exception as e:
                task.status = TaskStatus.ERROR
                task.error_msg = str(e)
                print(f"[ERROR] 任务 {task_id} 执行失败: {e}")
            finally:
                task.end_time = datetime.now()
        
        # 启动线程
        thread = threading.Thread(target=run_wrapper, daemon=True)
        task._thread = thread
        
        with self._tasks_lock:
            self._tasks[task_id] = task
        
        thread.start()
        return task_id
    
    def pause_task(self, task_id: str) -> bool:
        """暂停任务"""
        with self._tasks_lock:
            task = self._tasks.get(task_id)
            if not task:
                return False
            if task.status == TaskStatus.RUNNING:
                task._pause_event.clear()  # 清除标志，暂停执行
                task.status = TaskStatus.PAUSED
                return True
        return False
    
    def resume_task(self, task_id: str) -> bool:
        """恢复任务"""
        with self._tasks_lock:
            task = self._tasks.get(task_id)
            if not task:
                return False
            if task.status == TaskStatus.PAUSED:
                task._pause_event.set()  # 设置标志，恢复执行
                task.status = TaskStatus.RUNNING
                return True
        return False
    
    def stop_task(self, task_id: str) -> bool:
        """停止任务"""
        with self._tasks_lock:
            task = self._tasks.get(task_id)
            if not task:
                return False
            if task.status in [TaskStatus.RUNNING, TaskStatus.PAUSED, TaskStatus.PENDING]:
                task._stop_event.set()  # 设置停止标志
                task._pause_event.set()  # 确保从暂停状态恢复以便退出
                task.status = TaskStatus.STOPPING
                return True
        return False
    
    def get_task(self, task_id: str) -> Optional[Task]:
        """获取任务"""
        with self._tasks_lock:
            return self._tasks.get(task_id)
    
    def list_tasks(self) -> list:
        """列出所有任务"""
        with self._tasks_lock:
            return [task.to_dict() for task in self._tasks.values()]
    
    def remove_task(self, task_id: str) -> bool:
        """移除已完成的任务"""
        with self._tasks_lock:
            task = self._tasks.get(task_id)
            if task and task.status in [TaskStatus.COMPLETED, TaskStatus.ERROR]:
                del self._tasks[task_id]
                return True
        return False
    
    def stop_all_tasks(self, timeout: float = 5.0):
        """停止所有任务"""
        with self._tasks_lock:
            tasks_copy = list(self._tasks.values())
        
        # 发送停止信号
        for task in tasks_copy:
            self.stop_task(task.id)
        
        # 等待任务结束
        for task in tasks_copy:
            if task._thread and task._thread.is_alive():
                task._thread.join(timeout=timeout)


class InterruptiblePipelineEngine:
    """可中断的 Pipeline 引擎包装器"""
    
    def __init__(self, task: Task, progress_callback: Optional[Callable] = None):
        self.task = task
        self.progress_callback = progress_callback
        self.total_steps = 0
        self.current_step_idx = 0
    
    def check_control(self):
        """检查暂停/停止信号"""
        # 检查停止信号
        if self.task._stop_event.is_set():
            raise InterruptedError("任务被用户停止")
        
        # 检查暂停信号
        while not self.task._pause_event.is_set() and not self.task._stop_event.is_set():
            time.sleep(0.1)
        
        if self.task._stop_event.is_set():
            raise InterruptedError("任务被用户停止")
    
    def run(self, engine, steps, ctx):
        """包装执行方法，添加控制检查"""
        self.total_steps = len(steps)
        
        # 导入需要的内置操作
        from core.pipeline_engine import PipelineError, UserCancelledError
        from core.logger import StepContext
        
        builtin_ops = {
            "end": engine._op_end,
            "goto": engine._op_goto,
            "condition": engine._op_condition,
            "print": engine._op_print,
            "load_step_result": engine._op_load_step_result,
            "clear_context": engine._op_clear_context,
        }
        
        i = 0
        base_dir = ctx.get("base_dir", "")
        executed_steps = []
        
        self.task.add_log(f"[INFO] 开始执行 Pipeline: {engine._name}")
        self.task.add_log(f"[INFO] 共 {len(steps)} 个步骤")
        
        while 0 <= i < len(steps):
            # 检查控制信号
            self.check_control()
            
            step = steps[i]
            step_id = step.get("step_id", f"step_{i}")
            op_type = step.get("op_type", "")
            params = step.get("params_json", {})
            enabled = step.get("enabled", "Y")
            
            # 更新任务状态
            self.task.current_step = step_id
            self.current_step_idx = i
            if self.total_steps > 0:
                self.task.progress = (i / self.total_steps) * 100
            
            # 上报进度
            if self.progress_callback:
                self.progress_callback(self.task.id, step_id, self.task.progress)
            
            # 检查是否启用
            if enabled != "Y":
                self.task.add_log(f"[SKIP] 步骤 {step_id} 已禁用，跳过")
                i += 1
                continue
            
            # 使用步骤上下文管理器
            with StepContext(engine.logger, step_id, op_type):
                try:
                    self.task.add_log(f"[INFO] 执行步骤 {step_id}: {op_type}")
                    
                    if op_type in builtin_ops:
                        result = builtin_ops[op_type](ctx, params, steps, i)
                        if result == -1:  # end 操作
                            self.task.add_log(f"[INFO] 遇到 end 操作，Pipeline 结束")
                            break
                        i = result
                    else:
                        # 执行用户操作
                        i = engine._execute_user_op(ctx, step, params, base_dir, i)
                    
                    executed_steps.append(step_id)
                    self.task.add_log(f"[INFO] 步骤 {step_id} 完成")
                    
                except UserCancelledError:
                    self.task.add_log("[WARN] 用户取消了操作，Pipeline 终止")
                    raise
                    
                except Exception as e:
                    error_msg = f"步骤 '{step_id}' 执行失败: {e}"
                    self.task.add_log(f"[ERROR] {error_msg}")
                    engine.logger.step_error(step_id, e, {
                        "op_type": op_type,
                        "params": params
                    })
                    
                    if isinstance(e, PipelineError):
                        raise
                    
                    raise PipelineError(
                        error_msg,
                        step_id=step_id,
                        original_error=e
                    )
        
        self.task.add_log(f"[INFO] Pipeline 执行完成，成功执行 {len(executed_steps)} 个步骤")
        ctx["_executed_steps"] = executed_steps
        ctx["_total_steps"] = len(steps)
        return ctx


def run_pipeline_with_hooks(config_path: str, 
                            task: Task,
                            progress_callback: Optional[Callable[[str, str, float], None]] = None):
    """
    带钩子函数的 pipeline 运行器
    
    Args:
        config_path: 配置文件路径
        task: 任务对象（包含暂停/停止事件）
        progress_callback: 进度回调函数 (task_id, step_name, progress)
    """
    from core.pipeline_engine import PipelineEngine
    from core.context import create_context
    from core.registry import auto_discover, OpRegistry
    from core.constants import DATA_DIR
    import os
    import time
    
    # 确保所有操作已注册
    ops_map = OpRegistry.get_op_map()
    if not ops_map:
        ops_map = auto_discover()
    
    # 创建 Pipeline 引擎
    engine = PipelineEngine(
        ops=ops_map,
        init_ctx=lambda: create_context(base_dir=DATA_DIR).to_dict()
    )
    
    # 设置 pipeline 名称
    engine._name = os.path.splitext(os.path.basename(config_path))[0]
    engine._t0 = time.time()
    engine.logger.set_pipeline(engine._name)
    
    # 解析配置
    steps = engine.parse_pipeline(os.path.abspath(config_path))
    
    # 初始化上下文
    ctx = engine.init_ctx() if engine.init_ctx else {}
    ctx.setdefault("base_dir", os.path.dirname(config_path))
    ctx.setdefault("last_result", None)
    ctx.setdefault("results", {})
    
    # 使用可中断的执行器
    interruptible = InterruptiblePipelineEngine(task, progress_callback)
    result = interruptible.run(engine, steps, ctx)
    
    # 执行完成
    elapsed = time.time() - engine._t0
    engine.logger.info(f"总耗时: {elapsed:.3f}s")
    
    return result


# 全局任务管理器实例
task_manager = TaskManager()
