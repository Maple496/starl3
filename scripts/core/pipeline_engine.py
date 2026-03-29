# pipeline_engine.py
import re
import json
import os
import time
import traceback
from typing import Any, Callable, Dict, List, Optional

from .data_context import DataContext
from .logger import Logger, LogLevel, StepContext
from .safe_eval import eval_condition, SafeEvalError
from .context import PipelineContext


def _safe_json_dump(obj):
    """安全地打印对象，处理DataFrame等无法JSON序列化的类型"""
    try:
        # 尝试标准JSON序列化
        return json.dumps(obj, ensure_ascii=False, indent=2)
    except (TypeError, ValueError):
        # 如果失败，转换为字符串表示
        try:
            # 如果是DataFrame，使用to_json
            if hasattr(obj, 'to_json'):
                return obj.to_json(orient='records', force_ascii=False, indent=2)
            # 如果是DataFrame的groupby结果等
            elif hasattr(obj, 'to_string'):
                return obj.to_string()
            # 其他类型转为字符串
            else:
                return str(obj)
        except Exception:
            return str(obj)


class PipelineError(Exception):
    """Pipeline 执行错误"""
    
    def __init__(self, message: str, step_id: Optional[str] = None, 
                 original_error: Optional[Exception] = None):
        self.step_id = step_id
        self.original_error = original_error
        super().__init__(message)


class UserCancelledError(Exception):
    """用户取消操作"""
    pass


class PipelineEngine:
    """Pipeline 执行引擎"""
    
    def __init__(
        self,
        ops: Dict[str, Callable],
        init_ctx: Optional[Callable] = None,
        eval_vars_fn: Optional[Callable] = None,
        result_handler: Optional[Callable] = None,
        done_fn: Optional[Callable] = None,
        logger: Optional[Logger] = None
    ):
        self.m = ops
        self.i = init_ctx
        self.e = eval_vars_fn
        self.r = result_handler
        self.d = done_fn
        self.l = logger or Logger()
        
        self._name = ""
        self._t0 = 0
    
    def parse_pipeline(self, config_path: str) -> List[Dict[str, Any]]:
        """解析 pipeline 配置文件"""
        # self.l.info(f"解析配置文件: {config_path}")
        
        try:
            with open(config_path, encoding='utf-8') as f:
                data = json.load(f)
            
            rows = data.get("rows", data) if isinstance(data, dict) else data
            
            steps = []
            for row in rows:
                step = dict(zip(
                    ["step_id", "step_order", "op_type", "params_json", "enabled", "note"],
                    row
                ))
                
                # 解析 params_json
                raw_params = step.get("params_json")
                step["_has_params"] = raw_params is not None
                
                if isinstance(raw_params, str):
                    try:
                        step["params_json"] = json.loads(raw_params)
                    except json.JSONDecodeError as e:
                        step_id = step.get("step_id", "unknown")
                        raise PipelineError(
                            f"步骤 '{step_id}' 的 params_json 格式错误: {e}",
                            step_id=step_id
                        )
                else:
                    step["params_json"] = raw_params or {}
                
                steps.append(step)
            
            # 按 step_order 排序
            def sort_key(x):
                order = str(x.get('step_order', '0'))
                match = re.match(r'\d+', order)
                return int(match.group()) if match else 0
            
            steps.sort(key=sort_key)
            
            # self.l.info(f"成功解析 {len(steps)} 个步骤")
            return steps
            
        except FileNotFoundError:
            raise PipelineError(f"配置文件不存在: {config_path}")
        except json.JSONDecodeError as e:
            raise PipelineError(f"配置文件 JSON 格式错误: {e}")
        except Exception as e:
            raise PipelineError(f"解析配置文件失败: {e}")
    
    def run(self, steps: List[Dict[str, Any]], ctx: Dict[str, Any]) -> Dict[str, Any]:
        """执行 pipeline"""
        # 内置操作
        builtin_ops = {
            "end": self._op_end,
            "goto": self._op_goto,
            "condition": self._op_condition,
            "print": self._op_print,
            "load_step_result": self._op_load_step_result,
            "clear_context": self._op_clear_context,
        }
        
        i = 0
        base_dir = ctx.get("base_dir", "")
        executed_steps = []
        
        # self.l.info(f"开始执行 pipeline，共 {len(steps)} 个步骤")
        
        while 0 <= i < len(steps):
            step = steps[i]
            step_id = step.get("step_id", f"step_{i}")
            op_type = step.get("op_type", "")
            params = step.get("params_json", {})
            enabled = step.get("enabled", "Y")
            note = step.get("note", "")
            
            # 检查是否启用
            if enabled != "Y":
                self.l.debug(f"步骤 '{step_id}' 已禁用，跳过")
                i += 1
                continue
            
            # 使用步骤上下文管理器
            with StepContext(self.l, step_id, op_type):
                try:
                    if op_type in builtin_ops:
                        # 执行内置操作
                        result = builtin_ops[op_type](ctx, params, steps, i)
                        if result == -1:  # end 操作
                            # self.l.info("Pipeline 执行结束 (end 操作)")
                            break
                        i = result
                    else:
                        # 执行用户操作
                        i = self._execute_user_op(
                            ctx, step, params, base_dir, i
                        )
                    
                    executed_steps.append(step_id)
                    
                except UserCancelledError:
                    # 用户取消操作，优雅终止
                    self.l.info("用户取消了操作，Pipeline 终止")
                    raise
                    
                except Exception as e:
                    self.l.step_error(step_id, e, {
                        "op_type": op_type,
                        "params": params
                    })
                    
                    # 包装异常
                    if isinstance(e, PipelineError):
                        raise
                    
                    raise PipelineError(
                        f"步骤 '{step_id}' 执行失败: {e}",
                        step_id=step_id,
                        original_error=e
                    )
        
        ctx["_executed_steps"] = executed_steps
        ctx["_total_steps"] = len(steps)
        return ctx
    
    def _execute_user_op(
        self,
        ctx: Any,  # 支持 Dict 或 PipelineContext
        step: Dict[str, Any],
        params: Dict[str, Any],
        base_dir: str,
        index: int
    ) -> int:
        """执行用户定义的操作"""
        step_id = step.get("step_id", "")
        op_type = step.get("op_type", "")
        
        # 统一上下文访问方式：支持 dict 和 PipelineContext
        def get_ctx_value(key: str, default=None):
            if isinstance(ctx, PipelineContext):
                return getattr(ctx, key, default) if hasattr(ctx, key) else ctx.get(key, default)
            return ctx.get(key, default)
        
        def set_ctx_value(key: str, value: Any):
            if isinstance(ctx, PipelineContext):
                if hasattr(ctx, key):
                    setattr(ctx, key, value)
                else:
                    ctx.set(key, value)
            else:
                ctx[key] = value
        
        # 数据上下文管理
        last_result = get_ctx_value("last_result")
        
        if last_result is not None:
            data_ctx = DataContext.from_result(
                last_result, source="pipeline", base_dir=base_dir
            )
        else:
            data_ctx = DataContext(base_dir)
        
        # 保留上一步的文件上下文
        prev_ctx = get_ctx_value("_data_context")
        if prev_ctx and hasattr(prev_ctx, 'files') and prev_ctx.files \
           and not data_ctx.has_files():
            data_ctx.files = prev_ctx.files.copy()
        
        set_ctx_value("_data_context", data_ctx)
        
        # 自动注入文件路径
        if data_ctx.has_files() and not step.get("_has_params", False):
            if len(data_ctx.files) == 1:
                params["file_path"] = data_ctx.files[0]["abs_path"]
            elif len(data_ctx.files) > 0:
                if op_type == "add_attachment":
                    params["file_path"] = [f["abs_path"] for f in data_ctx.files]
                else:
                    params["attachments"] = [f["abs_path"] for f in data_ctx.files]
        
        # 检查操作是否存在
        if op_type not in self.m:
            raise PipelineError(
                f"未知的操作类型: '{op_type}'",
                step_id=step_id
            )
        
        # 执行操作
        self.l.debug(f"执行操作: {op_type}")
        result = self.m[op_type](ctx, params)
        
        # 处理结果
        result = self._process_result(result, base_dir)
        
        # 更新上下文
        if self.r:
            self.r(ctx, step_id, result, self.l)
        else:
            # 兼容 dict 和 PipelineContext
            if isinstance(ctx, PipelineContext):
                ctx.set_result(step_id, result)
            else:
                ctx.setdefault("results", {})[step_id] = result
                ctx["last_result"] = result
        
        return index + 1
    
    def _process_result(self, result: Any, base_dir: str) -> Any:
        """处理操作结果"""
        import pandas as pd
        
        if result is None:
            return None
        
        if isinstance(result, DataContext):
            return result
        
        if isinstance(result, pd.DataFrame):
            return result
        
        if isinstance(result, dict):
            # 保持特定格式
            if 'base_path' in result or 'items' in result:
                return result
            if 'status' in result:
                return result
        
        # 其他类型包装为 DataContext
        return DataContext.from_result(
            result, source="pipeline", base_dir=base_dir
        ).to_output()
    
    def _op_end(self, ctx, params, steps, index):
        """结束 pipeline"""
        return -1
    
    def _op_goto(self, ctx, params, steps, index):
        """跳转到指定步骤"""
        target = params.get("target")
        if not target:
            raise PipelineError("goto 操作需要 'target' 参数")
        
        for i, step in enumerate(steps):
            if step.get("step_id") == target:
                self.l.info(f"跳转到步骤: {target}")
                return i
        
        raise PipelineError(f"goto 目标步骤不存在: {target}")
    
    def _op_condition(self, ctx, params, steps, index):
        """条件跳转"""
        check_expr = params.get("check")
        then_target = params.get("then")
        else_target = params.get("else")
        
        if not check_expr:
            raise PipelineError("condition 操作需要 'check' 参数")
        
        # 准备变量环境
        variables = self.e(ctx) if self.e else ctx.copy()
        
        try:
            result = eval_condition(check_expr, variables)
        except SafeEvalError as e:
            raise PipelineError(f"条件表达式错误: {e}")
        
        target = then_target if result else else_target
        
        if target:
            for i, step in enumerate(steps):
                if step.get("step_id") == target:
                    self.l.info(f"条件 {result}，跳转到: {target}")
                    return i
            raise PipelineError(f"condition 目标步骤不存在: {target}")
        
        return index + 1
    
    def _op_print(self, ctx, params, steps, index):
        """打印当前结果"""
        step_id = params.get("step_id")
        if step_id:
            data = ctx.get("results", {}).get(step_id)
        else:
            data = ctx.get("last_result")
        
        output = _safe_json_dump(data)
        # self.l.info(f"[PRINT] {output}")
        return index + 1
    
    def _op_load_step_result(self, ctx, params, steps, index):
        """加载历史步骤结果"""
        step_id = params.get("step_id") or params.get("uid")
        if not step_id:
            raise PipelineError("load_step_result 需要 'step_id' 或 'uid' 参数")
        
        result = ctx.get("results", {}).get(step_id)
        if result is None:
            self.l.warning(f"步骤 '{step_id}' 的结果不存在")
        
        ctx["last_result"] = result
        return index + 1
    
    def _op_clear_context(self, ctx, params, steps, index):
        """清空上下文和内存
        
        参数:
            clear_results: bool - 是否清空所有历史步骤结果（默认 False）
            keep_steps: list - 保留指定步骤的结果（当 clear_results=True 时有效）
            gc: bool - 是否触发垃圾回收（默认 True）
        """
        import gc
        
        # 清空当前数据流
        old_result = ctx.get("last_result")
        ctx["last_result"] = None
        
        # 可选：清空历史结果
        if params.get("clear_results", False):
            keep_steps = set(params.get("keep_steps", []))
            results = ctx.get("results", {})
            removed = []
            
            for step_id in list(results.keys()):
                if step_id not in keep_steps:
                    del results[step_id]
                    removed.append(step_id)
            
            if removed:
                self.l.info(f"已清空 {len(removed)} 个历史步骤结果: {', '.join(removed[:5])}{'...' if len(removed) > 5 else ''}")
        
        # 清空数据上下文
        if "_data_context" in ctx:
            del ctx["_data_context"]
        
        # 触发垃圾回收
        if params.get("gc", True):
            gc.collect()
        
        # 记录内存释放
        if hasattr(old_result, '__class__') and 'DataFrame' in old_result.__class__.__name__:
            self.l.info(f"已清空 DataFrame 上下文，释放内存")
        else:
            self.l.info("上下文已清空")
        
        return index + 1
    
    def execute(self, config_path: str) -> Dict[str, Any]:
        """执行 pipeline 配置文件"""
        self._name = os.path.splitext(os.path.basename(config_path))[0]
        self._t0 = time.time()
        
        # 设置 logger 的 pipeline 名称
        self.l.set_pipeline(self._name)
        
        # self.l.info("=" * 50)
        # self.l.info(f"开始执行 Pipeline: {self._name}")
        # self.l.info("=" * 50)
        
        try:
            # 解析配置
            steps = self.parse_pipeline(os.path.abspath(config_path))
            
            # 初始化上下文
            ctx = self.i() if self.i else {}
            ctx.setdefault("base_dir", os.path.dirname(config_path))
            ctx.setdefault("last_result", None)
            ctx.setdefault("results", {})
            
            # 执行
            result = self.run(steps, ctx)
            
            # 执行完成回调
            elapsed = time.time() - self._t0
            self.l.info(f"总耗时: {elapsed:.3f}s")
            
            if self.d:
                self.d(result, self.l)
            
            return result
            
        except UserCancelledError:
            # 用户取消，优雅终止
            elapsed = time.time() - self._t0
            self.l.info(f"Pipeline 被用户取消，耗时: {elapsed:.3f}s")
            raise
        except PipelineError as e:
            elapsed = time.time() - self._t0
            self.l.error(
                f"Pipeline 执行失败！耗时: {elapsed:.3f}s",
                extra={"error": str(e), "step_id": e.step_id}
            )
            raise
        except Exception as e:
            elapsed = time.time() - self._t0
            self.l.error(
                f"Pipeline 执行发生未预期错误！耗时: {elapsed:.3f}s",
                extra={"error": str(e)},
                exc_info=e
            )
            raise PipelineError(f"未预期的错误: {e}", original_error=e)
    
    @classmethod
    def main(
        cls,
        ops: Dict[str, Callable],
        cfg: Optional[str] = None,
        **kwargs
    ):
        """主入口"""
        import sys
        
        config_path = cfg or (sys.argv[1] if len(sys.argv) > 1 else "config.json")
        
        # 初始化上下文
        init_ctx = kwargs.get("init_ctx", lambda: {})
        kwargs["init_ctx"] = lambda: {
            "last_result": None,
            "results": {},
            **init_ctx()
        }
        
        # 设置默认完成回调
        def default_done(ctx, logger):
            logger.success("Pipeline 执行流程结束!")
        
        kwargs.setdefault("done_fn", default_done)
        
        # 创建 logger
        logger = kwargs.pop("logger", None) or Logger()
        
        try:
            engine = cls(ops, logger=logger, **kwargs)
            return engine.execute(config_path)
        except PipelineError as e:
            print(f"\n[ERROR] Pipeline 执行失败: {e}")
            sys.exit(1)
        except KeyboardInterrupt:
            print("\n[WARNING] 用户中断执行")
            sys.exit(130)
        except Exception as e:
            print(f"\n[CRITICAL] 发生严重错误: {e}")
            traceback.print_exc()
            sys.exit(1)
