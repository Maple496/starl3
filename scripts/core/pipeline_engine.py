# pipeline_engine.py
import re
import json
import os
import time
import traceback
from typing import Any, Callable, Dict, List, Optional

from .context import DataContext
from .logger import Logger, StepContext
from .safe_eval import eval_condition, SafeEvalError


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
        self.ops = ops
        self.init_ctx = init_ctx
        self.eval_vars_fn = eval_vars_fn
        self.result_handler = result_handler
        self.done_fn = done_fn
        self.logger = logger or Logger()
        self._name = ""
        self._t0 = 0
    
    def parse_pipeline(self, config_path: str) -> List[Dict[str, Any]]:
        """解析 pipeline 配置文件"""
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
                
                raw_params = step.get("params_json")
                step["_has_params"] = raw_params is not None
                
                if isinstance(raw_params, str):
                    try:
                        step["params_json"] = json.loads(raw_params)
                    except json.JSONDecodeError as e:
                        step_id = step.get("step_id", "unknown")
                        raise PipelineError(f"步骤 '{step_id}' 的 params_json 格式错误: {e}", step_id=step_id)
                else:
                    step["params_json"] = raw_params or {}
                
                steps.append(step)
            
            # 按 step_order 排序（提取数字部分）
            def sort_key(x):
                order = str(x.get('step_order', '0'))
                match = re.match(r'\d+', order)
                return int(match.group()) if match else 0
            
            steps.sort(key=sort_key)
            return steps
            
        except FileNotFoundError:
            raise PipelineError(f"配置文件不存在: {config_path}")
        except json.JSONDecodeError as e:
            raise PipelineError(f"配置文件 JSON 格式错误: {e}")
    
    def run(self, steps: List[Dict[str, Any]], ctx: Dict[str, Any]) -> Dict[str, Any]:
        """执行 pipeline"""
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
        
        while 0 <= i < len(steps):
            step = steps[i]
            step_id = step.get("step_id", f"step_{i}")
            op_type = step.get("op_type", "")
            params = step.get("params_json", {})
            enabled = step.get("enabled", "Y")
            
            if enabled != "Y":
                i += 1
                continue
            
            with StepContext(self.logger, step_id, op_type):
                try:
                    if op_type in builtin_ops:
                        result = builtin_ops[op_type](ctx, params, steps, i)
                        if result == -1:
                            break
                        i = result
                    else:
                        i = self._execute_user_op(ctx, step, params, base_dir, i)
                    
                    executed_steps.append(step_id)
                    
                except UserCancelledError:
                    self.logger.info("用户取消了操作，Pipeline 终止")
                    raise
                except Exception as e:
                    self.logger.step_error(step_id, e, {"op_type": op_type, "params": params})
                    if isinstance(e, PipelineError):
                        raise
                    raise PipelineError(f"步骤 '{step_id}' 执行失败: {e}", step_id=step_id, original_error=e)
        
        ctx["_executed_steps"] = executed_steps
        ctx["_total_steps"] = len(steps)
        return ctx
    
    def _execute_user_op(self, ctx, step, params, base_dir, index):
        """执行用户定义的操作"""
        step_id = step.get("step_id", "")
        op_type = step.get("op_type", "")
        
        # 统一上下文访问辅助函数
        def get_ctx(key, default=None):
            return ctx.get(key, default)
        
        def set_ctx(key, value):
            ctx[key] = value
        
        # 数据上下文管理
        last_result = get_ctx("last_result")
        data_ctx = DataContext.from_result(last_result, source="pipeline", base_dir=base_dir) if last_result is not None else DataContext(base_dir)
        
        # 保留上一步的文件上下文
        prev_ctx = get_ctx("_data_context")
        if prev_ctx and hasattr(prev_ctx, 'files') and prev_ctx.files and not data_ctx.has_files():
            data_ctx.files = prev_ctx.files.copy()
        
        set_ctx("_data_context", data_ctx)
        
        # 自动注入文件路径
        if data_ctx.has_files() and not step.get("_has_params", False):
            if len(data_ctx.files) == 1:
                params["file_path"] = data_ctx.files[0]["abs_path"]
            elif len(data_ctx.files) > 0:
                params["attachments"] = [f["abs_path"] for f in data_ctx.files]
        
        if op_type not in self.ops:
            raise PipelineError(f"未知的操作类型: '{op_type}'", step_id=step_id)
        
        self.logger.debug(f"执行操作: {op_type}")
        result = self.ops[op_type](ctx, params)
        result = self._process_result(result, base_dir)
        
        if self.result_handler:
            self.result_handler(ctx, step_id, result, self.logger)
        else:
            ctx.setdefault("results", {})[step_id] = result
            ctx["last_result"] = result
        
        return index + 1
    
    def _process_result(self, result: Any, base_dir: str) -> Any:
        """处理操作结果"""
        import pandas as pd
        
        if result is None or isinstance(result, (DataContext, pd.DataFrame)):
            return result
        
        if isinstance(result, dict) and any(k in result for k in ('base_path', 'items', 'status')):
            return result
        
        return DataContext.from_result(result, source="pipeline", base_dir=base_dir).to_output()
    
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
                self.logger.info(f"跳转到步骤: {target}")
                return i
        
        raise PipelineError(f"goto 目标步骤不存在: {target}")
    
    def _op_condition(self, ctx, params, steps, index):
        """条件跳转"""
        check_expr = params.get("check")
        then_target = params.get("then")
        else_target = params.get("else")
        
        if not check_expr:
            raise PipelineError("condition 操作需要 'check' 参数")
        
        variables = self.eval_vars_fn(ctx) if self.eval_vars_fn else ctx.copy()
        
        try:
            result = eval_condition(check_expr, variables)
        except SafeEvalError as e:
            raise PipelineError(f"条件表达式错误: {e}")
        
        target = then_target if result else else_target
        
        if target:
            for i, step in enumerate(steps):
                if step.get("step_id") == target:
                    self.logger.info(f"条件 {result}，跳转到: {target}")
                    return i
            raise PipelineError(f"condition 目标步骤不存在: {target}")
        
        return index + 1
    
    def _op_print(self, ctx, params, steps, index):
        """打印当前结果"""
        return index + 1
    
    def _op_load_step_result(self, ctx, params, steps, index):
        """加载历史步骤结果"""
        step_id = params.get("step_id") or params.get("uid")
        if not step_id:
            raise PipelineError("load_step_result 需要 'step_id' 或 'uid' 参数")
        
        result = ctx.get("results", {}).get(step_id)
        if result is None:
            self.logger.warning(f"步骤 '{step_id}' 的结果不存在")
        
        ctx["last_result"] = result
        return index + 1
    
    def _op_clear_context(self, ctx, params, steps, index):
        """清空上下文和内存"""
        import gc
        
        ctx["last_result"] = None
        
        if params.get("clear_results", False):
            keep_steps = set(params.get("keep_steps", []))
            results = ctx.get("results", {})
            removed = [k for k in list(results.keys()) if k not in keep_steps]
            for step_id in removed:
                del results[step_id]
            if removed:
                self.logger.info(f"已清空 {len(removed)} 个历史步骤结果")
        
        if "_data_context" in ctx:
            del ctx["_data_context"]
        
        if params.get("gc", True):
            gc.collect()
        
        self.logger.info("上下文已清空")
        return index + 1
    
    def execute(self, config_path: str) -> Dict[str, Any]:
        """执行 pipeline 配置文件"""
        self._name = os.path.splitext(os.path.basename(config_path))[0]
        self._t0 = time.time()
        self.logger.set_pipeline(self._name)
        
        try:
            steps = self.parse_pipeline(os.path.abspath(config_path))
            
            ctx = self.init_ctx() if self.init_ctx else {}
            ctx.setdefault("base_dir", os.path.dirname(config_path))
            ctx.setdefault("last_result", None)
            ctx.setdefault("results", {})
            
            result = self.run(steps, ctx)
            
            elapsed = time.time() - self._t0
            self.logger.info(f"总耗时: {elapsed:.3f}s")
            
            if self.done_fn:
                self.done_fn(result, self.logger)
            
            return result
            
        except UserCancelledError:
            elapsed = time.time() - self._t0
            self.logger.info(f"Pipeline 被用户取消，耗时: {elapsed:.3f}s")
            raise
        except PipelineError as e:
            elapsed = time.time() - self._t0
            self.logger.error(f"Pipeline 执行失败！耗时: {elapsed:.3f}s", extra={"error": str(e), "step_id": e.step_id})
            raise
        except Exception as e:
            elapsed = time.time() - self._t0
            self.logger.error(f"Pipeline 执行发生未预期错误！耗时: {elapsed:.3f}s", extra={"error": str(e)})
            raise PipelineError(f"未预期的错误: {e}", original_error=e)
    
    @classmethod
    def main(cls, ops: Dict[str, Callable], cfg: Optional[str] = None, **kwargs):
        """主入口"""
        import sys
        
        config_path = cfg or (sys.argv[1] if len(sys.argv) > 1 else "config.json")
        
        init_ctx = kwargs.get("init_ctx", lambda: {})
        kwargs["init_ctx"] = lambda: {"last_result": None, "results": {}, **init_ctx()}
        
        def default_done(ctx, logger):
            logger.success("Pipeline 执行流程结束!")
        
        kwargs.setdefault("done_fn", default_done)
        
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
