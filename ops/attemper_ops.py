import subprocess
import os
import time
import logging
import json
from core.pipeline_engine import PipelineEngine
from . import elt_ops
from . import file_ops
from . import ai_ops

log = logging.getLogger("scheduler")

OPS_MODULE_MAP = {
    "elt": elt_ops,
    "file_ops": file_ops,
    "ai_ops": ai_ops,
}

# ==========================================
# 核心数据流控制函数
# ==========================================

def load_step_result(ctx, params):
    """
    【新增】专门负责将历史步骤的结果提取到当前 last_result 中。
    后续节点只需读取 last_result 即可实现数据闭环。
    """
    step_id = params.get("step_id")
    # 从 ctx["results"] 获取指定步骤的输出
    res = ctx.get("results", {}).get(step_id)
    if res is not None:
        ctx["last_result"] = res
    return res

# ==========================================
# 执行节点函数
# ==========================================

def run_py(ctx, params):
    from . import attemper_ops
    OPS_MODULE_MAP["attemper"] = attemper_ops
    
    file = params["file"]
    basename = os.path.basename(file)
    
    # 根据文件名前缀匹配对应的 Ops 处理器
    ops_key = next((k for k in OPS_MODULE_MAP if basename.startswith(f"{k}_ops")), None)
    if ops_key is None:
        raise RuntimeError(f"无法识别 ops 类型: {basename}")
    
    # 执行具体的脚本逻辑
    result = OPS_MODULE_MAP[ops_key].run(file)
    ctx["last_result"] = result
    return result
    
def run_exe(ctx, params):
    # 兼容参数名 exe 或 script
    exe = params.get("exe") or params.get("script")
    args = params.get("args", [])
    
    if isinstance(args, str):
        args = [args] if args else []
        
    cmd = [exe] + [str(a) for a in args]
    r = subprocess.run(cmd, cwd=params.get("cwd"), capture_output=True, text=True)
    
    if r.returncode != 0:
        raise RuntimeError(f"exe 失败: {exe}\n{r.stderr}")
    
    output = r.stdout.strip()
    ctx["last_result"] = output
    return output

def wait(ctx, params):
    seconds = float(params.get("seconds", 0))
    time.sleep(seconds)
    return seconds

# ==========================================
# 注册与运行
# ==========================================

OP_MAP = {
    "load_step_result": load_step_result,
    "run_py": run_py,
    "run_exe": run_exe,
    "wait": wait,
}

# 自动合并 ai_ops 中定义的其他节点
OP_MAP.update(ai_ops.OP_MAP)

def run(config_path=None):
    PipelineEngine(
        OP_MAP,
        init_ctx=lambda: {"results": {}, "last_result": ""},
        # result_handler 会自动将 return 的结果存入 ctx["results"][step_id]
        result_handler=lambda ctx, sid, res, lg: ctx["results"].__setitem__(sid, res) if res is not None else None,
        done_fn=lambda ctx, lg: lg.info(f"执行完成, 共 {len(ctx['results'])} 个步骤有结果")
    ).execute(config_path)

if __name__ == '__main__':
    run()