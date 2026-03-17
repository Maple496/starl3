import subprocess
import os
import time
import logging
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
def run_py(ctx, params):
    from . import attemper_ops
    OPS_MODULE_MAP["attemper"] = attemper_ops
    file = params["file"]
    basename = os.path.basename(file)
    ops = next((k for k in OPS_MODULE_MAP if basename.startswith(f"{k}_ops")), None)
    if ops is None:
        raise RuntimeError(f"无法识别ops类型: {basename}")
    OPS_MODULE_MAP[ops].run(file)
def run_exe(ctx, params):
    exe = params.get("exe") or params.get("script")
    args = params.get("args", [])
    if isinstance(args, str):
        args = [args] if args else []
    r = subprocess.run([exe] + [str(a) for a in args], cwd=params.get("cwd"), capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"exe失败: {exe}\n{r.stderr}")
    return r.stdout.strip()
def wait(ctx, params):
    time.sleep(float(params.get("seconds", 0)))
OP_MAP = {
    "run_py": run_py,
    "run_exe": run_exe,
    "wait": wait,
}
OP_MAP.update(ai_ops.OP_MAP)
def run(config_path=None):
    PipelineEngine(
        OP_MAP,
        init_ctx=lambda: {"results": {}},
        result_handler=lambda ctx, sid, res, lg: ctx["results"].__setitem__(sid, res) if res else None,
        done_fn=lambda ctx, lg: lg.info(f"调度执行完成, 共 {len(ctx['results'])} 个步骤有结果")
    ).execute(config_path)
if __name__ == '__main__':
    run()