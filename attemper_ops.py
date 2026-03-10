#attemper_ops.py
import subprocess
import sys
import os
import time
import logging
from pipeline_engine import PipelineEngine
import elt_ops
import file_ops

log = logging.getLogger("scheduler")

# ========== 先不放自己，避免循环引用 ==========
OPS_MODULE_MAP = {
    "elt": elt_ops,
    "file": file_ops,
    # "attemper" 延迟注册
}

def run_py(ctx, params):
    file = params["file"]
    import attemper_ops
    OPS_MODULE_MAP["attemper"] = attemper_ops
    basename = os.path.basename(file)
    ops = None
    for key in OPS_MODULE_MAP:
        if basename.startswith(f"{key}_ops"):
            ops = key
            break
    if ops is None:
        raise RuntimeError(f"无法从文件名 '{basename}' 识别ops类型, 文件名需以 {'、'.join(k+'_ops' for k in OPS_MODULE_MAP)} 开头")
    OPS_MODULE_MAP[ops].run(file)

def run_exe(ctx, params):
    """调度EXE程序"""
    exe = params.get("exe") or params.get("script")
    if not exe:
        raise RuntimeError(f"run_exe缺少exe/script参数: {params}")
    args = params.get("args", [])
    cwd = params.get("cwd", None)
    if isinstance(args, str):
        args = [args] if args else []
    cmd = [exe] + [str(a) for a in args]
    print(f"[run_exe] 执行: {' '.join(cmd)}")
    r = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if r.stdout.strip():
        print(r.stdout.strip())
    if r.returncode != 0:
        raise RuntimeError(f"exe失败: {exe}\n{r.stderr}")
    return r.stdout.strip()

def wait(ctx, params):
    """等待指定秒数"""
    seconds = params.get("seconds", 0)
    message = params.get("message", "")
    if message:
        print(f"[wait] {message}")
    print(f"[wait] 等待 {seconds} 秒...")
    time.sleep(float(seconds))
    print(f"[wait] 等待完成")
    return f"waited {seconds}s"

OP_MAP = {
    "run_py": run_py,
    "run_exe": run_exe,
    "wait": wait,
}

def _result_handler(ctx, sid, result, lg):
    if result is not None:
        ctx["results"][sid] = result

def run(config_path):
    """外部调用入口"""
    PipelineEngine(
        OP_MAP,
        log=log,
        default_config="scheduler_config.json",
        init_ctx=lambda: {"results": {}},
        result_handler=_result_handler,
        done_fn=lambda ctx, lg: lg.info(f"调度执行完成, 共 {len(ctx['results'])} 个步骤有结果")
    ).execute(config_path)

if __name__ == '__main__':
    run(None)