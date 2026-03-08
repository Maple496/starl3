import subprocess
import sys
import os
import time
from file_ops import OP_MAP as FILE_OPS
from elt_ops import OP_MAP as ELT_OPS

def run_py(ctx, params):
    """调度Python脚本"""
    script = params.get("script")
    args = params.get("args", [])
    cwd = params.get("cwd", None)
    # args 兼容字符串和列表
    if isinstance(args, str):
        args = [args] if args else []
    cmd = [sys.executable, script] + [str(a) for a in args]
    print(f"[run_py] 执行: {' '.join(cmd)}")
    r = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if r.stdout.strip():
        print(r.stdout.strip())
    if r.returncode != 0:
        raise RuntimeError(f"py脚本失败: {script}\n{r.stderr}")
    return r.stdout.strip()

def run_exe(ctx, params):
    """调度EXE程序"""
    # 兼容 exe 和 script 两种字段名
    exe = params.get("exe") or params.get("script")
    if not exe:
        raise RuntimeError(f"run_exe缺少exe/script参数: {params}")
    args = params.get("args", [])
    cwd = params.get("cwd", None)
    # args 兼容字符串和列表
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

if __name__ == '__main__':
    from pipeline_engine import PipelineEngine
    import logging
    log = logging.getLogger("scheduler")
    PipelineEngine(OP_MAP, log=log, default_config="scheduler_config.json").main()