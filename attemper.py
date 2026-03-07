import subprocess
import sys
import os
from file_ops import OP_MAP as FILE_OPS  # 直接导入
from elt_ops import OP_MAP as ELT_OPS  # 直接导入

def run_py(ctx, params):
    script = params["script"]
    args = params.get("args", [])
    cwd = params.get("cwd", None)
    cmd = [sys.executable, script] + [str(a) for a in args]
    r = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"py脚本失败: {script}\n{r.stderr}")
    return r.stdout.strip()

def run_exe(ctx, params):
    """调度EXE程序"""
    exe = params["exe"]
    args = params.get("args", [])
    cwd = params.get("cwd", None)
    cmd = [exe] + [str(a) for a in args]
    r = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"exe失败: {exe}\n{r.stderr}")
    return r.stdout.strip()

OP_MAP = {
    "run_py": run_py,
    "run_exe": run_exe,
}

if __name__ == '__main__':
    from pipeline_engine import PipelineEngine
    import logging
    log = logging.getLogger("scheduler")
    PipelineEngine(OP_MAP, log=log, default_config="scheduler_config.json").main()