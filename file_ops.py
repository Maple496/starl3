
import os, shutil
import pandas as pd
from pipeline_engine import BASE_DIR, PipelineEngine

def _resolve(path):
    return path if os.path.isabs(path) else os.path.join(BASE_DIR, path)

def op_copy(ctx, params):
    src, dest = _resolve(params["file"]), _resolve(params["dest"])
    os.makedirs(dest, exist_ok=True)
    target = os.path.join(dest, os.path.basename(src))
    shutil.copy2(src, target) if not os.path.isdir(src) else shutil.copytree(src, target, dirs_exist_ok=True)
    return pd.DataFrame([{"source": src, "target": target}])

def op_move(ctx, params):
    src, dest = _resolve(params["file"]), _resolve(params["dest"])
    os.makedirs(dest, exist_ok=True)
    target = os.path.join(dest, os.path.basename(src))
    shutil.move(src, target)
    return pd.DataFrame([{"source": src, "target": target}])

def op_rename(ctx, params):
    src = _resolve(params["file"])
    target = os.path.join(os.path.dirname(src), params["new_name"])
    os.rename(src, target)
    return pd.DataFrame([{"source": src, "target": target}])

OP_MAP = {"copy": op_copy, "move": op_move, "rename": op_rename}

def _result_handler(ctx, sid, result, lg):
    if result is not None:
        ctx["df"] = result
        ctx["results"][sid] = result
def run(config_path):
    PipelineEngine(
        OP_MAP, 
        init_ctx=lambda: {"df": None, "results": {}, "base_dir": BASE_DIR},
        eval_vars_fn=lambda ctx: {"row_count": len(ctx["df"]) if ctx["df"] is not None else 0},
        result_handler=_result_handler,
        done_fn=lambda ctx, lg: lg.info(f"执行完成 total_rows={len(ctx['df']) if ctx['df'] is not None else 0}")
    ).execute(config_path)
if __name__ == '__main__':
    run(None)