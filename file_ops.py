#file_ops.py

import os, shutil, glob, logging, sys
import pandas as pd
from pipeline_engine import BASE_DIR, PipelineEngine



log = logging.getLogger("fileOps")

def _resolve(path):
    return path if os.path.isabs(path) else os.path.join(BASE_DIR, path)

def _expand(pattern):
    matched = glob.glob(_resolve(pattern), recursive=True)
    return matched

def op_copy(ctx, params):
    sources = params.get("sources", [params["source"]] if "source" in params else [])
    dest, overwrite, dest_is_dir = _resolve(params["dest"]), params.get("overwrite", False), params.get("dest_is_dir", True)
    copied = []
    for pat in sources:
        for src in _expand(pat):
            if dest_is_dir:
                os.makedirs(dest, exist_ok=True)
                target = os.path.join(dest, os.path.basename(src))
            else:
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                target = dest
            if os.path.exists(target) and not overwrite: continue
            if os.path.isdir(src): shutil.copytree(src, target, dirs_exist_ok=True)
            else: shutil.copy2(src, target)
            copied.append({"source": src, "target": target})
    ctx["file_count"] = ctx.get("file_count", 0) + len(copied)
    return pd.DataFrame(copied) if copied else ctx["df"]

def op_move(ctx, params):
    sources = params.get("sources", [params["source"]] if "source" in params else [])
    dest, overwrite, dest_is_dir = _resolve(params["dest"]), params.get("overwrite", False), params.get("dest_is_dir", True)
    moved = []
    for pat in sources:
        for src in _expand(pat):
            if dest_is_dir:
                os.makedirs(dest, exist_ok=True)
                target = os.path.join(dest, os.path.basename(src))
            else:
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                target = dest
            if os.path.exists(target) and not overwrite: continue
            shutil.move(src, target)
            moved.append({"source": src, "target": target})
    ctx["file_count"] = ctx.get("file_count", 0) + len(moved)
    return pd.DataFrame(moved) if moved else ctx["df"]

def op_delete(ctx, params):
    sources = params.get("sources", [params["source"]] if "source" in params else [])
    force, deleted = params.get("force", False), []
    for pat in sources:
        for src in _expand(pat):
            if os.path.isdir(src): shutil.rmtree(src) if force else os.rmdir(src)
            else: os.remove(src)
            deleted.append({"path": src})
    ctx["file_count"] = ctx.get("file_count", 0) + len(deleted)
    return pd.DataFrame(deleted) if deleted else ctx["df"]

def op_mkdir(ctx, params):
    dirs = params.get("dirs", [params["dir"]] if "dir" in params else [])
    created = []
    for d in dirs:
        p = _resolve(d)
        os.makedirs(p, exist_ok=True)
        created.append({"path": p})
    ctx["file_count"] = ctx.get("file_count", 0) + len(created)
    return pd.DataFrame(created) if created else ctx["df"]

def op_rename(ctx, params):
    src, new_name = _resolve(params["source"]), params["new_name"]
    target = os.path.join(os.path.dirname(src), new_name)
    os.rename(src, target)
    ctx["file_count"] = ctx.get("file_count", 0) + 1
    return pd.DataFrame([{"source": src, "target": target}])

def op_list(ctx, params):
    pattern = params.get("pattern", "*")
    recursive = params.get("recursive", False)
    matched = glob.glob(_resolve(pattern), recursive=recursive)
    files = [{"path": f, "name": os.path.basename(f), "size": os.stat(f).st_size, "is_dir": os.path.isdir(f)} for f in matched]
    return pd.DataFrame(files) if files else ctx["df"]

def op_log(ctx, params):
    log.info(params["message"].format(file_count=ctx.get("file_count", 0), row_count=len(ctx["df"]) if ctx["df"] is not None else 0))
    return ctx["df"]

OP_MAP = {"copy": op_copy, "move": op_move, "delete": op_delete, "mkdir": op_mkdir, "rename": op_rename, "list": op_list, "log": op_log}

def _result_handler(ctx, sid, result, lg):
    if result is not None:
        ctx["df"] = result
        ctx["results"][sid] = result

def run(config_path):
    PipelineEngine(
        OP_MAP, log=log, default_config="config.json",
        init_ctx=lambda: {"df": None, "results": {}},
        eval_vars_fn=lambda ctx: {"row_count": len(ctx["df"]) if ctx["df"] is not None else 0},
        result_handler=_result_handler,
        done_fn=lambda ctx, lg: lg.info(f"执行完成 total_rows={len(ctx['df']) if ctx['df'] is not None else 0}")
    ).execute(config_path)
if __name__ == '__main__':
    run(None)