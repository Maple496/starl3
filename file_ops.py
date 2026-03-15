import os, shutil
from pipeline_engine import PipelineEngine

def op_copy(ctx, params):
    src, dest = params["file"], params["dest"]
    os.makedirs(dest, exist_ok=True)
    if os.path.isdir(src):
        shutil.copytree(src, os.path.join(dest, os.path.basename(src)), dirs_exist_ok=True)
    else:
        shutil.copy2(src, os.path.join(dest, os.path.basename(src)))
def op_move(ctx, params):
    src, dest = params["file"], params["dest"]
    os.makedirs(dest, exist_ok=True)
    shutil.move(src, os.path.join(dest, os.path.basename(src)))
def op_rename(ctx, params):
    src = params["file"]
    os.rename(src, os.path.join(os.path.dirname(src), params["new_name"]))

OP_MAP = {"copy": op_copy, "move": op_move, "rename": op_rename}

def run(config_path=None):
    PipelineEngine(
        OP_MAP,
        default_config="scheduler_config.json",
        init_ctx=lambda: {},
        done_fn=lambda ctx, lg: lg.info("执行完成")
    ).execute(config_path)

if __name__ == '__main__':
    run()