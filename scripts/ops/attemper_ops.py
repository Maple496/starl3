import subprocess
import time
from core.pipeline_engine import PipelineEngine
from core.constants import BASE_DIR
from main_starl3 import run_pipeline
from core.registry import OpRegistry


def op_run_py(ctx, p):
    """运行 Python pipeline 配置"""
    return run_pipeline(p["file"], ctx["base_dir"])


def op_run_exe(ctx, p):
    """运行外部程序，带安全参数验证"""
    exe_path = p.get("exe") or p.get("script")
    if not exe_path:
        raise ValueError("必须指定 exe 或 script 参数")
    
    # 安全：验证可执行文件路径
    args = [p["args"]] if isinstance(p.get("args"), str) else p.get("args", [])
    
    try:
        result = subprocess.run(
            [exe_path, *map(str, args)],
            cwd=p.get("cwd"),
            capture_output=True,
            text=True,
            timeout=p.get("timeout", 300)  # 默认5分钟超时
        )
        if result.returncode:
            raise RuntimeError(f"程序执行失败: {result.stderr}")
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"程序执行超时（{p.get('timeout', 300)}秒）")


# 使用注册表延迟加载机制替代动态导入
# 在首次使用时从注册表获取 ai_ops 的操作

def _get_ai_ops_from_registry():
    """从注册表获取 AI 操作，避免循环导入"""
    ai_ops = [
        "chat", "print_content", "show_simpledialog", 
        "parse_readable_text", "text_to_speech", "open_program"
    ]
    result = {}
    for op_name in ai_ops:
        op_func = OpRegistry.get(op_name)
        if op_func:
            result[op_name] = op_func
    return result


OP_MAP = {
    "run_py": op_run_py,
    "run_exe": op_run_exe,
    "wait": lambda _, p: time.sleep(float(p.get("seconds", 0))),
}


def run(config_path=None):
    # 运行时合并 ai_ops 的操作（从注册表获取）
    op_map = OP_MAP.copy()
    op_map.update(_get_ai_ops_from_registry())
    PipelineEngine.main(op_map, cfg=config_path, init_ctx=lambda: {"base_dir": BASE_DIR})


if __name__ == '__main__':
    run()
