"""
StarL3 统一入口 - 简化版
支持跨模块配置，通过 op_type 自动路由
"""

import sys
import os
import importlib

# 项目根目录
BASE_DIR = os.path.dirname(
    sys.executable if getattr(sys, 'frozen', False) 
    else os.path.abspath(__file__)
)

# 加载所有模块的操作
def load_all_ops():
    """加载所有 ops/*_ops.py 模块的 OP_MAP"""
    ops_dir = os.path.join(BASE_DIR, "ops")
    all_ops = {}
    
    if not os.path.exists(ops_dir):
        return all_ops
    
    for filename in os.listdir(ops_dir):
        if filename.endswith("_ops.py"):
            module_name = filename[:-3]  # 去掉 .py
            try:
                module = importlib.import_module(f"ops.{module_name}")
                if hasattr(module, 'OP_MAP'):
                    all_ops.update(module.OP_MAP)
                    print(f"[INFO] 加载模块: {module_name}, 操作数: {len(module.OP_MAP)}")
            except Exception as e:
                print(f"[WARNING] 加载模块 {module_name} 失败: {e}")
    
    return all_ops


def run_pipeline(config_path, base_dir=BASE_DIR):
    """
    运行 Pipeline
    
    参数:
        config_path: 配置文件路径
        base_dir: 项目根目录
    """
    from core.pipeline_engine import PipelineEngine
    
    # 加载所有操作
    all_ops = load_all_ops()
    
    if not all_ops:
        raise RuntimeError("没有加载到任何操作模块")
    
    print(f"[INFO] 共加载 {len(all_ops)} 个操作")
    
    # 执行 pipeline
    return PipelineEngine.main(
        all_ops, 
        cfg=config_path, 
        init_ctx=lambda: {"base_dir": base_dir}
    )


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_pipeline(sys.argv[1])
    else:
        print("用法: python main.py <config.json>")
        print("示例: python main.py configs/workflow.json")
        sys.exit(1)
