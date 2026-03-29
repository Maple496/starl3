import sys
import os

# 从 core.constants 导入 BASE_DIR
from core.constants import BASE_DIR
from core.registry import auto_discover, OpRegistry
from core.pipeline_engine import UserCancelledError


def run_pipeline(path: str, base_dir: str = BASE_DIR, trigger_ctx: dict = None):
    """运行 pipeline 配置文件
    
    Args:
        path: 配置文件路径
        base_dir: 基础目录
        trigger_ctx: 触发上下文（可选），如 {"trigger_file": "...", "trigger_time": "..."}
        
    Returns:
        pipeline 执行结果
    """
    from core.pipeline_engine import PipelineEngine
    from core.context import create_context
    
    # 确保所有操作已注册
    ops_map = OpRegistry.get_op_map()
    if not ops_map:
        ops_map = auto_discover()
    
    # 创建上下文（合并触发上下文）
    def init_context():
        ctx = create_context(base_dir=base_dir).to_dict()
        if trigger_ctx:
            ctx.update(trigger_ctx)
        return ctx
    
    # 创建 Pipeline 引擎
    engine = PipelineEngine(
        ops=ops_map,
        init_ctx=init_context
    )
    
    # 执行
    full_path = os.path.join(base_dir, path)
    return engine.execute(full_path)


def main():
    """主入口"""
    if len(sys.argv) <= 1:
        print("用法: python main_starl3.py <config.json>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    try:
        result = run_pipeline(config_path)
        return result
    except UserCancelledError as e:
        print(f"\n[INFO] {e}")
        sys.exit(0)
    except Exception as e:
        print(f"\n[ERROR] Pipeline 执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
