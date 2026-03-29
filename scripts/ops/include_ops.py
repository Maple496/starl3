"""
Include 操作模块 - 支持配置文件嵌套引用
"""

import json
import os
from core.pipeline_engine import PipelineEngine
from core.registry import op
from core.logger import get_logger

logger = get_logger("include_ops")


@op("include", category="flow", description="引用子配置文件")
def op_include(ctx, params):
    """
    引用并执行子配置文件，将子流程的结果合并到当前流程
    
    参数:
        file: str - 子配置文件路径
        return_as: str - 可选，将子流程结果存入指定key，不填则作为last_result
        inherit_context: bool - 是否继承当前上下文，默认True
    """
    file_path = os.path.join(ctx["base_dir"], params["file"])
    return_as = params.get("return_as")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"子配置文件不存在: {file_path}")
    
    logger.info(f"加载子配置: {params['file']}")
    
    # 读取子配置
    with open(file_path, 'r', encoding='utf-8') as f:
        sub_config = json.load(f)
    
    # 执行子流程
    # 复制当前结果作为子流程的输入
    sub_ctx = {
        "base_dir": ctx["base_dir"],
        "last_result": ctx.get("last_result"),
        "results": ctx.get("results", {}).copy(),
        "_is_sub_flow": True,
        "_parent_step_id": params.get("_step_id", "unknown")
    }
    
    # 获取引擎实例并执行
    from core.registry import OpRegistry
    op_map = OpRegistry.get_op_map()
    
    engine = PipelineEngine(op_map, init_ctx=lambda: sub_ctx)
    result = engine.execute(file_path)
    
    # 将子流程结果合并回主流程
    if return_as:
        ctx.setdefault("results", {})[return_as] = result.get("last_result")
        logger.info(f"子流程结果已保存至: {return_as}")
    
    return result.get("last_result")


@op("block", category="flow", description="代码块分组标记")
def op_block(ctx, params):
    """
    代码块分组标记 - 仅用于可视化分组，无实际功能
    
    参数:
        name: str - 块名称
        color: str - 可选，颜色标记
    """
    name = params.get("name", "未命名块")
    logger.info(f"进入代码块: {name}")
    return ctx.get("last_result")


def run(config_path=None):
    """模块测试入口"""
    from core.constants import BASE_DIR
    from core.registry import OpRegistry
    PipelineEngine.main(
        OpRegistry.get_op_map(), 
        cfg=config_path, 
        init_ctx=lambda: {"base_dir": BASE_DIR}
    )


if __name__ == '__main__':
    run()
