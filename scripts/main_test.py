#!/usr/bin/env python3
"""
StarL3 Pipeline 测试工具
用于快速测试 elt_purchase.json 配置文件，无需打包

使用方法:
    python main_test.py
    或双击 test.bat
"""

import sys
import os
import time
import traceback

# 确保可以导入本地模块
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

from core.constants import DATA_DIR
from core.registry import auto_discover, OpRegistry
from core.pipeline_engine import PipelineEngine, UserCancelledError
from core.context import create_context


# 默认配置文件路径
DEFAULT_CONFIG = os.path.join(script_dir, 'configs', 'elt_purchase.json')


def print_header(text):
    """打印标题"""
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70 + "\n")


def run_test():
    """运行测试"""
    print_header("StarL3 Pipeline 测试工具")
    
    # 检查配置文件
    config_path = DEFAULT_CONFIG
    if not os.path.exists(config_path):
        print(f"[错误] 配置文件不存在: {config_path}")
        return
    
    print(f"[配置] {config_path}\n")
    
    # 自动加载操作
    print("[初始化] 加载操作模块...")
    ops_map = auto_discover()
    print(f"[初始化] 已加载 {len(ops_map)} 个操作\n")
    
    # 创建引擎
    def init_context():
        return create_context(base_dir=DATA_DIR)
    
    engine = PipelineEngine(
        ops=ops_map,
        init_ctx=init_context
    )
    
    # 执行
    start_time = time.time()
    
    try:
        print_header("开始执行 Pipeline")
        result = engine.execute(config_path)
        
        elapsed = time.time() - start_time
        print_header("执行完成")
        print(f"总耗时: {elapsed:.3f} 秒")
        
        # 显示结果摘要
        if hasattr(result, 'keys') and 'results' in result:
            results = result['results']
            print(f"\n执行结果摘要:")
            print(f"  - 执行步骤数: {len(result.get('step_history', []))}")
            last = results.get('last_result')
            print(f"  - 最终结果类型: {type(last).__name__ if last else 'None'}")
        
    except UserCancelledError as e:
        print(f"\n[用户取消] {e}")
    except Exception as e:
        print_header("执行出错")
        print(f"错误类型: {type(e).__name__}")
        print(f"错误信息: {e}")
        print("\n详细堆栈:")
        traceback.print_exc()


def pause():
    """等待用户按键"""
    print("\n" + "-" * 70)
    try:
        input("按回车键关闭窗口...")
    except KeyboardInterrupt:
        pass
    print()


if __name__ == '__main__':
    try:
        run_test()
    except Exception as e:
        print(f"\n[致命错误] {e}")
        traceback.print_exc()
    finally:
        pause()
