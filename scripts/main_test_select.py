#!/usr/bin/env python3
"""
StarL3 Pipeline 资源选择测试工具
先弹出资源选择窗口，然后运行 pipeline，无需打包

使用方法:
    python main_test_select.py
    或双击 test_select.bat
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

# 导入 tkinter 用于资源选择
from tkinter import Tk, filedialog


# 默认配置文件路径
DEFAULT_CONFIG = os.path.join(script_dir, 'configs', 'elt_purchase.json')


def print_header(text):
    """打印标题"""
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70 + "\n")


def select_resource():
    """
    弹出资源选择窗口，让用户选择要处理的资源
    
    返回:
        dict: 包含选择的资源信息
    """
    print_header("资源选择")
    print("请选择要处理的数据源（文件夹或文件）\n")
    
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    selected_path = None
    selection_type = None
    
    try:
        # 第一步：选择类型
        print("请选择数据源类型：")
        print("  1. 文件夹（包含多个数据文件）")
        print("  2. 单个文件")
        
        choice = input("\n请输入选项 (1/2) [默认: 1]: ").strip()
        
        if choice == "2":
            # 选择文件
            selection_type = "file"
            file_path = filedialog.askopenfilename(
                title="请选择数据文件",
                initialdir=DATA_DIR,
                filetypes=[
                    ("Excel 文件", "*.xlsx;*.xls"),
                    ("CSV 文件", "*.csv"),
                    ("所有文件", "*.*")
                ]
            )
            if file_path:
                selected_path = file_path.replace('\\', '/')
        else:
            # 选择文件夹
            selection_type = "folder"
            folder_path = filedialog.askdirectory(
                title="请选择数据源文件夹",
                initialdir=DATA_DIR
            )
            if folder_path:
                selected_path = folder_path.replace('\\', '/')
    finally:
        root.destroy()
    
    if not selected_path:
        print("\n[警告] 未选择任何资源，将使用配置文件中定义的默认路径")
        return None
    
    print(f"\n[已选择] {selected_path}")
    
    # 选择输出目录
    print("\n请选择输出目录（用于保存处理结果）：")
    
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    output_dir = None
    try:
        output_path = filedialog.askdirectory(
            title="请选择输出目录",
            initialdir=DATA_DIR
        )
        if output_path:
            output_dir = output_path.replace('\\', '/')
    finally:
        root.destroy()
    
    if not output_dir:
        # 使用默认输出目录
        output_dir = os.path.join(DATA_DIR, 'output').replace('\\', '/')
        print(f"[默认输出] {output_dir}")
    else:
        print(f"[已选择输出] {output_dir}")
    
    return {
        "source_path": selected_path,
        "source_type": selection_type,
        "output_dir": output_dir
    }


def run_test_with_selection():
    """运行测试（带资源选择）"""
    print_header("StarL3 Pipeline 资源选择测试工具")
    
    # 检查配置文件
    config_path = DEFAULT_CONFIG
    if not os.path.exists(config_path):
        print(f"[错误] 配置文件不存在: {config_path}")
        return
    
    print(f"[配置] {config_path}\n")
    
    # 弹出资源选择窗口
    selection = select_resource()
    
    # 自动加载操作
    print("\n[初始化] 加载操作模块...")
    ops_map = auto_discover()
    print(f"[初始化] 已加载 {len(ops_map)} 个操作\n")
    
    # 创建引擎
    def init_context():
        ctx = create_context(base_dir=DATA_DIR)
        # 如果用户选择了资源，设置到上下文中
        if selection:
            if selection["source_type"] == "folder":
                ctx["paths.source_dir"] = selection["source_path"]
            else:
                ctx["paths.source_file"] = selection["source_path"]
            ctx["paths.output_dir"] = selection["output_dir"]
            print(f"[上下文] 已设置源路径: {selection['source_path']}")
            print(f"[上下文] 已设置输出目录: {selection['output_dir']}")
        return ctx
    
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
        run_test_with_selection()
    except Exception as e:
        print(f"\n[致命错误] {e}")
        traceback.print_exc()
    finally:
        pause()
