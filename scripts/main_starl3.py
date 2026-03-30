import sys
import os
import json

# 从 core.constants 导入 DATA_DIR（用户数据目录，可迁移）
from core.constants import DATA_DIR
from core.registry import auto_discover, OpRegistry
from core.pipeline_engine import UserCancelledError


def select_json_file():
    """弹出文件选择对话框选择 JSON 文件"""
    import tkinter as tk
    from tkinter import filedialog
    
    # 创建主窗口
    root = tk.Tk()
    root.withdraw()
    # 确保窗口在最前
    root.attributes('-topmost', True)
    
    # 弹出文件选择对话框
    file_path = filedialog.askopenfilename(
        title="选择 Pipeline 配置文件",
        filetypes=[("JSON 文件", "*.json"), ("所有文件", "*.*")]
    )
    
    root.destroy()
    return file_path


def show_error(msg):
    """显示错误信息（支持无控制台模式）"""
    import tkinter as tk
    from tkinter import messagebox
    root = tk.Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    messagebox.showerror("错误", msg)
    root.destroy()


def show_info(msg):
    """显示信息（支持无控制台模式）"""
    import tkinter as tk
    from tkinter import messagebox
    root = tk.Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    messagebox.showinfo("提示", msg)
    root.destroy()


def run_pipeline(path: str, base_dir: str = DATA_DIR, trigger_ctx: dict = None):
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


def list_configs():
    """列出所有配置"""
    from core.dynamic_config import list_configs_table, get_config_manager
    
    print("=" * 80)
    print("动态配置列表")
    print("=" * 80)
    print()
    print(list_configs_table())
    print()
    print("提示: 使用 --open-config-index 在 Excel 中打开 CSV 索引文件")


def show_config(config_name: str):
    """显示特定配置的详情"""
    from core.dynamic_config import get_config_manager
    
    manager = get_config_manager()
    config = manager.get_config(config_name)
    
    if not config:
        print(f"[ERROR] 配置不存在: {config_name}")
        sys.exit(1)
    
    print("=" * 80)
    print(f"配置详情: {config_name}")
    print("=" * 80)
    print()
    print(f"配置文件: {manager._get_config_path(config_name)}")
    print(f"创建时间: {config.get('created_at', 'N/A')}")
    print(f"更新时间: {config.get('updated_at', 'N/A')}")
    print(f"备注: {config.get('note', 'N/A')}")
    print()
    print("配置值:")
    print("-" * 40)
    value = config.get('config_value')
    if isinstance(value, (dict, list)):
        print(json.dumps(value, ensure_ascii=False, indent=2))
    else:
        print(value)


def delete_config(config_name: str):
    """删除特定配置"""
    from core.dynamic_config import get_config_manager
    
    manager = get_config_manager()
    
    if not manager.get_config(config_name):
        print(f"[ERROR] 配置不存在: {config_name}")
        sys.exit(1)
    
    confirm = input(f"确定要删除配置 '{config_name}' 吗? (y/N): ")
    if confirm.lower() != 'y':
        print("已取消")
        return
    
    if manager.delete_config(config_name):
        print(f"[OK] 配置已删除: {config_name}")
    else:
        print(f"[ERROR] 删除失败: {config_name}")
        sys.exit(1)


def clear_all_configs():
    """清除所有配置"""
    from core.dynamic_config import get_config_manager
    
    manager = get_config_manager()
    configs = manager.list_configs()
    
    if not configs:
        print("当前没有配置")
        return
    
    print(f"警告: 这将删除所有 {len(configs)} 条配置记录！")
    confirm = input("确定要继续吗? (输入 'yes' 确认): ")
    
    if confirm != 'yes':
        print("已取消")
        return
    
    manager.clear_all_configs()
    print("[OK] 所有配置已清除")


def open_config_index():
    """打开 CSV 配置文件"""
    from core.dynamic_config import get_config_manager
    
    manager = get_config_manager()
    manager.open_csv()
    print(f"正在打开 CSV: {manager.csv_path}")


def print_help():
    """打印帮助信息"""
    help_text = """
StarL3 ELT Pipeline - 使用方法

基本用法:
    python main_starl3.py <config.json>          运行 Pipeline 配置文件

配置管理命令:
    python main_starl3.py --list-configs         列出所有动态配置
    python main_starl3.py --show-config <name>   查看特定配置详情
    python main_starl3.py --delete-config <name> 删除特定配置
    python main_starl3.py --clear-configs        清除所有配置（需确认）
    python main_starl3.py --open-config-index    用 Excel 打开配置索引

帮助:
    python main_starl3.py --help                 显示此帮助信息

示例:
    python main_starl3.py my_pipeline.json
    python main_starl3.py --list-configs
    python main_starl3.py --show-config elt_purchase.source_dir
    python main_starl3.py --open-config-index
"""
    print(help_text)


def main():
    """主入口"""
    # 检测是否在控制台运行
    has_console = sys.stdout is not None and hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()
    
    if len(sys.argv) <= 1:
        # 没有命令行参数时，弹出文件选择对话框
        file_path = select_json_file()
        if not file_path:
            # 用户取消选择，静默退出
            sys.exit(0)
        # 使用选择的文件路径作为参数
        sys.argv.append(file_path)
    
    arg = sys.argv[1]
    
    # 处理命令行参数
    if arg in ('--help', '-h'):
        print_help()
        sys.exit(0)
    
    elif arg == '--list-configs':
        list_configs()
        sys.exit(0)
    
    elif arg == '--open-config-index':
        open_config_index()
        sys.exit(0)
    
    elif arg == '--show-config':
        if len(sys.argv) < 3:
            msg = "[ERROR] 请指定配置名称\n用法: python main_starl3.py --show-config <config_name>"
            if has_console:
                print(msg)
            else:
                show_error("请指定配置名称")
            sys.exit(1)
        show_config(sys.argv[2])
        sys.exit(0)
    
    elif arg == '--delete-config':
        if len(sys.argv) < 3:
            msg = "[ERROR] 请指定配置名称\n用法: python main_starl3.py --delete-config <config_name>"
            if has_console:
                print(msg)
            else:
                show_error("请指定配置名称")
            sys.exit(1)
        delete_config(sys.argv[2])
        sys.exit(0)
    
    elif arg == '--clear-configs':
        clear_all_configs()
        sys.exit(0)
    
    # 否则当作 pipeline 配置文件运行
    config_path = arg
    
    try:
        result = run_pipeline(config_path)
        if not has_console:
            show_info("处理完成！")
        return result
    except UserCancelledError as e:
        if has_console:
            print(f"\n[INFO] {e}")
        sys.exit(0)
    except Exception as e:
        msg = f"Pipeline 执行失败: {e}"
        if has_console:
            print(f"\n[ERROR] {msg}")
        else:
            show_error(msg)
        sys.exit(1)


if __name__ == "__main__":
    main()
