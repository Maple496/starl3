"""
StarL3 操作模块包

自动导入所有操作模块并注册到 OpRegistry
"""

# 使用延迟导入避免循环依赖
def _auto_import():
    """自动发现并导入所有操作模块"""
    import importlib
    import pkgutil
    import os
    
    # 当前目录
    current_dir = os.path.dirname(__file__)
    
    # 遍历所有模块
    for importer, modname, ispkg in pkgutil.iter_modules([current_dir]):
        if modname.endswith('_ops') and not ispkg:
            try:
                importlib.import_module(f'.{modname}', __package__)
            except Exception as e:
                print(f"Warning: 无法导入模块 {modname}: {e}")


# 应用启动时自动导入
_auto_import()

# 导出注册表
from core.registry import OpRegistry

__all__ = ['OpRegistry']
