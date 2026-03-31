"""
StarL3 操作注册表模块
"""

import functools
from typing import Callable, Dict, Any, Optional
from dataclasses import dataclass, field


@dataclass
class OpMetadata:
    """操作元数据"""
    name: str
    category: str
    description: str = ""
    params_schema: Dict[str, Any] = field(default_factory=dict)


class OpRegistry:
    """操作注册表"""
    
    _ops: Dict[str, Callable] = {}
    _metadata: Dict[str, OpMetadata] = {}
    _categories: Dict[str, set] = {}
    
    @classmethod
    def register(cls, name: str, category: str = "", description: str = "", 
                 params_schema: Optional[Dict] = None) -> Callable:
        """注册操作"""
        def decorator(func: Callable) -> Callable:
            cls._ops[name] = func
            cls._metadata[name] = OpMetadata(
                name=name, category=category, description=description or func.__doc__ or "",
                params_schema=params_schema or {}
            )
            if category:
                cls._categories.setdefault(category, set()).add(name)
            
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            
            wrapper._op_name = name
            wrapper._op_category = category
            return wrapper
        
        return decorator
    
    @classmethod
    def get(cls, name: str) -> Optional[Callable]:
        """获取操作"""
        return cls._ops.get(name)
    
    @classmethod
    def has(cls, name: str) -> bool:
        """检查操作是否存在"""
        return name in cls._ops
    
    @classmethod
    def list_ops(cls, category: Optional[str] = None) -> Dict[str, Callable]:
        """列出操作"""
        if category:
            return {k: v for k, v in cls._ops.items() if k in cls._categories.get(category, set())}
        return cls._ops.copy()
    
    @classmethod
    def get_metadata(cls, name: str) -> Optional[OpMetadata]:
        """获取操作元数据"""
        return cls._metadata.get(name)
    
    @classmethod
    def unregister(cls, name: str) -> bool:
        """注销操作"""
        if name not in cls._ops:
            return False
        meta = cls._metadata.get(name)
        if meta and meta.category:
            cls._categories.get(meta.category, set()).discard(name)
        del cls._ops[name]
        del cls._metadata[name]
        return True
    
    @classmethod
    def clear(cls):
        """清空所有注册"""
        cls._ops.clear()
        cls._metadata.clear()
        cls._categories.clear()
    
    @classmethod
    def get_op_map(cls) -> Dict[str, Callable]:
        """获取操作映射字典"""
        return cls._ops.copy()


def op(name: str, category: str = "", **kwargs):
    """操作装饰器便捷函数"""
    return OpRegistry.register(name, category, **kwargs)


def auto_discover():
    """自动发现并注册所有操作"""
    import importlib
    import pkgutil
    import ops
    
    for importer, modname, ispkg in pkgutil.iter_modules(ops.__path__):
        if modname.endswith('_ops'):
            importlib.import_module(f'ops.{modname}')
    
    return OpRegistry.get_op_map()
