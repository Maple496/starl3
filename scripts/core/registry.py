"""
StarL3 操作注册表模块
替代运行时扫描目录，提供显式注册机制
"""

import functools
from typing import Callable, Dict, Any, Optional
from dataclasses import dataclass, field


@dataclass
class OpMetadata:
    """操作元数据"""
    name: str
    category: str  # elt, file, ai, crawler, email, config, attemper
    description: str = ""
    params_schema: Dict[str, Any] = field(default_factory=dict)
    returns: str = ""


class OpRegistry:
    """操作注册表
    
    使用方式:
        @OpRegistry.register("read_csv", category="elt")
        def op_read_csv(ctx, params):
            ...
    
    替代原有的运行时目录扫描:
        # 旧方式
        for m in os.listdir("ops"):
            if f.startswith(m.split('_')[0]):
                importlib.import_module(f"ops.{m}")
    """
    
    _ops: Dict[str, Callable] = {}
    _metadata: Dict[str, OpMetadata] = {}
    _categories: Dict[str, set] = {}
    
    @classmethod
    def register(
        cls,
        name: str,
        category: str = "",
        description: str = "",
        params_schema: Optional[Dict] = None
    ) -> Callable:
        """注册操作
        
        Args:
            name: 操作名称
            category: 操作类别
            description: 操作描述
            params_schema: 参数 JSON Schema
        """
        def decorator(func: Callable) -> Callable:
            # 注册操作
            cls._ops[name] = func
            
            # 注册元数据
            cls._metadata[name] = OpMetadata(
                name=name,
                category=category,
                description=description or func.__doc__ or "",
                params_schema=params_schema or {}
            )
            
            # 分类索引
            if category:
                if category not in cls._categories:
                    cls._categories[category] = set()
                cls._categories[category].add(name)
            
            # 保留原函数
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
            return {k: v for k, v in cls._ops.items() 
                   if k in cls._categories.get(category, set())}
        return cls._ops.copy()
    
    @classmethod
    def get_metadata(cls, name: str) -> Optional[OpMetadata]:
        """获取操作元数据"""
        return cls._metadata.get(name)
    
    @classmethod
    def list_categories(cls) -> list:
        """列出所有类别"""
        return list(cls._categories.keys())
    
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
        """获取操作映射字典（兼容旧接口）"""
        return cls._ops.copy()


# ========== 便捷装饰器 ==========

def op(name: str, category: str = "", **kwargs):
    """操作装饰器便捷函数
    
    使用方式:
        @op("read_csv", category="elt", description="读取CSV")
        def op_read_csv(ctx, params):
            ...
    """
    return OpRegistry.register(name, category, **kwargs)


# ========== 自动发现机制（可选） ==========

def auto_discover():
    """自动发现并注册所有操作
    
    在应用启动时调用一次，替代每次运行时扫描
    """
    import importlib
    import pkgutil
    import ops
    
    # 导入所有 ops 子模块
    for importer, modname, ispkg in pkgutil.iter_modules(ops.__path__):
        if modname.endswith('_ops'):
            importlib.import_module(f'ops.{modname}')
    
    return OpRegistry.get_op_map()
