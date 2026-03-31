"""
StarL3 Pipeline 上下文管理模块
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List
from datetime import datetime


@dataclass
class PipelineContext:
    """Pipeline 执行上下文"""
    
    base_dir: str = ""
    last_result: Any = None
    results: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    step_history: List[str] = field(default_factory=list)
    _storage: Dict[str, Any] = field(default_factory=dict, repr=False)
    
    def __post_init__(self):
        if not self.metadata:
            self.metadata = {"created_at": datetime.now().isoformat(), "version": "2.0"}
    
    def set_result(self, step_id: str, result: Any) -> 'PipelineContext':
        """设置步骤结果"""
        self.results[step_id] = result
        self.last_result = result
        self.step_history.append(step_id)
        return self
    
    def get_result(self, step_id: str, default: Any = None) -> Any:
        """获取步骤结果"""
        return self.results.get(step_id, default)
    
    def set(self, key: str, value: Any) -> 'PipelineContext':
        """设置存储值"""
        self._storage[key] = value
        return self
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取存储值"""
        return self._storage.get(key, default)
    
    def __getitem__(self, key: str) -> Any:
        if key in ("base_dir", "last_result", "results"):
            return getattr(self, key)
        return self._storage[key]
    
    def __setitem__(self, key: str, value: Any):
        if key in ("base_dir", "last_result", "results"):
            setattr(self, key, value)
        else:
            self._storage[key] = value
    
    def __contains__(self, key: str) -> bool:
        return key in ("base_dir", "last_result", "results") or key in self._storage
    
    def setdefault(self, key: str, default: Any) -> Any:
        """支持 ctx.setdefault()"""
        if key in ("base_dir", "last_result", "results"):
            current = getattr(self, key)
            if current in ("", None, {}):
                setattr(self, key, default)
                return default
            return current
        return self._storage.setdefault(key, default)
    
    def copy(self) -> 'PipelineContext':
        """深拷贝上下文"""
        import copy
        new_ctx = PipelineContext(
            base_dir=self.base_dir,
            last_result=copy.deepcopy(self.last_result),
            results=copy.deepcopy(self.results),
            metadata=copy.deepcopy(self.metadata),
            step_history=copy.deepcopy(self.step_history)
        )
        new_ctx._storage = copy.deepcopy(self._storage)
        return new_ctx
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "base_dir": self.base_dir,
            "last_result": self.last_result,
            "results": self.results,
            "metadata": self.metadata,
            "step_history": self.step_history,
            "_storage": self._storage
        }


def create_context(base_dir: str = "", **kwargs) -> PipelineContext:
    """创建上下文"""
    return PipelineContext(base_dir=base_dir, **kwargs)
