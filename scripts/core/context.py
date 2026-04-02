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


"""
StarL3 数据上下文管理模块 V2
"""

import os
from pathlib import Path
import pandas as pd


class DataContext:
    """数据上下文 - 分离内存数据和文件数据"""
    
    def __init__(self, base_dir: str = ""):
        self.base_dir = base_dir
        self._source = ""
        self.files: List[Dict[str, Any]] = []
        self.memory_data: Any = None
        self.memory_type: str = "empty"
        self.metadata: Dict[str, Any] = {}
    
    def add_file(self, path: str, metadata: Dict = None) -> bool:
        """添加单个文件"""
        if not os.path.exists(path):
            return False
        
        abs_path = os.path.abspath(path)
        file_info = {
            'name': os.path.basename(path),
            'path': path,
            'abs_path': abs_path,
            'size': os.path.getsize(abs_path),
            'ext': Path(path).suffix[1:]
        }
        if metadata:
            file_info.update(metadata)
        
        self.files.append(file_info)
        return True
    
    def get_files(self, ext: str = None) -> List[Dict[str, Any]]:
        """获取文件列表，可按扩展名筛选"""
        if not ext:
            return self.files
        return [f for f in self.files if f.get('ext') == ext]
    
    def has_files(self) -> bool:
        """是否有文件数据"""
        return len(self.files) > 0
    
    def set_memory_data(self, data: Any, data_type: str, source: str = ""):
        """设置内存数据"""
        self.memory_data = data
        self.memory_type = data_type
        self._source = source
    
    def get_dataframe(self) -> Optional[pd.DataFrame]:
        """获取DataFrame，如果不是则返回None"""
        if self.memory_type == "dataframe" and isinstance(self.memory_data, pd.DataFrame):
            return self.memory_data
        return None
    
    def to_output(self) -> Dict[str, Any]:
        """转换为输出格式"""
        return {
            '_version': '2.0',
            '_source': self._source,
            'files': self.files,
            'files_count': len(self.files),
            'memory_data': '...' if self.memory_data is not None else None,
            'memory_type': self.memory_type,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_result(cls, result: Any, source: str = "", base_dir: str = "") -> 'DataContext':
        """从结果创建上下文"""
        ctx = cls(base_dir)
        ctx._source = source
        
        if result is None:
            return ctx
        
        # 处理 DataContext 格式
        if isinstance(result, dict) and result.get('_version') == '2.0':
            ctx.files = result.get('files', [])
            ctx.memory_type = result.get('memory_type', 'empty')
            ctx.metadata = result.get('metadata', {})
            return ctx
        
        # 处理 file_ops 格式
        if isinstance(result, dict) and 'items' in result and 'base_path' in result:
            base_path = result.get('base_path', base_dir)
            for name, info in result.get('items', {}).items():
                if info.get('type') == 'file':
                    file_path = os.path.join(base_path, name)
                    ctx.add_file(file_path, {
                        'modify_time': info.get('modify_time'),
                        'ext': info.get('extension', '')
                    })
            return ctx
        
        # 处理 DataFrame
        if isinstance(result, pd.DataFrame):
            ctx.set_memory_data(result, 'dataframe', source)
            return ctx
        
        # 处理列表（可能是文件路径列表）
        if isinstance(result, list) and result and isinstance(result[0], str):
            for path in result:
                ctx.add_file(path)
            return ctx
        
        # 其他数据作为 JSON 处理
        ctx.set_memory_data(result, 'json', source)
        return ctx


def create_dataframe_context(df: pd.DataFrame, base_dir: str = "") -> DataContext:
    """创建DataFrame上下文"""
    ctx = DataContext(base_dir)
    ctx.set_memory_data(df, 'dataframe')
    return ctx
