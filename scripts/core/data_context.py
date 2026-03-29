"""
StarL3 数据上下文管理模块 V2
分离内存数据和文件数据，显式转换
"""

import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import pandas as pd


class DataContext:
    """
    数据上下文 - 分离内存数据和文件数据
    
    设计原则:
    1. 内存数据(memory_data)和文件数据(files)独立存储
    2. 各模块只操作自己擅长的数据类型
    3. 只有显式转换步骤才改变数据类型
    4. 类型不匹配时报错，不自动转换
    """
    
    def __init__(self, base_dir: str = ""):
        self.base_dir = base_dir
        self._source = ""  # 数据来源模块
        
        # 文件数据 - 可被 file_ops, email_ops 操作
        self.files: List[Dict[str, Any]] = []
        
        # 内存数据 - 可被 elt_ops, crawler_ops 操作
        self.memory_data: Any = None
        self.memory_type: str = "empty"  # empty, dataframe, json, text
        
        # 元数据
        self.metadata: Dict[str, Any] = {}
    
    # ========== 文件数据操作 ==========
    
    def set_files(self, files: List[Dict[str, Any]], source: str = ""):
        """设置文件列表"""
        self.files = files
        self._source = source
    
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
    
    def get_file_paths(self, ext: str = None) -> List[str]:
        """获取文件路径列表"""
        files = self.get_files(ext)
        return [f['abs_path'] for f in files]
    
    def has_files(self) -> bool:
        """是否有文件数据"""
        return len(self.files) > 0
    
    # ========== 内存数据操作 ==========
    
    def set_memory_data(self, data: Any, data_type: str, source: str = ""):
        """设置内存数据"""
        self.memory_data = data
        self.memory_type = data_type
        self._source = source
    
    def get_memory_data(self) -> Any:
        """获取内存数据"""
        return self.memory_data
    
    def get_dataframe(self) -> Optional[pd.DataFrame]:
        """获取DataFrame，如果不是则返回None"""
        if self.memory_type == "dataframe" and isinstance(self.memory_data, pd.DataFrame):
            return self.memory_data
        return None
    
    def has_memory_data(self) -> bool:
        """是否有内存数据"""
        return self.memory_data is not None
    
    # ========== 类型检查 ==========
    
    def check_files_required(self, module_name: str) -> bool:
        """检查是否有文件数据，没有则报错"""
        if not self.has_files():
            raise TypeError(
                f"[{module_name}] 需要文件数据，但上下文没有文件。\n"
                f"请先执行文件操作步骤，或添加转换步骤将内存数据保存为文件。"
            )
        return True
    
    def check_memory_data_required(self, module_name: str, expected_type: str = None) -> bool:
        """检查是否有内存数据，没有则报错"""
        if not self.has_memory_data():
            raise TypeError(
                f"[{module_name}] 需要内存数据，但上下文为空。\n"
                f"请先执行数据读取步骤（如 read_csv）。"
            )
        
        if expected_type and self.memory_type != expected_type:
            raise TypeError(
                f"[{module_name}] 需要 {expected_type} 类型数据，"
                f"但当前是 {self.memory_type}。"
            )
        return True
    
    # ========== 转换方法 ==========
    
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
        
        # 处理已经是 DataContext 格式的
        if isinstance(result, dict) and result.get('_version') == '2.0':
            ctx.files = result.get('files', [])
            ctx.memory_type = result.get('memory_type', 'empty')
            ctx.metadata = result.get('metadata', {})
            # memory_data 暂不恢复
            return ctx
        
        # 处理 file_ops 格式
        if isinstance(result, dict) and 'items' in result and 'base_path' in result:
            ctx._convert_file_ops_result(result, base_dir)
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
    
    def _convert_file_ops_result(self, result: Dict, base_dir: str):
        """转换 file_ops 的输出"""
        base_path = result.get('base_path', base_dir)
        items = result.get('items', {})
        
        for name, info in items.items():
            if info.get('type') == 'file':
                file_path = os.path.join(base_path, name)
                self.add_file(file_path, {
                    'modify_time': info.get('modify_time'),
                    'ext': info.get('extension', '')
                })
    
    def __repr__(self):
        return f"DataContext(files={len(self.files)}, memory={self.memory_type})"


# ========== 便捷函数 ==========

def create_file_context(files: List[str], base_dir: str = "") -> DataContext:
    """创建文件上下文"""
    ctx = DataContext(base_dir)
    for f in files:
        ctx.add_file(f)
    return ctx


def create_dataframe_context(df: pd.DataFrame, base_dir: str = "") -> DataContext:
    """创建DataFrame上下文"""
    ctx = DataContext(base_dir)
    ctx.set_memory_data(df, 'dataframe')
    return ctx
