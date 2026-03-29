"""
StarL3 Pipeline 上下文管理模块
使用类封装替代字典，提供类型安全和追踪能力
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List
from datetime import datetime


@dataclass
class PipelineContext:
    """Pipeline 执行上下文
    
    替代原有的 ctx: Dict[str, Any] 字典，提供：
    1. 类型安全
    2. 变更追踪
    3. 隔离性
    4. 序列化支持
    """
    
    # 基础配置
    base_dir: str = ""
    
    # 执行状态
    last_result: Any = None
    results: Dict[str, Any] = field(default_factory=dict)
    
    # 元数据
    metadata: Dict[str, Any] = field(default_factory=dict)
    step_history: List[str] = field(default_factory=list)
    
    # 扩展存储（用于 email_config, session 等）
    _storage: Dict[str, Any] = field(default_factory=dict, repr=False)
    
    def __post_init__(self):
        """初始化后的处理"""
        if not self.metadata:
            self.metadata = {
                "created_at": datetime.now().isoformat(),
                "version": "2.0"
            }
    
    # ========== 结果操作 ==========
    
    def set_result(self, step_id: str, result: Any) -> 'PipelineContext':
        """设置步骤结果，自动更新 last_result"""
        self.results[step_id] = result
        self.last_result = result
        self.step_history.append(step_id)
        return self
    
    def get_result(self, step_id: str, default: Any = None) -> Any:
        """获取步骤结果"""
        return self.results.get(step_id, default)
    
    def has_result(self, step_id: str) -> bool:
        """检查是否有步骤结果"""
        return step_id in self.results
    
    # ========== 存储操作 ==========
    
    def set(self, key: str, value: Any) -> 'PipelineContext':
        """设置存储值（类似 dict）"""
        self._storage[key] = value
        return self
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取存储值"""
        return self._storage.get(key, default)
    
    def has(self, key: str) -> bool:
        """检查是否有存储值"""
        return key in self._storage
    
    def delete(self, key: str) -> bool:
        """删除存储值"""
        if key in self._storage:
            del self._storage[key]
            return True
        return False
    
    # ========== 便捷属性 ==========
    
    @property
    def email_config(self) -> Optional[Dict]:
        """获取邮件配置"""
        return self._storage.get("_email_config")
    
    @email_config.setter
    def email_config(self, value: Dict):
        """设置邮件配置"""
        self._storage["_email_config"] = value
    
    @property
    def email_content(self) -> Optional[Dict]:
        """获取邮件内容"""
        return self._storage.get("_email_content")
    
    @email_content.setter
    def email_content(self, value: Dict):
        """设置邮件内容"""
        self._storage["_email_content"] = value
    
    @property
    def session(self) -> Any:
        """获取 HTTP Session"""
        from requests import Session
        if "_session" not in self._storage:
            self._storage["_session"] = Session()
        return self._storage["_session"]
    
    def clear_session(self):
        """清除 Session"""
        from requests import Session
        if "_session" in self._storage:
            self._storage["_session"] = Session()
    
    @property
    def data_context(self) -> Any:
        """获取数据上下文"""
        return self._storage.get("_data_context")
    
    @data_context.setter
    def data_context(self, value: Any):
        """设置数据上下文"""
        self._storage["_data_context"] = value
    
    # ========== 兼容性支持 ==========
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（用于序列化）"""
        return {
            "base_dir": self.base_dir,
            "last_result": self.last_result,
            "results": self.results,
            "metadata": self.metadata,
            "step_history": self.step_history,
            "_storage": self._storage
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PipelineContext':
        """从字典创建"""
        ctx = cls(
            base_dir=data.get("base_dir", ""),
            last_result=data.get("last_result"),
            results=data.get("results", {}),
            metadata=data.get("metadata", {}),
            step_history=data.get("step_history", [])
        )
        ctx._storage = data.get("_storage", {})
        return ctx
    
    # 兼容 dict-like 访问
    def __getitem__(self, key: str) -> Any:
        """支持 ctx[key] 访问"""
        if key in ["base_dir", "last_result", "results"]:
            return getattr(self, key)
        return self._storage[key]
    
    def __setitem__(self, key: str, value: Any):
        """支持 ctx[key] = value 设置"""
        if key in ["base_dir", "last_result", "results"]:
            setattr(self, key, value)
        else:
            self._storage[key] = value
    
    def __contains__(self, key: str) -> bool:
        """支持 key in ctx 检查"""
        if key in ["base_dir", "last_result", "results"]:
            return True
        return key in self._storage
    
    def get_executed_summary(self) -> str:
        """获取执行摘要"""
        return f"执行了 {len(self.step_history)} 步: {', '.join(self.step_history[-5:])}"


# ========== 便捷函数 ==========

def create_context(base_dir: str = "", **kwargs) -> PipelineContext:
    """创建上下文"""
    return PipelineContext(base_dir=base_dir, **kwargs)
