"""
动态配置管理模块
支持缓存用户选择和输入，避免重复弹窗
"""
import os
import json
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, Union
from core.constants import BASE_DIR


class DynamicConfigManager:
    """动态配置管理器"""
    
    def __init__(self, config_dir: str = "dynamic_configs"):
        """
        初始化配置管理器
        
        Args:
            config_dir: 配置文件夹路径（相对 BASE_DIR）
        """
        self.config_dir = os.path.join(BASE_DIR, config_dir)
        self._ensure_dir_exists()
    
    def _ensure_dir_exists(self):
        """确保配置目录存在"""
        os.makedirs(self.config_dir, exist_ok=True)
    
    def _get_config_filename(self, config_name: str) -> str:
        """
        获取配置文件名
        使用配置名称的 MD5 哈希作为文件名，避免特殊字符问题
        """
        # 使用 MD5 哈希确保文件名合法
        name_hash = hashlib.md5(config_name.encode('utf-8')).hexdigest()[:16]
        return f"{name_hash}.json"
    
    def _get_config_path(self, config_name: str) -> str:
        """获取配置文件完整路径"""
        filename = self._get_config_filename(config_name)
        return os.path.join(self.config_dir, filename)
    
    def get_config(self, config_name: str) -> Optional[Dict[str, Any]]:
        """
        获取配置
        
        Args:
            config_name: 配置名称
            
        Returns:
            配置字典，如果不存在返回 None
        """
        config_path = self._get_config_path(config_name)
        
        if not os.path.exists(config_path):
            return None
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None
    
    def get_config_value(self, config_name: str, default: Any = None) -> Any:
        """
        获取配置值
        
        Args:
            config_name: 配置名称
            default: 默认值
            
        Returns:
            配置值，如果不存在返回 default
        """
        config = self.get_config(config_name)
        if config and 'config_value' in config:
            return config['config_value']
        return default
    
    def save_config(self, config_name: str, config_value: Any, note: str = ""):
        """
        保存配置
        
        Args:
            config_name: 配置名称
            config_value: 配置值
            note: 配置备注说明
        """
        config_path = self._get_config_path(config_name)
        
        # 读取现有配置（如果存在）以保留创建时间
        existing = self.get_config(config_name)
        created_at = existing['created_at'] if existing else datetime.now().isoformat()
        
        config_data = {
            'config_name': config_name,
            'config_value': config_value,
            'note': note,
            'created_at': created_at,
            'updated_at': datetime.now().isoformat(),
            'version': 1
        }
        
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, ensure_ascii=False, indent=2)
    
    def delete_config(self, config_name: str) -> bool:
        """
        删除配置
        
        Args:
            config_name: 配置名称
            
        Returns:
            是否成功删除
        """
        config_path = self._get_config_path(config_name)
        
        if os.path.exists(config_path):
            try:
                os.remove(config_path)
                return True
            except IOError:
                return False
        return False
    
    def clear_all_configs(self):
        """清除所有配置"""
        for filename in os.listdir(self.config_dir):
            if filename.endswith('.json'):
                filepath = os.path.join(self.config_dir, filename)
                try:
                    os.remove(filepath)
                except IOError:
                    pass
    
    def list_configs(self) -> list:
        """
        列出所有配置
        
        Returns:
            配置信息列表
        """
        configs = []
        
        if not os.path.exists(self.config_dir):
            return configs
        
        for filename in os.listdir(self.config_dir):
            if filename.endswith('.json'):
                filepath = os.path.join(self.config_dir, filename)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        config = json.load(f)
                        configs.append({
                            'config_name': config.get('config_name', 'Unknown'),
                            'note': config.get('note', ''),
                            'updated_at': config.get('updated_at', '')
                        })
                except (json.JSONDecodeError, IOError):
                    pass
        
        return sorted(configs, key=lambda x: x['updated_at'], reverse=True)


# 全局配置管理器实例
_config_manager: Optional[DynamicConfigManager] = None


def get_config_manager() -> DynamicConfigManager:
    """获取全局配置管理器实例"""
    global _config_manager
    if _config_manager is None:
        _config_manager = DynamicConfigManager()
    return _config_manager


def reset_config_manager():
    """重置配置管理器（主要用于测试）"""
    global _config_manager
    _config_manager = None
