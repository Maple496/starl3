"""
动态配置管理模块
"""
import os
import json
from datetime import datetime
from typing import Optional, Dict, Any, List
from core.constants import DATA_DIR


class DynamicConfigManager:
    """动态配置管理器 - JSON 存储"""
    
    def __init__(self, config_dir: str = "dynamic_configs"):
        self.config_dir = os.path.join(DATA_DIR, config_dir)
        self.json_path = os.path.join(self.config_dir, "configs.json")
        os.makedirs(self.config_dir, exist_ok=True)
        self._ensure_json_exists()
    
    def _ensure_json_exists(self):
        """确保 JSON 文件存在"""
        if not os.path.exists(self.json_path):
            self._save_data({'version': '1.0', 'configs': []})
    
    def _load_data(self) -> Dict[str, Any]:
        """加载 JSON 数据"""
        try:
            with open(self.json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {'version': '1.0', 'configs': []}
    
    def _save_data(self, data: Dict[str, Any]):
        """保存 JSON 数据"""
        with open(self.json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def _find_config(self, configs: List[Dict], name: str) -> Optional[Dict]:
        """查找配置项"""
        for config in configs:
            if config.get('name') == name:
                return config
        return None
    
    def get_config(self, config_name: str) -> Optional[Dict[str, Any]]:
        """获取配置（包含所有字段）"""
        data = self._load_data()
        config = self._find_config(data.get('configs', []), config_name)
        if config:
            return {
                'config_name': config['name'],
                'config_value': config['value'],
                'note': config.get('note', ''),
                'created_at': config.get('created_at', ''),
                'updated_at': config.get('updated_at', '')
            }
        return None
    
    def get_config_value(self, config_name: str, default: Any = None) -> Any:
        """获取配置值"""
        config = self.get_config(config_name)
        return config['config_value'] if config else default
    
    def save_config(self, config_name: str, config_value: Any, note: str = None):
        """保存配置"""
        data = self._load_data()
        configs = data.get('configs', [])
        now = datetime.now().isoformat()
        
        existing = self._find_config(configs, config_name)
        if existing:
            existing['value'] = config_value
            existing['updated_at'] = now
            if note is not None:
                existing['note'] = note
        else:
            configs.append({
                'name': config_name,
                'value': config_value,
                'note': note or "",
                'created_at': now,
                'updated_at': now
            })
        
        self._save_data(data)
    
    def update_config_note(self, config_name: str, note: str) -> bool:
        """更新配置备注"""
        data = self._load_data()
        config = self._find_config(data.get('configs', []), config_name)
        if config:
            config['note'] = note
            config['updated_at'] = datetime.now().isoformat()
            self._save_data(data)
            return True
        return False
    
    def delete_config(self, config_name: str) -> bool:
        """删除配置"""
        data = self._load_data()
        configs = data.get('configs', [])
        original_len = len(configs)
        data['configs'] = [c for c in configs if c.get('name') != config_name]
        
        if len(data['configs']) < original_len:
            self._save_data(data)
            return True
        return False
    
    def list_configs(self) -> List[Dict[str, Any]]:
        """列出所有配置"""
        data = self._load_data()
        result = []
        for config in data.get('configs', []):
            value = config.get('value', '')
            preview = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
            result.append({
                'name': config.get('name', ''),
                'value_preview': preview,
                'note': config.get('note', ''),
                'created_at': config.get('created_at', ''),
                'updated_at': config.get('updated_at', '')
            })
        return result
    
    def open_config_file(self):
        """用默认程序打开配置文件"""
        import subprocess
        import platform
        
        self._ensure_json_exists()
        system = platform.system()
        try:
            if system == 'Windows':
                os.startfile(self.json_path)
            elif system == 'Darwin':
                subprocess.call(['open', self.json_path])
            else:
                subprocess.call(['xdg-open', self.json_path])
        except Exception as e:
            print(f"无法打开配置文件: {e}")
            print(f"路径: {self.json_path}")


_config_manager: Optional[DynamicConfigManager] = None


def get_config_manager() -> DynamicConfigManager:
    """获取全局配置管理器"""
    global _config_manager
    if _config_manager is None:
        _config_manager = DynamicConfigManager()
    return _config_manager


def reset_config_manager():
    """重置配置管理器"""
    global _config_manager
    _config_manager = None
