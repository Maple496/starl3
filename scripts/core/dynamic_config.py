"""
动态配置管理模块
支持 JSON 作为主存储，格式：name, value, note, created_at, updated_at
"""
import os
import json
import csv
from datetime import datetime
from typing import Optional, Dict, Any, List
from core.constants import DATA_DIR


class ConfigItem:
    """配置项数据类"""
    def __init__(self, name: str, value: Any, note: str = "", 
                 created_at: str = None, updated_at: str = None):
        self.name = name
        self.value = value
        self.note = note
        self.created_at = created_at or datetime.now().isoformat()
        self.updated_at = updated_at or datetime.now().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'value': self.value,
            'note': self.note,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ConfigItem':
        return cls(
            name=data.get('name', ''),
            value=data.get('value'),
            note=data.get('note', ''),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at')
        )


class DynamicConfigManager:
    """动态配置管理器 - JSON 存储"""
    
    def __init__(self, config_dir: str = "dynamic_configs"):
        self.config_dir = os.path.join(DATA_DIR, config_dir)
        self.json_path = os.path.join(self.config_dir, "configs.json")
        self.csv_path = os.path.join(self.config_dir, "configs.csv")  # 旧格式
        os.makedirs(self.config_dir, exist_ok=True)
        
        # 检查是否需要从 CSV 迁移
        self._migrate_from_csv_if_needed()
        self._ensure_json_exists()
    
    def _migrate_from_csv_if_needed(self):
        """如果存在旧版 CSV，自动迁移到 JSON"""
        if os.path.exists(self.csv_path) and not os.path.exists(self.json_path):
            print(f"[INFO] 检测到旧版 CSV 配置，正在迁移到 JSON...")
            try:
                configs = []
                encodings = ['utf-8-sig', 'gbk', 'utf-8']
                rows = []
                
                for encoding in encodings:
                    try:
                        with open(self.csv_path, 'r', encoding=encoding, newline='') as f:
                            rows = list(csv.DictReader(f))
                            break
                    except (IOError, UnicodeDecodeError):
                        continue
                
                for row in rows:
                    name = row.get('config_name', '')
                    value_str = row.get('value', '')
                    if name:
                        value = self._str_to_value(value_str)
                        configs.append(ConfigItem(name=name, value=value))
                
                # 保存为 JSON
                self._save_configs_list(configs)
                
                # 备份旧 CSV
                backup_path = self.csv_path + '.backup'
                os.rename(self.csv_path, backup_path)
                print(f"[INFO] 迁移完成，已备份旧文件: {backup_path}")
                
            except Exception as e:
                print(f"[ERROR] 迁移失败: {e}")
    
    def _ensure_json_exists(self):
        """确保 JSON 文件存在"""
        if not os.path.exists(self.json_path):
            self._save_data({'version': '1.0', 'configs': []})
    
    def _load_data(self) -> Dict[str, Any]:
        """加载 JSON 数据"""
        if not os.path.exists(self.json_path):
            return {'version': '1.0', 'configs': []}
        
        try:
            with open(self.json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"[ERROR] 加载配置失败: {e}")
            return {'version': '1.0', 'configs': []}
    
    def _save_data(self, data: Dict[str, Any]):
        """保存 JSON 数据"""
        with open(self.json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def _load_configs_list(self) -> List[ConfigItem]:
        """加载所有配置项"""
        data = self._load_data()
        return [ConfigItem.from_dict(c) for c in data.get('configs', [])]
    
    def _save_configs_list(self, configs: List[ConfigItem]):
        """保存所有配置项"""
        data = {
            'version': '1.0',
            'configs': [c.to_dict() for c in configs]
        }
        self._save_data(data)
    
    def _str_to_value(self, value_str: str) -> Any:
        """将字符串转回原始值"""
        if value_str == "":
            return None
        # 尝试解析 JSON
        try:
            return json.loads(value_str)
        except json.JSONDecodeError:
            pass
        # 尝试解析数字
        try:
            if '.' in value_str:
                return float(value_str)
            return int(value_str)
        except ValueError:
            pass
        # 布尔值
        if value_str.lower() in ('true', 'false'):
            return value_str.lower() == 'true'
        # 默认字符串
        return value_str
    
    def get_config(self, config_name: str) -> Optional[Dict[str, Any]]:
        """获取配置（包含所有字段）"""
        configs = self._load_configs_list()
        for config in configs:
            if config.name == config_name:
                return {
                    'config_name': config.name,
                    'config_value': config.value,
                    'note': config.note,
                    'created_at': config.created_at,
                    'updated_at': config.updated_at
                }
        return None
    
    def get_config_value(self, config_name: str, default: Any = None) -> Any:
        """获取配置值"""
        config = self.get_config(config_name)
        if config:
            return config['config_value']
        return default
    
    def save_config(self, config_name: str, config_value: Any, note: str = None):
        """保存配置"""
        configs = self._load_configs_list()
        now = datetime.now().isoformat()
        
        # 查找并更新
        found = False
        for config in configs:
            if config.name == config_name:
                config.value = config_value
                if note is not None:
                    config.note = note
                config.updated_at = now
                found = True
                break
        
        # 新增
        if not found:
            configs.append(ConfigItem(
                name=config_name,
                value=config_value,
                note=note or "",
                created_at=now,
                updated_at=now
            ))
        
        self._save_configs_list(configs)
    
    def update_config_note(self, config_name: str, note: str):
        """更新配置备注"""
        configs = self._load_configs_list()
        for config in configs:
            if config.name == config_name:
                config.note = note
                config.updated_at = datetime.now().isoformat()
                self._save_configs_list(configs)
                return True
        return False
    
    def delete_config(self, config_name: str) -> bool:
        """删除配置"""
        configs = self._load_configs_list()
        original_len = len(configs)
        configs = [c for c in configs if c.name != config_name]
        
        if len(configs) < original_len:
            self._save_configs_list(configs)
            return True
        return False
    
    def clear_all_configs(self):
        """清除所有配置"""
        self._save_data({'version': '1.0', 'configs': []})
    
    def list_configs(self) -> List[Dict[str, Any]]:
        """列出所有配置"""
        configs = self._load_configs_list()
        result = []
        for config in configs:
            # 生成值预览
            if isinstance(config.value, str):
                preview = config.value[:50] + "..." if len(config.value) > 50 else config.value
            else:
                preview = str(config.value)[:50]
            
            result.append({
                'name': config.name,
                'value_preview': preview,
                'note': config.note,
                'created_at': config.created_at,
                'updated_at': config.updated_at
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


# 全局实例
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


def list_configs_table() -> str:
    """生成配置列表表格"""
    manager = get_config_manager()
    configs = manager.list_configs()
    
    if not configs:
        return "暂无配置"
    
    name_width = max(25, max(len(c['config_name']) for c in configs))
    
    lines = []
    header = f"{'Config Name':<{name_width}} | {'Note':<20} | Value Preview"
    lines.append(header)
    lines.append('-' * len(header))
    
    for c in configs:
        preview = c['value_preview'][:40]
        note = (c.get('note', '') or '')[:18]
        lines.append(f"{c['config_name']:<{name_width}} | {note:<20} | {preview}")
    
    lines.append('')
    lines.append(f"共 {len(configs)} 条配置")
    lines.append(f"JSON: {manager.json_path}")
    
    return '\n'.join(lines)
