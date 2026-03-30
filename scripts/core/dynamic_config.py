"""
动态配置管理模块
支持 CSV 作为主存储，极简格式：config_name, value
"""
import os
import json
import csv
from datetime import datetime
from typing import Optional, Dict, Any, List
from core.constants import DATA_DIR


class DynamicConfigManager:
    """动态配置管理器 - CSV 极简存储"""
    
    # 极简 CSV 表头：只保留配置名和值
    CSV_HEADERS = ['config_name', 'value']
    
    def __init__(self, config_dir: str = "dynamic_configs"):
        self.config_dir = os.path.join(DATA_DIR, config_dir)
        self.csv_path = os.path.join(self.config_dir, "configs.csv")
        os.makedirs(self.config_dir, exist_ok=True)
        self._ensure_csv_exists()
    
    def _ensure_csv_exists(self):
        """确保 CSV 文件存在且有表头"""
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=self.CSV_HEADERS)
                writer.writeheader()
    
    def _read_all_rows(self) -> List[Dict[str, str]]:
        """读取所有行"""
        if not os.path.exists(self.csv_path):
            return []
        
        encodings = ['utf-8-sig', 'gbk', 'utf-8']
        for encoding in encodings:
            try:
                with open(self.csv_path, 'r', encoding=encoding, newline='') as f:
                    return list(csv.DictReader(f))
            except (IOError, UnicodeDecodeError):
                continue
        return []
    
    def _write_all_rows(self, rows: List[Dict[str, str]]):
        """写入所有行"""
        with open(self.csv_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=self.CSV_HEADERS)
            writer.writeheader()
            writer.writerows(rows)
    
    def _value_to_str(self, value: Any) -> str:
        """将任意值转为字符串存储"""
        if value is None:
            return ""
        elif isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        else:
            return str(value)
    
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
        """获取配置"""
        rows = self._read_all_rows()
        for row in rows:
            if row.get('config_name') == config_name:
                value_str = row.get('value', '')
                return {
                    'config_name': config_name,
                    'config_value': self._str_to_value(value_str)
                }
        return None
    
    def get_config_value(self, config_name: str, default: Any = None) -> Any:
        """获取配置值"""
        config = self.get_config(config_name)
        if config:
            return config['config_value']
        return default
    
    def save_config(self, config_name: str, config_value: Any, note: str = ""):
        """保存配置（note 参数保留兼容但不存储）"""
        rows = self._read_all_rows()
        value_str = self._value_to_str(config_value)
        
        # 查找并更新或追加
        found = False
        for row in rows:
            if row.get('config_name') == config_name:
                row['value'] = value_str
                found = True
                break
        
        if not found:
            rows.append({
                'config_name': config_name,
                'value': value_str
            })
        
        self._write_all_rows(rows)
    
    def delete_config(self, config_name: str) -> bool:
        """删除配置"""
        rows = self._read_all_rows()
        original_len = len(rows)
        rows = [r for r in rows if r.get('config_name') != config_name]
        
        if len(rows) < original_len:
            self._write_all_rows(rows)
            return True
        return False
    
    def clear_all_configs(self):
        """清除所有配置"""
        with open(self.csv_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=self.CSV_HEADERS)
            writer.writeheader()
    
    def list_configs(self) -> list:
        """列出所有配置"""
        rows = self._read_all_rows()
        result = []
        for row in rows:
            name = row.get('config_name', '')
            value = self._str_to_value(row.get('value', ''))
            # 生成预览
            if isinstance(value, str):
                preview = value[:50] + "..." if len(value) > 50 else value
            else:
                preview = str(value)[:50]
            
            result.append({
                'config_name': name,
                'value_preview': preview
            })
        return result
    
    def open_csv(self):
        """用默认程序打开 CSV 文件"""
        import subprocess
        import platform
        
        self._ensure_csv_exists()
        system = platform.system()
        try:
            if system == 'Windows':
                os.startfile(self.csv_path)
            elif system == 'Darwin':
                subprocess.call(['open', self.csv_path])
            else:
                subprocess.call(['xdg-open', self.csv_path])
        except Exception as e:
            print(f"无法打开 CSV: {e}")
            print(f"路径: {self.csv_path}")


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
    header = f"{'Config Name':<{name_width}} | Value Preview"
    lines.append(header)
    lines.append('-' * len(header))
    
    for c in configs:
        preview = c['value_preview'][:40]
        lines.append(f"{c['config_name']:<{name_width}} | {preview}")
    
    lines.append('')
    lines.append(f"共 {len(configs)} 条配置")
    lines.append(f"CSV: {manager.csv_path}")
    
    return '\n'.join(lines)
