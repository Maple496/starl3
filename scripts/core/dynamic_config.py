"""
动态配置管理模块
支持缓存用户选择和输入，避免重复弹窗
使用 CSV 索引文件实现直观的管理界面
"""
import os
import json
import csv
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from core.constants import DATA_DIR


class DynamicConfigManager:
    """动态配置管理器 - 支持 CSV 索引"""
    
    def __init__(self, config_dir: str = "dynamic_configs"):
        """
        初始化配置管理器
        
        Args:
            config_dir: 配置文件夹路径（相对 DATA_DIR）
        """
        self.config_dir = os.path.join(DATA_DIR, config_dir)
        self.csv_index_path = os.path.join(self.config_dir, "configs_index.csv")
        self._ensure_dir_exists()
    
    def _ensure_dir_exists(self):
        """确保配置目录存在"""
        os.makedirs(self.config_dir, exist_ok=True)
    
    def _get_pipeline_dir(self, config_name: str) -> str:
        """
        获取配置所属的 Pipeline 目录
        从 config_name 中提取 pipeline 名称（第一部分）
        """
        parts = config_name.split('.')
        if len(parts) >= 2:
            return parts[0]
        return "global"
    
    def _get_config_path(self, config_name: str) -> str:
        """获取配置文件完整路径 - 按 Pipeline 分组存储"""
        pipeline = self._get_pipeline_dir(config_name)
        pipeline_dir = os.path.join(self.config_dir, pipeline)
        os.makedirs(pipeline_dir, exist_ok=True)
        
        # 使用安全的文件名（去掉 pipeline 前缀）
        config_short_name = config_name[len(pipeline)+1:] if config_name.startswith(pipeline + '.') else config_name
        safe_name = "".join(c if c.isalnum() or c in ('-', '_') else '_' for c in config_short_name)
        filename = f"{safe_name}.json"
        return os.path.join(pipeline_dir, filename)
    
    def _get_old_config_path(self, config_name: str) -> str:
        """获取旧格式配置文件路径（用于兼容和清理）"""
        import hashlib
        name_hash = hashlib.md5(config_name.encode('utf-8')).hexdigest()[:16]
        filename = f"{name_hash}.json"
        return os.path.join(self.config_dir, filename)
    
    def _migrate_old_config(self, config_name: str) -> bool:
        """迁移旧格式配置到新格式"""
        old_path = self._get_old_config_path(config_name)
        new_path = self._get_config_path(config_name)
        
        if os.path.exists(old_path) and not os.path.exists(new_path):
            try:
                with open(old_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                with open(new_path, 'w', encoding='utf-8') as f:
                    json.dump(config, f, ensure_ascii=False, indent=2)
                os.remove(old_path)
                return True
            except (IOError, json.JSONDecodeError):
                pass
        return False
    
    def _get_relative_path(self, full_path: str) -> str:
        """获取相对于 config_dir 的路径"""
        try:
            return os.path.relpath(full_path, self.config_dir)
        except ValueError:
            return full_path
    
    def _update_csv_index(self):
        """更新 CSV 索引文件 - 扫描所有配置并重建索引"""
        configs = self._scan_all_configs()
        
        # CSV 表头
        headers = [
            'pipeline',
            'config_name', 
            'description', 
            'value_type', 
            'value_preview', 
            'json_file', 
            'updated_at',
            'note'
        ]
        
        # 按 pipeline 和 config_name 排序
        configs.sort(key=lambda x: (x['pipeline'], x['config_name']))
        
        with open(self.csv_index_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(configs)
    
    def _scan_all_configs(self) -> List[Dict[str, str]]:
        """扫描所有配置文件，返回索引列表（只扫描子目录，忽略根目录旧文件）"""
        configs = []
        
        if not os.path.exists(self.config_dir):
            return configs
        
        for root, dirs, files in os.walk(self.config_dir):
            # 跳过根目录（只扫描子目录中的配置文件）
            if root == self.config_dir:
                continue
            
            for filename in files:
                if not filename.endswith('.json'):
                    continue
                
                filepath = os.path.join(root, filename)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        config = json.load(f)
                    
                    config_name = config.get('config_name', 'Unknown')
                    config_value = config.get('config_value', '')
                    
                    # 确定 value_type 和预览
                    value_type, value_preview = self._analyze_value(config_value)
                    
                    configs.append({
                        'pipeline': self._get_pipeline_dir(config_name),
                        'config_name': config_name,
                        'description': self._generate_description(config_name),
                        'value_type': value_type,
                        'value_preview': value_preview[:80] + '...' if len(value_preview) > 80 else value_preview,
                        'json_file': self._get_relative_path(filepath),
                        'updated_at': config.get('updated_at', ''),
                        'note': config.get('note', '')
                    })
                except (json.JSONDecodeError, IOError):
                    continue
        
        return configs
    
    def _analyze_value(self, value: Any) -> tuple:
        """分析值的类型和预览"""
        if value is None:
            return "null", "<空>"
        elif isinstance(value, bool):
            return "boolean", str(value)
        elif isinstance(value, (int, float)):
            return "number", str(value)
        elif isinstance(value, str):
            if len(value) > 100:
                return "string", value[:100] + "..."
            return "string", value
        elif isinstance(value, list):
            if len(value) == 0:
                return "array", "<空数组>"
            preview = str(value[0]) if len(value) == 1 else f"[{value[0]}, ...] 共{len(value)}项"
            return "array", preview[:80]
        elif isinstance(value, dict):
            keys = list(value.keys())
            if len(keys) == 0:
                return "object", "<空对象>"
            preview = f"{{{', '.join(keys[:3])}}}"
            if len(keys) > 3:
                preview += f" 等{len(keys)}个键"
            return "object", preview
        else:
            return "unknown", str(value)[:80]
    
    def _generate_description(self, config_name: str) -> str:
        """根据配置名生成描述"""
        # 常见的配置名映射（关键词 -> 描述）
        descriptions = {
            'source_dir': '源文件目录',
            'output_dir': '输出目录',
            'html_dir': 'HTML输出目录',
            'mapping_file': '字段映射文件',
            'last_directory': '上次使用的目录',
            'last_file': '上次使用的文件',
        }
        
        # 提取 pipeline 名称
        parts = config_name.split('.')
        if len(parts) >= 2:
            pipeline = parts[0]
        else:
            pipeline = 'global'
        
        # 尝试从配置名中匹配关键词
        config_lower = config_name.lower()
        for k, v in descriptions.items():
            if k in config_lower:
                return f"{pipeline} - {v}"
        
        # 默认返回 pipeline + 原始名称
        return f"{pipeline} - {config_name}"
    
    def get_config(self, config_name: str) -> Optional[Dict[str, Any]]:
        """
        获取配置（自动迁移旧格式）
        
        Args:
            config_name: 配置名称
            
        Returns:
            配置字典，如果不存在返回 None
        """
        config_path = self._get_config_path(config_name)
        
        # 如果新格式不存在，尝试迁移旧格式
        if not os.path.exists(config_path):
            self._migrate_old_config(config_name)
        
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
        保存配置 - 同时更新 CSV 索引
        
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
        
        # 更新 CSV 索引
        self._update_csv_index()
    
    def delete_config(self, config_name: str) -> bool:
        """
        删除配置 - 同时更新 CSV 索引
        
        Args:
            config_name: 配置名称
            
        Returns:
            是否成功删除
        """
        config_path = self._get_config_path(config_name)
        deleted = False
        
        # 删除新格式文件
        if os.path.exists(config_path):
            try:
                os.remove(config_path)
                deleted = True
            except IOError:
                pass
        
        # 同时删除旧格式文件（如果存在）
        old_path = self._get_old_config_path(config_name)
        if os.path.exists(old_path):
            try:
                os.remove(old_path)
                deleted = True
            except IOError:
                pass
        
        if deleted:
            # 清理空目录
            pipeline_dir = os.path.dirname(config_path)
            if os.path.exists(pipeline_dir) and not os.listdir(pipeline_dir):
                try:
                    os.rmdir(pipeline_dir)
                except IOError:
                    pass
            # 更新 CSV 索引
            self._update_csv_index()
            return True
        return False
    
    def clear_all_configs(self):
        """清除所有配置 - 同时清空 CSV 索引"""
        if os.path.exists(self.config_dir):
            for root, dirs, files in os.walk(self.config_dir, topdown=False):
                for filename in files:
                    # 保留 CSV 索引文件
                    if filename.endswith('.json'):
                        filepath = os.path.join(root, filename)
                        try:
                            os.remove(filepath)
                        except IOError:
                            pass
                # 删除空目录（保留根目录）
                for dirname in dirs:
                    dirpath = os.path.join(root, dirname)
                    if dirpath != self.config_dir:
                        try:
                            if not os.listdir(dirpath):
                                os.rmdir(dirpath)
                        except IOError:
                            pass
        
        # 重建空索引
        self._update_csv_index()
    
    def list_configs(self) -> list:
        """
        列出所有配置 - 从 CSV 索引读取
        
        Returns:
            配置信息列表
        """
        if not os.path.exists(self.csv_index_path):
            return []
        
        configs = []
        try:
            with open(self.csv_index_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    configs.append({
                        'pipeline': row.get('pipeline', ''),
                        'config_name': row.get('config_name', ''),
                        'description': row.get('description', ''),
                        'value_type': row.get('value_type', ''),
                        'value_preview': row.get('value_preview', ''),
                        'json_file': row.get('json_file', ''),
                        'updated_at': row.get('updated_at', ''),
                        'note': row.get('note', '')
                    })
        except (IOError, csv.Error):
            pass
        
        return configs
    
    def open_csv_index(self):
        """用默认程序打开 CSV 索引文件（方便用户查看/编辑）"""
        import subprocess
        import platform
        
        # 确保索引存在
        if not os.path.exists(self.csv_index_path):
            self._update_csv_index()
        
        system = platform.system()
        try:
            if system == 'Windows':
                os.startfile(self.csv_index_path)
            elif system == 'Darwin':  # macOS
                subprocess.call(['open', self.csv_index_path])
            else:  # Linux
                subprocess.call(['xdg-open', self.csv_index_path])
        except Exception as e:
            print(f"无法自动打开 CSV 文件: {e}")
            print(f"请手动打开: {self.csv_index_path}")


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


def list_configs_table() -> str:
    """
    生成配置列表的表格字符串 - 用于 CLI 显示
    
    Returns:
        格式化的表格字符串
    """
    manager = get_config_manager()
    configs = manager.list_configs()
    
    if not configs:
        return "暂无配置记录"
    
    # 计算列宽
    pipeline_width = max(12, max(len(c['pipeline']) for c in configs))
    name_width = max(20, max(len(c['config_name']) for c in configs))
    desc_width = max(25, max(len(c['description']) for c in configs))
    type_width = max(8, max(len(c['value_type']) for c in configs))
    
    lines = []
    
    # 表头
    header = f"{'Pipeline':<{pipeline_width}} | {'Config Name':<{name_width}} | {'Description':<{desc_width}} | {'Type':<{type_width}} | {'Updated At'}"
    lines.append(header)
    lines.append('-' * len(header))
    
    # 数据行
    for config in configs:
        line = f"{config['pipeline']:<{pipeline_width}} | {config['config_name']:<{name_width}} | {config['description']:<{desc_width}} | {config['value_type']:<{type_width}} | {config['updated_at'][:19]}"
        lines.append(line)
    
    lines.append('')
    lines.append(f"共 {len(configs)} 条配置")
    lines.append(f"CSV 索引: {manager.csv_index_path}")
    
    return '\n'.join(lines)
