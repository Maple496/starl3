"""
配置管理模块 - 支持外部配置注入
实现配置与代码分离，支持多环境管理
"""

import os
import re
import json
from pathlib import Path
from typing import Dict, Any, Optional
from core.constants import DATA_DIR
from core.registry import op


class ConfigLoader:
    """配置加载器 - 支持变量替换和配置合并"""
    
    VAR_PATTERN = re.compile(r'\$\{([^}]+)\}')
    
    def __init__(self, base_dir: str = ""):
        self.base_dir = base_dir
        self.external_configs: Dict[str, Any] = {}
        self.env_prefix = "STARL3_"
    
    def load_external_config(self, config_path: str, config_type: str = "auto") -> Dict:
        """加载外部配置文件"""
        full_path = os.path.join(self.base_dir, config_path)
        
        if not os.path.exists(full_path):
            return {}
        
        if config_type == "auto":
            ext = Path(full_path).suffix.lower()
            config_type = "yaml" if ext in ['.yml', '.yaml'] else "json"
        
        try:
            with open(full_path, 'r', encoding='utf-8') as f:
                if config_type == "json":
                    return json.load(f)
                elif config_type == "yaml":
                    try:
                        import yaml
                        return yaml.safe_load(f) or {}
                    except ImportError:
                        return json.load(f)
        except Exception as e:
            print(f"Error loading config from {full_path}: {e}")
        
        return {}
    
    def load_env_config(self, prefix: str = None) -> Dict:
        """从环境变量加载配置"""
        prefix = prefix or self.env_prefix
        config = {}
        
        for key, value in os.environ.items():
            if key.startswith(prefix):
                config_key = key[len(prefix):].lower().replace('_', '.')
                self._set_nested_value(config, config_key, self._convert_value(value))
        
        return config
    
    def _set_nested_value(self, config: Dict, key: str, value: Any):
        """设置嵌套字典值"""
        keys = key.split('.')
        current = config
        for k in keys[:-1]:
            if k not in current:
                current[k] = {}
            current = current[k]
        current[keys[-1]] = value
    
    def _convert_value(self, value: str) -> Any:
        """尝试将字符串转换为适当的类型"""
        if value.lower() in ('true', 'yes', 'on', '1'):
            return True
        if value.lower() in ('false', 'no', 'off', '0'):
            return False
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value
    
    def register_external(self, name: str, config: Dict):
        """注册外部配置"""
        self.external_configs[name] = config
    
    def resolve_variables(self, obj: Any) -> Any:
        """递归解析变量引用"""
        if isinstance(obj, str):
            return self._resolve_string(obj)
        elif isinstance(obj, dict):
            return {k: self.resolve_variables(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.resolve_variables(item) for item in obj]
        return obj
    
    def _resolve_string(self, value: str) -> Any:
        """解析字符串中的变量"""
        match = self.VAR_PATTERN.fullmatch(value)
        if match:
            var_path = match.group(1)
            result = self._get_variable_value(var_path)
            return result if result is not None else value
        
        return self.VAR_PATTERN.sub(
            lambda m: str(self._get_variable_value(m.group(1)) or m.group(0)), 
            value
        )
    
    def _get_variable_value(self, var_path: str) -> Any:
        """获取变量值，支持多级路径"""
        if var_path.startswith('ENV.'):
            return os.environ.get(var_path[4:])
        
        parts = var_path.split('.')
        if parts[0] in self.external_configs:
            config = self.external_configs[parts[0]]
            for part in parts[1:]:
                if isinstance(config, dict) and part in config:
                    config = config[part]
                else:
                    return None
            return config
        
        env_key = var_path.replace('.', '_').upper()
        if env_key in os.environ:
            return self._convert_value(os.environ[env_key])
        
        return None
    
    def merge_configs(self, *configs: Dict) -> Dict:
        """合并多个配置字典"""
        result = {}
        for config in configs:
            self._deep_merge(result, config)
        return result
    
    def _deep_merge(self, target: Dict, source: Dict):
        """深度合并字典"""
        for key, value in source.items():
            if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                self._deep_merge(target[key], value)
            else:
                target[key] = value


# 全局配置加载器实例
_config_loader: Optional[ConfigLoader] = None


def _get_loader(ctx: Dict) -> ConfigLoader:
    """获取或创建配置加载器"""
    global _config_loader
    if _config_loader is None:
        _config_loader = ConfigLoader(ctx.get("base_dir", DATA_DIR))
    return _config_loader


@op("load_external_config", category="config", description="加载外部配置文件")
def op_load_external_config(ctx, params):
    """加载外部配置文件"""
    loader = _get_loader(ctx)
    
    config_path = params.get("file", "")
    config_name = params.get("name", "external")
    config_type = params.get("type", "auto")
    
    if not config_path:
        return {"status": "error", "message": "未指定配置文件路径"}
    
    config = loader.load_external_config(config_path, config_type)
    loader.register_external(config_name, config)
    
    return {"status": "success", "name": config_name, "file": config_path, "config": config}


@op("load_env_config", category="config", description="从环境变量加载配置")
def op_load_env_config(ctx, params):
    """从环境变量加载配置"""
    loader = _get_loader(ctx)
    
    prefix = params.get("prefix", "STARL3_")
    config_name = params.get("name", "env")
    
    config = loader.load_env_config(prefix)
    loader.register_external(config_name, config)
    
    return {"status": "success", "name": config_name, "prefix": prefix, "config": config}


@op("resolve_config", category="config", description="解析配置中的变量")
def op_resolve_config(ctx, params):
    """解析配置中的变量"""
    loader = _get_loader(ctx)
    
    if "config" in params:
        config = params["config"]
    elif isinstance(ctx.get("last_result"), dict):
        config = ctx["last_result"]
    else:
        return {"status": "error", "message": "未找到待解析的配置"}
    
    resolved = loader.resolve_variables(config)
    
    return {"status": "success", "original": config, "resolved": resolved}


@op("get_config_value", category="config", description="获取配置中的特定值")
def op_get_config_value(ctx, params):
    """获取配置中的特定值"""
    loader = _get_loader(ctx)
    
    path = params.get("path", "")
    default = params.get("default")
    
    if not path:
        return {"status": "error", "message": "未指定配置路径"}
    
    value = loader._get_variable_value(path)
    
    return {
        "status": "success",
        "path": path,
        "value": value if value is not None else default,
        "found": value is not None
    }


@op("set_config_value", category="config", description="设置配置值（运行时）")
def op_set_config_value(ctx, params):
    """设置配置值（运行时）"""
    loader = _get_loader(ctx)
    
    config_name = params.get("name", "runtime")
    path = params.get("path", "")
    value = params.get("value")
    
    if config_name not in loader.external_configs:
        loader.external_configs[config_name] = {}
    
    loader._set_nested_value(loader.external_configs[config_name], path, value)
    
    return {"status": "success", "name": config_name, "path": path, "value": value}


@op("merge_configs", category="config", description="合并多个配置")
def op_merge_configs(ctx, params):
    """合并多个配置"""
    loader = _get_loader(ctx)
    
    config_names = params.get("configs", [])
    
    configs_to_merge = [loader.external_configs[name] for name in config_names if name in loader.external_configs]
    
    if isinstance(ctx.get("last_result"), dict):
        configs_to_merge.append(ctx["last_result"])
    
    if not configs_to_merge:
        return {"status": "error", "message": "没有找到可合并的配置"}
    
    merged = loader.merge_configs(*configs_to_merge)
    resolved = loader.resolve_variables(merged)
    
    return {"status": "success", "merged": resolved, "sources": config_names}



