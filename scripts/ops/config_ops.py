"""
配置管理模块 - 支持外部配置注入
实现配置与代码分离，支持多环境管理
"""

import os
import re
import json
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from tkinter import Tk, filedialog
from core.pipeline_engine import PipelineEngine
from core.constants import BASE_DIR, DATA_DIR
from core.registry import op


class ConfigLoader:
    """配置加载器 - 支持变量替换和配置合并"""
    
    # 变量匹配模式: ${var} 或 ${source.var}
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
        
        # 自动检测类型
        if config_type == "auto":
            ext = Path(full_path).suffix.lower()
            if ext == '.json':
                config_type = "json"
            elif ext in ['.yml', '.yaml']:
                config_type = "yaml"
            else:
                config_type = "json"
        
        try:
            with open(full_path, 'r', encoding='utf-8') as f:
                if config_type == "json":
                    return json.load(f)
                elif config_type == "yaml":
                    try:
                        import yaml
                        return yaml.safe_load(f) or {}
                    except ImportError:
                        print("Warning: PyYAML not installed, treating as JSON")
                        return json.load(f)
        except Exception as e:
            print(f"Error loading config from {full_path}: {e}")
            return {}
        
        return {}
    
    def load_env_config(self, prefix: str = None) -> Dict:
        """从环境变量加载配置"""
        prefix = prefix or self.env_prefix
        config = {}
        
        for key, value in os.environ.items():
            if key.startswith(prefix):
                # 去掉前缀，转换为小写
                config_key = key[len(prefix):].lower()
                # 支持嵌套键，如 STARL3_DB_HOST -> db.host
                config_key = config_key.replace('_', '.')
                self._set_nested_value(config, config_key, value)
        
        return config
    
    def _set_nested_value(self, config: Dict, key: str, value: Any):
        """设置嵌套字典值"""
        keys = key.split('.')
        current = config
        
        for k in keys[:-1]:
            if k not in current:
                current[k] = {}
            current = current[k]
        
        # 尝试转换值类型
        current[keys[-1]] = self._convert_value(value)
    
    def _convert_value(self, value: str) -> Any:
        """尝试将字符串转换为适当的类型"""
        # 布尔值
        if value.lower() in ('true', 'yes', 'on', '1'):
            return True
        if value.lower() in ('false', 'no', 'off', '0'):
            return False
        
        # 整数
        try:
            return int(value)
        except ValueError:
            pass
        
        # 浮点数
        try:
            return float(value)
        except ValueError:
            pass
        
        # 保持字符串
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
        else:
            return obj
    
    def _resolve_string(self, value: str) -> Any:
        """解析字符串中的变量"""
        if not isinstance(value, str):
            return value
        
        # 检查是否整个字符串都是变量
        match = self.VAR_PATTERN.fullmatch(value)
        if match:
            var_path = match.group(1)
            result = self._get_variable_value(var_path)
            return result if result is not None else value
        
        # 部分替换
        def replace_var(m):
            var_path = m.group(1)
            result = self._get_variable_value(var_path)
            return str(result) if result is not None else m.group(0)
        
        return self.VAR_PATTERN.sub(replace_var, value)
    
    def _get_variable_value(self, var_path: str) -> Any:
        """获取变量值，支持多级路径"""
        # 检查是否是环境变量
        if var_path.startswith('ENV.'):
            env_key = var_path[4:]
            return os.environ.get(env_key)
        
        # 检查是否是已注册的外部配置
        parts = var_path.split('.')
        
        # 首先检查外部配置名
        if parts[0] in self.external_configs:
            config = self.external_configs[parts[0]]
            # 遍历剩余路径
            for part in parts[1:]:
                if isinstance(config, dict) and part in config:
                    config = config[part]
                else:
                    return None
            return config
        
        # 检查环境变量（不带前缀）
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


def op_load_external_config(ctx, params):
    """加载外部配置文件
    
    参数:
        file: 配置文件路径
        name: 注册名称，供后续引用
        type: 配置文件类型 (json/yaml/auto)
    """
    loader = _get_loader(ctx)
    
    config_path = params.get("file", "")
    config_name = params.get("name", "external")
    config_type = params.get("type", "auto")
    
    if not config_path:
        return {"status": "error", "message": "未指定配置文件路径"}
    
    config = loader.load_external_config(config_path, config_type)
    loader.register_external(config_name, config)
    
    return {
        "status": "success",
        "name": config_name,
        "file": config_path,
        "config": config
    }


def op_load_env_config(ctx, params):
    """从环境变量加载配置
    
    参数:
        prefix: 环境变量前缀，默认 "STARL3_"
        name: 注册名称
    """
    loader = _get_loader(ctx)
    
    prefix = params.get("prefix", "STARL3_")
    config_name = params.get("name", "env")
    
    config = loader.load_env_config(prefix)
    loader.register_external(config_name, config)
    
    return {
        "status": "success", 
        "name": config_name,
        "prefix": prefix,
        "config": config
    }


def op_resolve_config(ctx, params):
    """解析配置中的变量
    
    参数:
        config: 需要解析的配置字典
        或从 last_result 读取
    """
    loader = _get_loader(ctx)
    
    # 获取待解析的配置
    if "config" in params:
        config = params["config"]
    elif isinstance(ctx.get("last_result"), dict):
        config = ctx["last_result"]
    else:
        return {"status": "error", "message": "未找到待解析的配置"}
    
    # 解析变量
    resolved = loader.resolve_variables(config)
    
    return {
        "status": "success",
        "original": config,
        "resolved": resolved
    }


def op_get_config_value(ctx, params):
    """获取配置中的特定值
    
    参数:
        path: 配置路径，如 "external.db.host"
        default: 默认值
    """
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


def op_set_config_value(ctx, params):
    """设置配置值（运行时）
    
    参数:
        name: 配置名称
        path: 配置路径
        value: 值
    """
    loader = _get_loader(ctx)
    
    config_name = params.get("name", "runtime")
    path = params.get("path", "")
    value = params.get("value")
    
    if config_name not in loader.external_configs:
        loader.external_configs[config_name] = {}
    
    loader._set_nested_value(loader.external_configs[config_name], path, value)
    
    return {
        "status": "success",
        "name": config_name,
        "path": path,
        "value": value
    }


def op_merge_configs(ctx, params):
    """合并多个配置
    
    参数:
        configs: 配置名称列表，如 ["external", "env"]
        或从 last_result 读取
    """
    loader = _get_loader(ctx)
    
    config_names = params.get("configs", [])
    
    configs_to_merge = []
    for name in config_names:
        if name in loader.external_configs:
            configs_to_merge.append(loader.external_configs[name])
    
    # 也合并 last_result 如果是字典
    if isinstance(ctx.get("last_result"), dict):
        configs_to_merge.append(ctx["last_result"])
    
    if not configs_to_merge:
        return {"status": "error", "message": "没有找到可合并的配置"}
    
    merged = loader.merge_configs(*configs_to_merge)
    
    # 解析变量
    resolved = loader.resolve_variables(merged)
    
    return {
        "status": "success",
        "merged": resolved,
        "sources": config_names
    }


@op("select_resource", category="config", description="弹出资源选择窗口")
def op_select_resource(ctx, params):
    """弹出资源选择窗口（文件或文件夹）
    
    参数:
        mode: 选择模式，可选 "folder"(文件夹), "file"(文件), "both"(两者都可)
        title: 对话框标题，默认 "选择资源"
        initial_dir: 初始目录，默认使用 base_dir
        file_types: 文件类型过滤，如 [["JSON", "*.json"], ["All", "*.*"]]
        multiple: 是否允许多选，默认 False
        save_to: 选择结果保存到配置的路径，如 "selected.path"
        
    返回:
        {
            "status": "success",
            "mode": "folder" | "file",
            "path": 选中的路径（单选）或 [路径列表]（多选）,
            "stored_to": 保存的配置路径（如果指定了 save_to）
        }
    """
    mode = params.get("mode", "folder")
    title = params.get("title", "选择资源")
    initial_dir = params.get("initial_dir") or ctx.get("base_dir", DATA_DIR)
    file_types = params.get("file_types", [["All", "*.*"]])
    multiple = params.get("multiple", False)
    save_to = params.get("save_to")
    
    # 确保初始目录存在
    if not os.path.exists(initial_dir):
        initial_dir = BASE_DIR
    
    # 创建隐藏的主窗口
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    selected = None
    actual_mode = None
    
    try:
        if mode == "folder":
            # 选择文件夹
            if multiple:
                # 多个文件夹选择（使用目录选择对话框多次）
                paths = []
                while True:
                    path = filedialog.askdirectory(
                        title=f"{title} ({len(paths)} 已选)",
                        initialdir=initial_dir if not paths else os.path.dirname(paths[-1])
                    )
                    if not path:
                        break
                    paths.append(path.replace('\\', '/'))
                    # 询问是否继续选择
                    from tkinter import messagebox
                    if not messagebox.askyesno("继续选择", "是否继续选择其他文件夹？"):
                        break
                selected = paths if paths else None
                actual_mode = "folder"
            else:
                # 单个文件夹
                path = filedialog.askdirectory(
                    title=title,
                    initialdir=initial_dir
                )
                if path:
                    selected = path.replace('\\', '/')
                    actual_mode = "folder"
                    
        elif mode == "file":
            # 选择文件
            if multiple:
                paths = filedialog.askopenfilenames(
                    title=title,
                    initialdir=initial_dir,
                    filetypes=file_types
                )
                if paths:
                    selected = [p.replace('\\', '/') for p in paths]
                    actual_mode = "file"
            else:
                path = filedialog.askopenfilename(
                    title=title,
                    initialdir=initial_dir,
                    filetypes=file_types
                )
                if path:
                    selected = path.replace('\\', '/')
                    actual_mode = "file"
                    
        elif mode == "both":
            # 通过对话框让用户选择类型
            from tkinter import messagebox
            choice = messagebox.askyesnocancel(
                title,
                "选择资源类型：\n\n是 = 选择文件\n否 = 选择文件夹\n取消 = 放弃选择"
            )
            if choice is None:
                selected = None
            elif choice:  # True = 文件
                path = filedialog.askopenfilename(
                    title=title,
                    initialdir=initial_dir,
                    filetypes=file_types
                )
                if path:
                    selected = path.replace('\\', '/')
                    actual_mode = "file"
            else:  # False = 文件夹
                path = filedialog.askdirectory(
                    title=title,
                    initialdir=initial_dir
                )
                if path:
                    selected = path.replace('\\', '/')
                    actual_mode = "folder"
    finally:
        root.destroy()
    
    # 用户取消选择
    if not selected:
        return {
            "status": "cancelled",
            "message": "用户取消了选择"
        }
    
    result = {
        "status": "success",
        "mode": actual_mode,
        "path": selected
    }
    
    # 如果指定了保存路径，存入配置
    if save_to:
        loader = _get_loader(ctx)
        config_name, _, config_path = save_to.partition('.')
        if not config_name:
            config_name = "runtime"
            config_path = save_to
        
        if config_name not in loader.external_configs:
            loader.external_configs[config_name] = {}
        
        # 设置嵌套值
        loader._set_nested_value(loader.external_configs[config_name], config_path, selected)
        result["stored_to"] = save_to
        
        # 同时存入 ctx，让后续步骤可以直接访问
        if config_name not in ctx:
            ctx[config_name] = {}
        if isinstance(ctx[config_name], dict):
            loader._set_nested_value(ctx[config_name], config_path, selected)
    
    return result


OP_MAP = {
    "load_external_config": op_load_external_config,
    "load_env_config": op_load_env_config,
    "resolve_config": op_resolve_config,
    "get_config_value": op_get_config_value,
    "set_config_value": op_set_config_value,
    "merge_configs": op_merge_configs,
    "select_resource": op_select_resource,
}


def run(config_path=None):
    PipelineEngine.main(OP_MAP, cfg=config_path, init_ctx=lambda: {"base_dir": DATA_DIR})


if __name__ == '__main__':
    run()
