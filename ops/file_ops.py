import os, re, shutil, platform, subprocess
from pathlib import Path
from core.pipeline_engine import PipelineEngine
import time
import json
from datetime import datetime

# ==========================================
# 辅助工具函数
# ==========================================

def _match(name, info, conditions):
    """匹配过滤条件的内部核心逻辑"""
    now = time.time()
    for cond in conditions:
        field = cond.get("field")
        operator = cond.get("operator")
        value = cond.get("value")
        
        field_value = {
            "name": name,
            "ext": info.get("extension", ""),
            "modify_time": info.get("modify_time", ""),
            "size": info.get("file_size", 0),
            "type": info.get("type", "")
        }.get(field)
        
        if field_value is None:
            continue
        
        matched = False
        
        # 正则匹配：直接用字符串形式
        if operator == "~":
            if re.search(str(value), str(field_value)):
                matched = True
        
        # 数值比较：需要转换为时间戳
        elif operator in (">", "<", ">=", "<=", "==", "="):
            # 如果是 modify_time 字段，需要转换为时间戳再比较
            if field == "modify_time":
                from datetime import datetime
                
                # 转换 field_value（字符串）为时间戳
                try:
                    field_ts = datetime.strptime(field_value, "%Y-%m-%d %H:%M:%S").timestamp()
                except:
                    continue
                
                # 转换 value 为时间戳
                if isinstance(value, str) and value.startswith("now"):
                    # 处理 "now+1000" 或 "now-3600" 格式
                    if "+" in value:
                        offset = int(value.split("+")[1])
                        value_ts = now + offset
                    elif "-" in value and value.index("-") > 0:
                        offset = int(value.split("-", 1)[1])
                        value_ts = now - offset
                    else:
                        value_ts = now
                else:
                    # 直接是时间戳数字
                    value_ts = float(value)
                
                # 比较时间戳
                if operator == ">" and field_ts > value_ts: matched = True
                elif operator == "<" and field_ts < value_ts: matched = True
                elif operator == ">=" and field_ts >= value_ts: matched = True
                elif operator == "<=" and field_ts <= value_ts: matched = True
                elif operator in ("==", "=") and field_ts == value_ts: matched = True
            else:
                # 其他字段的数值比较
                if operator == ">" and field_value > value: matched = True
                elif operator == "<" and field_value < value: matched = True
                elif operator == ">=" and field_value >= value: matched = True
                elif operator == "<=" and field_value <= value: matched = True
                elif operator in ("==", "=") and field_value == value: matched = True
        
        if not matched:
            return False
            
    return True

# ==========================================
# 管道操作函数 (Pipeline Steps)
# ==========================================

def scan_directory(ctx, params):
    folder_path = params.get("folder_path")
    items_data = {}
    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        stat = os.stat(item_path)
        mod_time = stat.st_mtime
        mod_time_str = datetime.fromtimestamp(mod_time).strftime("%Y-%m-%d %H:%M:%S")
        if os.path.isfile(item_path):
            items_data[item] = {"file_size": stat.st_size / 1024, "modify_time": mod_time_str, "extension": Path(item).suffix[1:], "type": "file"}
        else:
            items_data[item] = {"modify_time": mod_time_str, "type": "folder"}
            
    # 标准化返回结构：包含基准路径和文件列表字典
    result = {"base_path": folder_path, "items": items_data}
    ctx["last_result"] = result
    return result

def filter_files(ctx, params):
    data = ctx.get("last_result", {})
    base_path = data.get("base_path", "")
    items = data.get("items", {})
    
    conditions = params.get("conditions", [])
    filtered_items = {k: v for k, v in items.items() if _match(k, v, conditions)}
    
    result = {"base_path": base_path, "items": filtered_items}
    ctx["last_result"] = result
    return result

def batch_delete(ctx, params):
    data = ctx.get("last_result", {})
    base_path = data.get("base_path", "")
    items = data.get("items", {})

    results_info = {}
    for name, info in items.items():
        path = os.path.join(base_path, name)
        if os.path.exists(path):
            if info.get("type") == "folder": shutil.rmtree(path)
            else: os.remove(path)
            results_info[name] = "deleted"
            
    result = {"base_path": base_path, "items": results_info}
    ctx["last_result"] = result
    return result

def batch_rename(ctx, params):
    data = ctx.get("last_result", {})
    base_path = data.get("base_path", "")
    items = data.get("items", {})
    prefix = params.get("prefix", "file")
    
    new_items = {}
    for i, (old_name, info) in enumerate(items.items(), 1):
        ext = f".{info.get('extension')}" if info.get("extension") else ""
        new_name = f"{prefix}_{i}{ext}"
        
        old_path = os.path.join(base_path, old_name)
        new_path = os.path.join(base_path, new_name)
        
        counter = 1
        while os.path.exists(new_path):
            new_name = f"{prefix}_{i}_{counter}{ext}"
            new_path = os.path.join(base_path, new_name)
            counter += 1
            
        os.rename(old_path, new_path)
        # 更新新文件对应的 info (重命名后只变了名字，属性保留)
        new_items[new_name] = info
    
    result = {"base_path": base_path, "items": new_items}
    ctx["last_result"] = result
    return result

def limit_items(ctx, params):
    data = ctx.get("last_result", {})
    base_path = data.get("base_path", "")
    items = data.get("items", {})
    count = params.get("count")
    
    limited = dict(list(items.items())[:count])
    result = {"base_path": base_path, "items": limited}
    ctx["last_result"] = result
    return result

def batch_copy(ctx, params):
    data = ctx.get("last_result", {})
    base_path = data.get("base_path", "")
    items = data.get("items", {})
    dest_path = params.get("dest_path")
    
    os.makedirs(dest_path, exist_ok=True)
    copied_items = {}
    
    for name, info in items.items():
        src = os.path.join(base_path, name)
        dst = os.path.join(dest_path, name)
        if info.get("type") == "folder":
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dst)
        copied_items[name] = info # 写入新状态
        
    # 注意：复制完成后，上下文的 target 变成了 dest_path
    result = {"base_path": dest_path, "items": copied_items}
    ctx["last_result"] = result
    return result

def print_result(ctx, params):
    data = params.get("content") or ctx.get("last_result", {})
    print(">>> [Pipeline Step Result]:")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    return data

def open_program(ctx, params):
    data = ctx.get("last_result", {})
    
    # 允许显式指定路径，如果不指定，默认打开最后操作所在的文件夹(或者文件)
    path = params.get("path")
    if not path:
        path = data.get("base_path") if isinstance(data, dict) else str(data)

    if not path or not os.path.exists(path):
        return {"error": f"Path not found: {path}"}

    if platform.system() == "Windows": os.startfile(path)
    elif platform.system() == "Darwin": subprocess.Popen(["open", path])
    else: subprocess.Popen(["xdg-open", path])
    
    return path

# ==========================================
# 执行注册入口
# ==========================================

OP_MAP = {
    "scan_directory": scan_directory,
    "filter_files": filter_files,
    "batch_rename": batch_rename,
    "batch_copy": batch_copy,
    "limit_items": limit_items,
    "batch_delete": batch_delete,
    "print_result": print_result, 
    "open_program": open_program
}

def run(config_path=None):
    PipelineEngine(
        OP_MAP,
        init_ctx=lambda: {"last_result": None, "results": {}}, 
        done_fn=lambda ctx, lg: lg.info("执行完成")
    ).execute(config_path)

if __name__ == '__main__':
    run()