from core.pipeline_engine import PipelineEngine
import os
import re
import shutil
from pathlib import Path
from datetime import datetime

def scan_directory(ctx, params):
    folder_path = params.get("folder_path")
    result = {folder_path: {}}
    
    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        stat = os.stat(item_path)
        mod_time = datetime.fromtimestamp(stat.st_mtime).strftime("%Y%m%d%H%M%S")
        
        if os.path.isfile(item_path):
            size_kb = stat.st_size / 1024
            ext = Path(item).suffix[1:] if Path(item).suffix else ""
            result[folder_path][item] = {
                "file_size": f"{size_kb:.2f}kb",
                "modify_time": mod_time,
                "extension": ext
            }
        else:
            file_count = len(os.listdir(item_path))
            result[folder_path][item] = {
                "folder_size": f"{stat.st_size / 1024:.2f}kb",
                "modify_time": mod_time,
                "file_count": file_count
            }
    
    ctx["file_list"] = result
    return result

def filter_files(ctx, params):
    file_list = ctx.get("file_list", {})
    conditions = params.get("conditions", {})
    
    folder_path = list(file_list.keys())[0]
    items = file_list[folder_path]
    filtered = {k: v for k, v in items.items() if _match_conditions(k, v, conditions)}
    
    ctx["filtered_list"] = filtered
    return filtered

def _match_conditions(name, info, conditions):
    for key, value in conditions.items():
        if key == "date":
            if not _compare_value(info.get("modify_time", ""), value):
                return False
        elif key == "size":
            size_num = float(info.get("file_size", "0kb").replace("kb", ""))
            if not _compare_value(size_num, value):
                return False
        elif key == "name":
            if not re.search(value, name):
                return False
    return True

def _compare_value(actual, condition):
    if isinstance(condition, dict):
        for op, val in condition.items():
            if op == ">" and actual > val: return True
            elif op == ">=" and actual >= val: return True
            elif op == "<" and actual < val: return True
            elif op == "<=" and actual <= val: return True
            elif op == "=" and actual == val: return True
    return False

def sort_files(ctx, params):
    file_list = ctx.get("filtered_list") or ctx.get("file_list", {})
    sort_by = params.get("sort_by", "name")
    reverse = params.get("reverse", False)
    
    folder_path = list(file_list.keys())[0]
    items = file_list[folder_path]
    
    if sort_by == "name":
        sorted_items = sorted(items.items(), key=lambda x: x[0], reverse=reverse)
    elif sort_by == "size":
        sorted_items = sorted(items.items(), key=lambda x: float(x[1].get("file_size", "0kb").replace("kb", "")), reverse=reverse)
    elif sort_by == "date":
        sorted_items = sorted(items.items(), key=lambda x: x[1].get("modify_time", ""), reverse=reverse)
    else:
        sorted_items = list(items.items())
    
    ctx["sorted_list"] = {k: v for k, v in sorted_items}
    return ctx["sorted_list"]

def batch_rename(ctx, params):
    file_list = ctx.get("sorted_list") or ctx.get("filtered_list") or ctx.get("file_list", {})
    folder_path = params.get("folder_path")
    prefix = params.get("prefix", "file")
    
    folder_path = folder_path or list(file_list.keys())[0]
    items = file_list[folder_path]
    
    results = {}
    for idx, (name, info) in enumerate(items.items(), 1):
        old_path = os.path.join(folder_path, name)
        ext = info.get("extension", "")
        new_name = f"{prefix}_{idx}.{ext}" if ext else f"{prefix}_{idx}"
        new_path = os.path.join(folder_path, new_name)
        os.rename(old_path, new_path)
        results[name] = new_name
    
    ctx["rename_results"] = results
    return results

def batch_delete(ctx, params):
    file_list = ctx.get("sorted_list") or ctx.get("filtered_list") or ctx.get("file_list", {})
    folder_path = params.get("folder_path") or list(file_list.keys())[0]
    items = file_list[folder_path]
    
    deleted = []
    for name in items.keys():
        path = os.path.join(folder_path, name)
        if os.path.isfile(path):
            os.remove(path)
        else:
            shutil.rmtree(path)
        deleted.append(name)
    
    ctx["delete_results"] = deleted
    return deleted

def batch_move(ctx, params):
    file_list = ctx.get("sorted_list") or ctx.get("filtered_list") or ctx.get("file_list", {})
    folder_path = params.get("folder_path") or list(file_list.keys())[0]
    dest_path = params.get("dest_path")
    items = file_list[folder_path]
    
    os.makedirs(dest_path, exist_ok=True)
    moved = {}
    for name in items.keys():
        src = os.path.join(folder_path, name)
        dst = os.path.join(dest_path, name)
        shutil.move(src, dst)
        moved[name] = dst
    
    ctx["move_results"] = moved
    return moved

def batch_copy(ctx, params):
    file_list = ctx.get("sorted_list") or ctx.get("filtered_list") or ctx.get("file_list", {})
    folder_path = params.get("folder_path") or list(file_list.keys())[0]
    dest_path = params.get("dest_path")
    items = file_list[folder_path]
    
    os.makedirs(dest_path, exist_ok=True)
    copied = {}
    for name in items.keys():
        src = os.path.join(folder_path, name)
        dst = os.path.join(dest_path, name)
        if os.path.isfile(src):
            shutil.copy2(src, dst)
        else:
            shutil.copytree(src, dst, dirs_exist_ok=True)
        copied[name] = dst
    
    ctx["copy_results"] = copied
    return copied

OP_MAP = {
    "scan_directory": scan_directory,
    "filter_files": filter_files,
    "sort_files": sort_files,
    "batch_rename": batch_rename,
    "batch_delete": batch_delete,
    "batch_move": batch_move,
    "batch_copy": batch_copy,
}

def run(config_path=None):
    PipelineEngine(
        OP_MAP,
        init_ctx=lambda: {},
        done_fn=lambda ctx, lg: lg.info("执行完成")
    ).execute(config_path)

if __name__ == '__main__':
    run()