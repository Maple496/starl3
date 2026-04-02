import os
import re
import shutil
import platform
import subprocess
import json
import pandas as pd
import zipfile
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

from core.constants import DEFAULT_ENCODING, ENCODING_PRIORITY
from core.utils import ensure_dir_exists, resolve_path, safe_join
from core.registry import op
from core.logger import get_logger
from core.utils import detect_encoding, clean_newlines

logger = get_logger("file_ops")


# ==================== 数据导入操作 ====================

@op("import_excel", category="file", description="导入 Excel 文件为 DataFrame")
def op_import_excel(ctx, params) -> pd.DataFrame:
    """导入 Excel 文件"""
    file_path = resolve_path(params["file"], ctx)
    fp = file_path if os.path.isabs(file_path) else safe_join(ctx["base_dir"], file_path)
    
    sheet = params.get("sheet", 0)
    header = params.get("header_row", 1) - 1
    return pd.read_excel(fp, sheet_name=sheet, header=header)


@op("import_csv", category="file", description="导入 CSV 文件为 DataFrame")
def op_import_csv(ctx, params) -> pd.DataFrame:
    """导入 CSV 文件 - 优化编码检测"""
    file_path = resolve_path(params["file"], ctx)
    fp = file_path if os.path.isabs(file_path) else safe_join(ctx["base_dir"], file_path)
    
    delim = params.get("delimiter", ",")
    enc = params.get("encoding", "utf-8")
    
    if enc.strip().lower() == "auto":
        detected = detect_encoding(fp)
        encs = [detected] + [e for e in ENCODING_PRIORITY if e != detected]
    else:
        encs = [enc.strip()] + [e for e in ENCODING_PRIORITY if e != enc.strip().lower()]
    
    # 去重编码列表
    seen, unique_encs = set(), []
    for e in encs:
        if e.lower() not in seen:
            seen.add(e.lower())
            unique_encs.append(e)
    
    for encoding in unique_encs:
        try:
            return pd.read_csv(fp, encoding=encoding, delimiter=delim)
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    return pd.read_csv(fp, encoding="latin-1", delimiter=delim)


# ==================== 数据导出操作 ====================

@op("export_excel", category="file", description="导出 DataFrame 到 Excel")
def op_export_excel(ctx, params) -> pd.DataFrame:
    """导出 DataFrame 到 Excel"""
    file_path = resolve_path(params["file"], ctx)
    fp = file_path if os.path.isabs(file_path) else safe_join(ctx["base_dir"], file_path)
    
    ensure_dir_exists(fp)
    df = ctx["last_result"]
    df.to_excel(fp, sheet_name=params.get("sheet", "Sheet1"), index=False)
    return df


@op("export_csv", category="file", description="导出 DataFrame 到 CSV")
def op_export_csv(ctx, params) -> pd.DataFrame:
    """导出 DataFrame 到 CSV - 增强编码处理"""
    file_path = resolve_path(params["file"], ctx)
    fp = file_path if os.path.isabs(file_path) else safe_join(ctx["base_dir"], file_path)
    
    ensure_dir_exists(fp)
    df = ctx["last_result"]
    
    if params.get("clean_newlines", True):
        df = clean_newlines(df)
    
    encoding = params.get("encoding", DEFAULT_ENCODING)
    
    try:
        df.to_csv(fp, encoding=encoding, index=False, quoting=1)
    except UnicodeEncodeError as e:
        logger.warning(f"使用 {encoding} 编码失败，尝试使用 utf-8-sig: {e}")
        df.to_csv(fp, encoding='utf-8-sig', index=False, quoting=1)
        logger.info(f"已成功使用 utf-8-sig 编码写入: {os.path.basename(fp)}")
    
    return df


@op("export_split", category="file", description="按列分组导出多个 CSV")
def op_export_split(ctx, params) -> pd.DataFrame:
    """按列分组导出多个 CSV 文件"""
    import numpy as np
    
    df = ctx["last_result"]
    gc = params["group_col"]
    nc = params.get("name_col", gc)
    out_dir = resolve_path(params["out_dir"], ctx)
    out = out_dir if os.path.isabs(out_dir) else os.path.join(ctx["base_dir"], out_dir)
    
    os.makedirs(out, exist_ok=True)
    
    if params.get("clean_newlines", True):
        df = clean_newlines(df)
    
    encoding = params.get("encoding", DEFAULT_ENCODING)
    
    for name, group in df.groupby(gc):
        # 获取文件名，处理 NaN 情况
        name_val = group[nc].iloc[0]
        if pd.isna(name_val):
            # 如果用户名为空，使用 group_col 的值作为文件名
            filename = f"{name}.csv"
        else:
            filename = f"{name_val}.csv"
        
        filepath = os.path.join(out, filename)
        
        try:
            group.to_csv(filepath, index=False, encoding=encoding, quoting=1)
            logger.info(f"已生成拆分文件: {filename} ({len(group)} 行)")
        except UnicodeEncodeError:
            group.to_csv(filepath, index=False, encoding='utf-8-sig', quoting=1)
            logger.info(f"已生成拆分文件 (utf-8-sig): {filename} ({len(group)} 行)")
    
    return df


# ==================== 文件扫描和过滤 ====================

def _match(n: str, i: Dict, conditions: List[Dict]) -> bool:
    """匹配文件条件"""
    for c in conditions:
        field, op, value = c.get("field"), c.get("operator"), c.get("value")
        
        field_value = {
            "name": n,
            "ext": i.get("extension", ""),
            "modify_time": i.get("modify_time", ""),
            "size": i.get("file_size", 0),
            "type": i.get("type", "")
        }.get(field)
        
        if field_value is None:
            continue
        
        if op == "~":
            if not re.search(str(value), str(field_value)):
                return False
            continue
        
        if field == "modify_time":
            try:
                import time
                field_value = datetime.strptime(field_value, "%Y-%m-%d %H:%M:%S").timestamp()
                value_str = str(value)
                if "+" in value_str:
                    value = time.time() + int(value_str.split("+")[1])
                elif "-" in value_str:
                    value = time.time() - int(value_str.split("-")[1])
                else:
                    value = float(value)
            except (ValueError, TypeError):
                return False
        
        try:
            op_map = {
                "=": lambda a, b: a == b, "==": lambda a, b: a == b,
                "!=": lambda a, b: a != b, "<": lambda a, b: a < b,
                "<=": lambda a, b: a <= b, ">": lambda a, b: a > b,
                ">=": lambda a, b: a >= b,
            }
            
            if isinstance(field_value, str):
                result = op_map.get(op, lambda a, b: False)(field_value, str(value))
            else:
                try:
                    value_num = float(value)
                except (ValueError, TypeError):
                    value_num = value
                result = op_map.get(op, lambda a, b: False)(field_value, value_num)
            
            if not result:
                return False
                
        except Exception:
            return False
    
    return True


def build_item(path: str) -> Dict[str, Any]:
    """构建文件/目录信息字典"""
    try:
        st = os.stat(path)
        mt = datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        
        if os.path.isfile(path):
            return {
                "file_size": st.st_size / 1024,
                "modify_time": mt,
                "extension": Path(path).suffix[1:],
                "type": "file"
            }
        return {"modify_time": mt, "type": "folder"}
        
    except FileNotFoundError:
        raise FileNotFoundError(f"路径不存在: {path}")
    except PermissionError:
        raise PermissionError(f"无权限访问: {path}")


@op("scan_directory", category="file", description="扫描目录")
def scan_directory(ctx, p):
    """扫描目录"""
    folder_path = p.get("folder_path")
    if not folder_path:
        raise ValueError("folder_path 参数不能为空")
    
    folder_path = resolve_path(folder_path, ctx)
    
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"目录不存在: {folder_path}")
    if not os.path.isdir(folder_path):
        raise NotADirectoryError(f"不是目录: {folder_path}")
    
    try:
        items = {
            i: build_item(os.path.join(folder_path, i))
            for i in os.listdir(folder_path)
        }
        return {"base_path": folder_path, "items": items}
    except PermissionError as e:
        raise PermissionError(f"无法访问目录 {folder_path}: {e}")


@op("filter_files", category="file", description="过滤文件")
def filter_files(ctx, p):
    """过滤文件"""
    last_result = ctx.get("last_result", {})
    base_path = last_result.get("base_path") or ctx.get("base_dir", "")
    items = last_result.get("items", {})
    conditions = p.get("conditions", [])
    
    if not isinstance(conditions, list):
        raise ValueError("conditions 必须是列表")
    
    try:
        filtered = {k: v for k, v in items.items() if _match(k, v, conditions)}
        return {"base_path": base_path, "items": filtered}
    except Exception as e:
        raise RuntimeError(f"过滤文件时出错: {e}")


# ==================== 原有的文件操作 ====================

def refresh_items(base_path, items):
    """刷新项目信息"""
    return {name: build_item(os.path.join(base_path, name)) 
            for name, info in items.items() 
            if os.path.exists(os.path.join(base_path, name))}


def batch_delete(ctx: dict, p: dict) -> dict:
    """批量删除文件/目录"""
    last_result = ctx.get("last_result", {})
    base_path = last_result.get("base_path", "")
    items = last_result.get("items", {})
    
    deleted = {}
    errors = []
    
    for name, info in items.items():
        path = os.path.join(base_path, name)
        try:
            if info.get("type") == "folder":
                shutil.rmtree(path)
            else:
                os.remove(path)
            deleted[name] = "deleted"
        except FileNotFoundError:
            deleted[name] = "not_found"
        except PermissionError as e:
            errors.append(f"无权限删除 {name}: {e}")
            deleted[name] = "permission_denied"
        except Exception as e:
            errors.append(f"删除 {name} 失败: {e}")
            deleted[name] = "error"
    
    result = {"base_path": base_path, "items": deleted}
    if errors:
        result["errors"] = errors
    return result


def batch_rename(ctx: dict, p: dict) -> dict:
    """批量重命名文件"""
    last_result = ctx.get("last_result", {})
    base_path = last_result.get("base_path", "")
    items = last_result.get("items", {})
    prefix = p.get("prefix", "file")
    
    renamed = {}
    errors = []
    
    for idx, (old_name, info) in enumerate(items.items(), 1):
        ext = f".{info.get('extension')}" if info.get("extension") else ""
        
        counter = 0
        while True:
            suffix = f"_{counter}" if counter > 0 else ""
            new_name = f"{prefix}_{idx}{suffix}{ext}"
            new_path = os.path.join(base_path, new_name)
            
            if not os.path.exists(new_path):
                break
            counter += 1
        
        old_path = os.path.join(base_path, old_name)
        
        try:
            os.rename(old_path, new_path)
            renamed[new_name] = info
        except FileNotFoundError:
            errors.append(f"源文件不存在: {old_name}")
        except PermissionError as e:
            errors.append(f"无权限重命名 {old_name}: {e}")
        except Exception as e:
            errors.append(f"重命名 {old_name} 失败: {e}")
    
    result = {"base_path": base_path, "items": renamed}
    if errors:
        result["errors"] = errors
    return result


def batch_copy(ctx: dict, p: dict) -> dict:
    """批量复制文件/目录"""
    last_result = ctx.get("last_result", {})
    base_path = last_result.get("base_path", "")
    items = last_result.get("items", {})
    dest_path = p.get("dest_path")
    
    if not dest_path:
        raise ValueError("dest_path 参数不能为空")
    
    os.makedirs(dest_path, exist_ok=True)
    
    copied = {}
    errors = []
    
    for name, info in items.items():
        src = os.path.join(base_path, name)
        dst = os.path.join(dest_path, name)
        
        try:
            if info.get("type") == "folder":
                shutil.copytree(src, dst, dirs_exist_ok=True)
            else:
                shutil.copy2(src, dst)
            copied[name] = info
        except FileNotFoundError:
            errors.append(f"源文件不存在: {name}")
        except PermissionError as e:
            errors.append(f"无权限复制 {name}: {e}")
        except Exception as e:
            errors.append(f"复制 {name} 失败: {e}")
    
    result = {"base_path": dest_path, "items": refresh_items(dest_path, copied)}
    if errors:
        result["errors"] = errors
    return result


@op("print_result", category="file", description="打印结果")
def print_result(ctx, p):
    d = p.get("content") or ctx.get("last_result", {})
    print(f">>> [Pipeline Step Result]:\n{json.dumps(d, indent=2, ensure_ascii=False)}")
    return d


def open_program(ctx, p):
    d = ctx.get("last_result", {})
    path = p.get("path") or (d.get("base_path") if isinstance(d,dict) else str(d))
    if os.path.exists(path):
        opener = getattr(os, 'startfile', None)
        if opener:
            opener(path)
        else:
            subprocess.Popen(["open" if platform.system() == "Darwin" else "xdg-open", path])
    return path



