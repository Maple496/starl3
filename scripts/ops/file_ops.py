import os
import re
import shutil
import platform
import subprocess
import json
import chardet
import pandas as pd
import zipfile
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

from core.pipeline_engine import PipelineEngine
from core.safe_eval import SafeEvaluator, SafeEvalError
from core.constants import BASE_DIR, DATA_DIR, DEFAULT_ENCODING, ENCODING_PRIORITY
from core.path_utils import ensure_dir_exists, _resolve_path, safe_join
from core.registry import op
from core.logger import get_logger
from ops.plug_lib.csvtohtml1 import csv_to_html
from ops.plug_lib.csvtohtml2 import csv_to_html as csv_to_html2

logger = get_logger("file_ops")


def _detect_encoding(file_path: str, sample_size: int = 100000) -> str:
    """使用 chardet 预检测文件编码"""
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(sample_size)
            result = chardet.detect(raw_data)
            encoding = result.get('encoding', 'utf-8')
            confidence = result.get('confidence', 0)
            
            if confidence > 0.7:
                encoding_map = {
                    'GB2312': 'gb18030',
                    'GBK': 'gb18030',
                    'UTF-8-SIG': 'utf-8-sig',
                    'UTF-16-LE': 'utf-16',
                    'UTF-16-BE': 'utf-16',
                }
                return encoding_map.get(encoding.upper(), encoding)
    except Exception:
        pass
    return 'utf-8'


def _clean_newlines(df: pd.DataFrame) -> pd.DataFrame:
    """清理字符串列中的换行符"""
    str_cols = df.select_dtypes(include=["object"]).columns
    if len(str_cols) == 0:
        return df
    
    replacements = {
        col: df[col].str.replace(r'[\r\n]+', ' ', regex=True) 
        for col in str_cols
    }
    return df.assign(**replacements)


# ==================== 数据导入操作 ====================

@op("import_excel", category="file", description="导入 Excel 文件为 DataFrame")
def op_import_excel(ctx, params) -> pd.DataFrame:
    """导入 Excel 文件"""
    file_path = _resolve_path(params["file"], ctx)
    
    # 如果路径已经是绝对路径，直接使用；否则使用 safe_join
    if os.path.isabs(file_path):
        fp = file_path
    else:
        fp = safe_join(ctx["base_dir"], file_path)
    
    sheet = params.get("sheet", 0)
    header = params.get("header_row", 1) - 1
    return pd.read_excel(fp, sheet_name=sheet, header=header)


@op("import_csv", category="file", description="导入 CSV 文件为 DataFrame")
def op_import_csv(ctx, params) -> pd.DataFrame:
    """导入 CSV 文件 - 优化编码检测"""
    file_path = _resolve_path(params["file"], ctx)
    
    # 如果路径已经是绝对路径，直接使用；否则使用 safe_join
    if os.path.isabs(file_path):
        fp = file_path
    else:
        fp = safe_join(ctx["base_dir"], file_path)
    
    delim = params.get("delimiter", ",")
    enc = params.get("encoding", "utf-8")
    
    if enc.strip().lower() == "auto":
        detected = _detect_encoding(fp)
        encs = [detected] + [e for e in ENCODING_PRIORITY if e != detected]
    else:
        encs = [enc.strip()] + [e for e in ENCODING_PRIORITY if e != enc.strip().lower()]
    
    seen = set()
    unique_encs = []
    for e in encs:
        e_lower = e.lower()
        if e_lower not in seen:
            seen.add(e_lower)
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
    file_path = _resolve_path(params["file"], ctx)
    
    # 如果路径已经是绝对路径，直接使用；否则使用 safe_join
    if os.path.isabs(file_path):
        fp = file_path
    else:
        fp = safe_join(ctx["base_dir"], file_path)
    
    ensure_dir_exists(fp)
    
    df = ctx["last_result"]
    sheet = params.get("sheet", "Sheet1")
    df.to_excel(fp, sheet_name=sheet, index=False)
    return df


@op("export_csv", category="file", description="导出 DataFrame 到 CSV")
def op_export_csv(ctx, params) -> pd.DataFrame:
    """导出 DataFrame 到 CSV - 增强编码处理"""
    file_path = _resolve_path(params["file"], ctx)
    
    # 如果路径已经是绝对路径，直接使用；否则使用 safe_join
    if os.path.isabs(file_path):
        fp = file_path
    else:
        fp = safe_join(ctx["base_dir"], file_path)
    
    ensure_dir_exists(fp)
    
    df = ctx["last_result"]
    
    if params.get("clean_newlines", True):
        df = _clean_newlines(df)
    
    # 获取编码参数
    encoding = params.get("encoding", DEFAULT_ENCODING)
    
    # 尝试写入，如果失败则使用更宽松的编码策略
    try:
        df.to_csv(fp, encoding=encoding, index=False, quoting=1)
    except UnicodeEncodeError as e:
        logger.warning(f"使用 {encoding} 编码失败，尝试使用 utf-8-sig: {e}")
        try:
            df.to_csv(fp, encoding='utf-8-sig', index=False, quoting=1)
            logger.info(f"已成功使用 utf-8-sig 编码写入: {os.path.basename(fp)}")
        except Exception as e2:
            logger.error(f"写入 CSV 失败: {e2}")
            raise
    
    return df


@op("export_split", category="file", description="按列分组导出多个 CSV")
def op_export_split(ctx, params) -> pd.DataFrame:
    """按列分组导出多个 CSV 文件"""
    df = ctx["last_result"]
    gc = params["group_col"]
    nc = params.get("name_col", gc)
    out_dir = _resolve_path(params["out_dir"], ctx)
    
    # 如果路径已经是绝对路径，直接使用；否则拼接基础目录
    if os.path.isabs(out_dir):
        out = out_dir
    else:
        out = os.path.join(ctx["base_dir"], out_dir)
    
    os.makedirs(out, exist_ok=True)
    
    if params.get("clean_newlines", True):
        df = _clean_newlines(df)
    
    encoding = params.get("encoding", DEFAULT_ENCODING)
    
    failed_files = []
    for name, group in df.groupby(gc):
        filename = f"{group[nc].iloc[0]}.csv"
        filepath = os.path.join(out, filename)
        
        try:
            group.to_csv(filepath, index=False, encoding=encoding, quoting=1)
        except UnicodeEncodeError as e:
            logger.warning(f"使用 {encoding} 编码写入 {filename} 失败，尝试使用 utf-8-sig")
            try:
                group.to_csv(filepath, index=False, encoding='utf-8-sig', quoting=1)
                logger.info(f"已成功使用 utf-8-sig 编码写入: {filename}")
            except Exception as e2:
                logger.error(f"写入 {filename} 失败: {e2}")
                failed_files.append((filename, str(e2)))
    
    if failed_files:
        logger.warning(f"以下文件写入失败: {failed_files}")
    
    return df


# ==================== 批量合并操作 ====================

@op("merge_excel", category="file", description="合并多个 Excel 文件")
def op_merge_excel(ctx, params) -> pd.DataFrame:
    """合并多个 Excel 文件，自动去重标题"""
    # 获取文件列表
    source_step = params.get("source_step")
    if source_step:
        file_result = ctx.get("results", {}).get(source_step)
    else:
        file_result = ctx.get("last_result")
    
    if not file_result or not isinstance(file_result, dict):
        raise ValueError("没有可用的文件列表，请先执行 scan_directory 和 filter_files")
    
    items = file_result.get("items", {})
    base_path = file_result.get("base_path", "")
    
    # 筛选 Excel 文件
    excel_files = []
    for name, info in items.items():
        if info.get("type") == "file" and name.lower().endswith((".xlsx", ".xls")):
            excel_files.append(os.path.join(base_path, name))
    
    if not excel_files:
        logger.warning("没有找到 Excel 文件")
        return pd.DataFrame()
    
    logger.info(f"找到 {len(excel_files)} 个 Excel 文件待合并")
    
    # 读取并合并
    sheet = params.get("sheet", 0)
    header = params.get("header_row", 1) - 1
    
    all_data = []
    failed_files = []
    empty_files = []
    
    for fp in excel_files:
        try:
            # 首先检查文件是否损坏（尝试打开）
            if fp.lower().endswith('.xlsx'):
                try:
                    with zipfile.ZipFile(fp, 'r') as z:
                        z.testzip()
                except zipfile.BadZipFile:
                    logger.error(f"文件损坏或格式错误: {os.path.basename(fp)}")
                    failed_files.append((fp, "文件损坏或不是有效的Excel文件"))
                    continue
            
            df = pd.read_excel(fp, sheet_name=sheet, header=header)
            
            # 检查是否为空（无数据行）
            if len(df) == 0:
                logger.warning(f"文件为空（无数据行）: {os.path.basename(fp)}")
                empty_files.append(fp)
                continue
            
            # 检查是否缺少关键列
            required_cols = params.get("required_columns", [])
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.warning(f"文件缺少关键列 {missing_cols}: {os.path.basename(fp)}")
                # 继续处理，不跳过
            
            df["_source_file"] = os.path.basename(fp)  # 添加来源标记
            all_data.append(df)
            logger.info(f"已读取: {os.path.basename(fp)} ({len(df)} 行, {len(df.columns)} 列)")
            
        except Exception as e:
            logger.error(f"读取文件失败 {os.path.basename(fp)}: {e}")
            failed_files.append((fp, str(e)))
    
    # 统计结果
    if failed_files:
        logger.warning(f"读取失败文件: {len(failed_files)} 个")
    if empty_files:
        logger.warning(f"空文件: {len(empty_files)} 个")
    
    if not all_data:
        if failed_files and empty_files:
            raise ValueError(f"没有成功读取任何 Excel 文件。失败: {len(failed_files)} 个, 空文件: {len(empty_files)} 个")
        elif failed_files:
            raise ValueError(f"所有 Excel 文件读取失败，共 {len(failed_files)} 个")
        else:
            raise ValueError("所有 Excel 文件都为空（无数据行）")
    
    # 合并数据
    logger.info(f"开始合并 {len(all_data)} 个文件的数据...")
    merged = pd.concat(all_data, ignore_index=True)
    logger.info(f"合并完成: 共 {len(merged)} 行, {len(merged.columns)} 列")
    
    # 去重（如果指定了列）
    dedup_cols = params.get("dedup_columns")
    if dedup_cols and all(col in merged.columns for col in dedup_cols):
        before_count = len(merged)
        merged = merged.drop_duplicates(subset=dedup_cols)
        dropped = before_count - len(merged)
        if dropped > 0:
            logger.info(f"去重完成: 移除 {dropped} 行重复数据")
    
    # 移除辅助列
    if "_source_file" not in params.get("keep_columns", []):
        merged = merged.drop(columns=["_source_file"])
    
    logger.info(f"合并完成: 共 {len(merged)} 行, {len(merged.columns)} 列")
    return merged


# ==================== 原有的文件操作 ====================

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
            if op == "=":
                op = "=="
            
            if isinstance(field_value, str):
                if op == "==":
                    result = field_value == str(value)
                elif op == "!=":
                    result = field_value != str(value)
                elif op == "<":
                    result = field_value < str(value)
                elif op == "<=":
                    result = field_value <= str(value)
                elif op == ">":
                    result = field_value > str(value)
                elif op == ">=":
                    result = field_value >= str(value)
                else:
                    return False
            else:
                try:
                    value_num = float(value)
                except (ValueError, TypeError):
                    value_num = value
                    
                if op == "==":
                    result = field_value == value_num
                elif op == "!=":
                    result = field_value != value_num
                elif op == "<":
                    result = field_value < value_num
                elif op == "<=":
                    result = field_value <= value_num
                elif op == ">":
                    result = field_value > value_num
                elif op == ">=":
                    result = field_value >= value_num
                else:
                    return False
            
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

def refresh_items(base_path, items):
    new_items = {}
    for name, info in items.items():
        p = os.path.join(base_path, name)
        if os.path.exists(p):
            new_items[name] = build_item(p)
    return new_items

@op("scan_directory", category="file", description="扫描目录")
def scan_directory(ctx, p):
    """扫描目录"""
    folder_path = p.get("folder_path")
    if not folder_path:
        raise ValueError("folder_path 参数不能为空")
    
    folder_path = _resolve_path(folder_path, ctx)
    
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
        filtered = {
            k: v for k, v in items.items()
            if _match(k, v, conditions)
        }
        return {"base_path": base_path, "items": filtered}
    except Exception as e:
        raise RuntimeError(f"过滤文件时出错: {e}")

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
    
    if errors:
        return {
            "base_path": base_path,
            "items": deleted,
            "errors": errors
        }
    
    return {"base_path": base_path, "items": deleted}

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

def limit_items(ctx: dict, p: dict) -> dict:
    """限制返回的项目数量"""
    d = ctx.get("last_result", {})
    count = p.get("count")
    items = dict(list(d.get("items", {}).items())[:count]) if count else d.get("items", {})
    return {"base_path": d.get("base_path"), "items": items}

def batch_copy(ctx: dict, p: dict) -> dict:
    """批量复制文件/目录"""
    last_result = ctx.get("last_result", {})
    base_path = last_result.get("base_path", "")
    items = last_result.get("items", {})
    dest_path = p.get("dest_path")
    
    if not dest_path:
        raise ValueError("dest_path 参数不能为空")
    
    try:
        ensure_dir_exists(os.path.join(dest_path, ".placeholder"))
    except Exception as e:
        raise RuntimeError(f"无法创建目标目录 {dest_path}: {e}")
    
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
    
    try:
        new_items = refresh_items(dest_path, copied)
    except Exception:
        new_items = copied
    
    result = {"base_path": dest_path, "items": new_items}
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

def plug_in(ctx, p):
    d = ctx.get("last_result", {})
    base_path, items = d.get("base_path", ""), d.get("items", {})
    out_dir = Path(base_path) / "output"
    out_dir.mkdir(exist_ok=True)
    typ = p.get("type", "csvtohtml")
    if typ == "csvtohtml":
        html_items = {}
        for name, info in items.items():
            if info.get("type") == "file" and name.lower().endswith(".csv"):
                src, dst = Path(base_path)/name, out_dir/(Path(name).stem+".html")
                csv_to_html(src, dst)
                html_items[dst.name] = {"type":"file", "path": str(dst)}
        ctx["last_result"] = {"base_path": str(out_dir), "items": html_items}
        return ctx["last_result"]
    elif typ == "csvtohtml2":
        html_items = {}
        for name, info in items.items():
            if info.get("type") == "file" and name.lower().endswith(".csv"):
                src, dst = Path(base_path)/name, out_dir/(Path(name).stem+".html")
                csv_to_html2(src, dst)
                html_items[dst.name] = {"type":"file", "path": str(dst)}
        ctx["last_result"] = {"base_path": str(out_dir), "items": html_items}
        return ctx["last_result"]
    return d


OP_MAP = {
    "scan_directory": scan_directory,
    "filter_files": filter_files,
    "batch_delete": batch_delete,
    "batch_rename": batch_rename,
    "limit_items": limit_items,
    "batch_copy": batch_copy,
    "print_result": print_result,
    "open_program": open_program,
    "plug_in": plug_in,
}

def run(config_path=None):
    PipelineEngine.main(OP_MAP, cfg=config_path, init_ctx=lambda: {"base_dir": DATA_DIR})

if __name__ == '__main__':
    run()
