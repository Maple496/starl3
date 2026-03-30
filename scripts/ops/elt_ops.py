"""
ELT (Extract, Load, Transform) 操作模块
数据读取、转换、输出操作
"""

import os
import chardet
import pandas as pd
import numpy as np
from typing import Any
from tkinter import Tk, filedialog

from core.constants import BASE_DIR, DATA_DIR, DEFAULT_ENCODING, ENCODING_PRIORITY
from core.pipeline_engine import PipelineEngine, UserCancelledError
from core.logger import get_logger
from core.registry import op, OpRegistry
from core.safe_eval import eval_dataframe_expression, SafeEvalError
from core.path_utils import safe_join, ensure_dir_exists, _resolve_path
from core.dynamic_config import get_config_manager

logger = get_logger("elt_ops")

# pandas 优化设置
pd.set_option('mode.copy_on_write', True)


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

@op("read_excel", category="elt", description="读取 Excel 文件（兼容别名，实际调用 file_ops.import_excel）")
def op_read_excel(ctx, params) -> pd.DataFrame:
    """读取 Excel 文件 - 兼容别名，调用 file_ops.import_excel"""
    from ops.file_ops import op_import_excel
    return op_import_excel(ctx, params)


@op("read_csv", category="elt", description="读取 CSV 文件（兼容别名，实际调用 file_ops.import_csv）")
def op_read_csv(ctx, params) -> pd.DataFrame:
    """读取 CSV 文件 - 兼容别名，调用 file_ops.import_csv"""
    from ops.file_ops import op_import_csv
    return op_import_csv(ctx, params)


@op("filter", category="elt", description="过滤数据")
def op_filter(ctx, params) -> pd.DataFrame:
    """过滤数据"""
    df = ctx["last_result"]
    col = params["column"]
    op = params["op"]
    val = params.get("value")
    
    ops = {
        ">": df[df[col] > val],
        ">=": df[df[col] >= val],
        "<": df[df[col] < val],
        "<=": df[df[col] <= val],
        "==": df[df[col] == val],
        "!=": df[df[col] != val],
        "notna": df[df[col].notna()],
        "isna": df[df[col].isna()],
        "isempty": df[df[col].isna() | (df[col].astype(str).str.strip() == "")],
        "notempty": df[df[col].notna() & (df[col].astype(str).str.strip() != "")]
    }
    return ops[op]


@op("sort", category="elt", description="排序数据")
def op_sort(ctx, params) -> pd.DataFrame:
    """排序数据"""
    df = ctx["last_result"]
    col = params["column"]
    ascending = params.get("ascending", True)
    return df.sort_values(by=col, ascending=ascending)


@op("rename", category="elt", description="重命名列")
def op_rename(ctx, params) -> pd.DataFrame:
    """重命名列"""
    return ctx["last_result"].rename(columns=params["map"])


@op("drop", category="elt", description="删除列")
def op_drop(ctx, params) -> pd.DataFrame:
    """删除列"""
    return ctx["last_result"].drop(columns=params["columns"])


@op("select", category="elt", description="选择列")
def op_select(ctx, params) -> pd.DataFrame:
    """选择列"""
    return ctx["last_result"][params["columns"]]


@op("fill_null", category="elt", description="填充空值")
def op_fill_null(ctx, params) -> pd.DataFrame:
    """填充空值"""
    df = ctx["last_result"]
    col = params["column"]
    return df.assign(**{col: df[col].fillna(params["value"])})


@op("calc", category="elt", description="计算新列")
def op_calc(ctx, params) -> pd.DataFrame:
    """计算新列 - 使用安全表达式评估"""
    df = ctx["last_result"]
    new_col = params["new_col"]
    formula = params["formula"]
    
    if not new_col:
        raise ValueError("new_col 不能为空")
    if not formula:
        raise ValueError("formula 不能为空")
    
    try:
        # 使用安全的 DataFrame 表达式评估
        result = eval_dataframe_expression(df, formula)
        logger.info(f"成功计算列 '{new_col}'")
        return df.assign(**{new_col: result})
        
    except SafeEvalError as e:
        logger.error(f"计算列 '{new_col}' 失败", extra={"formula": formula, "error": str(e)})
        raise ValueError(f"公式 '{formula}' 评估失败: {e}")


@op("group", category="elt", description="分组聚合")
def op_group(ctx, params) -> pd.DataFrame:
    """分组聚合"""
    df = ctx["last_result"]
    by = params["by"]
    agg = params["agg"]
    return df.groupby(by).agg(agg).reset_index()


@op("join", category="elt", description="合并数据")
def op_join(ctx, params) -> pd.DataFrame:
    """合并数据"""
    df = ctx["last_result"].copy()
    source_key = params.get("source")
    if not source_key:
        raise ValueError("join 操作需要 'source' 参数指定源步骤")
    if source_key not in ctx.get("results", {}):
        raise ValueError(f"源步骤 '{source_key}' 不存在，请检查步骤ID")
    source = ctx["results"][source_key].copy()
    on = params.get("on")
    if not on:
        raise ValueError("join 操作需要 'on' 参数指定连接列")
    how = params.get("how", "left")
    
    # 统一 join 键的数据类型（转为字符串避免类型不匹配）
    join_cols = on if isinstance(on, list) else [on]
    for col in join_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)
        if col in source.columns:
            source[col] = source[col].astype(str)
    
    return df.merge(source, on=on, how=how)


@op("pivot", category="elt", description="透视表")
def op_pivot(ctx, params) -> pd.DataFrame:
    """透视表"""
    df = ctx["last_result"]
    return df.pivot_table(
        index=params["index"],
        columns=params["columns"],
        values=params["values"],
        aggfunc=params.get("agg", "sum")
    ).reset_index()


@op("unpivot", category="elt", description="逆透视")
def op_unpivot(ctx, params) -> pd.DataFrame:
    """逆透视"""
    df = ctx["last_result"]
    return df.melt(
        id_vars=params["id_cols"],
        var_name=params.get("var_col", "variable"),
        value_name=params.get("value_col", "value")
    )


@op("overlay", category="elt", description="列覆盖")
def op_overlay(ctx, params) -> pd.DataFrame:
    """列覆盖"""
    df = ctx["last_result"]
    src = params["source_col"]
    tgt = params["target_col"]
    
    new_tgt = df[src].where(df[src].notna(), df[tgt])
    result = df.assign(**{tgt: new_tgt})
    
    if params.get("drop", True):
        result = result.drop(columns=[src])
    return result


@op("split_write", category="elt", description="分组写入（兼容别名，实际调用 file_ops.export_split）")
def op_split_write(ctx, params) -> pd.DataFrame:
    """分组写入 - 兼容别名，调用 file_ops.export_split"""
    from ops.file_ops import op_export_split
    return op_export_split(ctx, params)


@op("fuzzy_override", category="elt", description="模糊匹配覆盖")
def op_fuzzy_override(ctx, params) -> pd.DataFrame:
    """模糊匹配覆盖 - 优化: 避免不必要的拷贝"""
    df = ctx["last_result"]
    rules = ctx["results"][params["source"]]
    
    mc = params["match_col"]
    tc = params["text_col"]
    cc = params["contains_col"]
    sv = params["source_val_col"]
    tv = params["target_val_col"]
    
    # 优化：使用 assign 创建新列，避免完整的 DataFrame 拷贝
    # 只有当确实需要修改时才创建拷贝
    result = df
    
    for _, rule in rules.iterrows():
        match_val = rule[mc]
        contain_val = str(rule[cc])
        source_val = rule[sv]
        
        mask = (result[mc] == match_val) & result[tc].astype(str).str.contains(contain_val, na=False, regex=False)
        result.loc[mask, tv] = source_val
    
    return result


@op("write_excel", category="elt", description="写入 Excel（兼容别名，实际调用 file_ops.export_excel）")
def op_write_excel(ctx, params) -> pd.DataFrame:
    """写入 Excel - 兼容别名，调用 file_ops.export_excel"""
    from ops.file_ops import op_export_excel
    return op_export_excel(ctx, params)


@op("write_csv", category="elt", description="写入 CSV（兼容别名，实际调用 file_ops.export_csv）")
def op_write_csv(ctx, params) -> pd.DataFrame:
    """写入 CSV - 兼容别名，调用 file_ops.export_csv"""
    from ops.file_ops import op_export_csv
    return op_export_csv(ctx, params)


@op("rank", category="elt", description="计算排名")
def op_rank(ctx, params) -> pd.DataFrame:
    """计算排名"""
    df = ctx["last_result"]
    col = params["column"]
    nc = params.get("new_col", f"{col}_rank")
    asc = params.get("ascending", True)
    method = params.get("method", "min")
    gb = params.get("group_by")
    
    if gb:
        rank_values = df.groupby(gb)[col].rank(method=method, ascending=asc)
    else:
        rank_values = df[col].rank(method=method, ascending=asc)
    
    result = df.assign(**{nc: rank_values})
    
    if params.get("top_n"):
        result = result[result[nc] <= params["top_n"]]
    
    return result


@op("value_counts", category="elt", description="值计数")
def op_value_counts(ctx, params) -> pd.DataFrame:
    """值计数"""
    df = ctx["last_result"]
    col = params["column"]
    
    vc = df[col].value_counts(
        normalize=params.get("normalize", False),
        dropna=params.get("dropna", False)
    ).reset_index()
    
    vc.columns = [col, params.get("count_col", "count")]
    
    if params.get("sort_ascending") is not None:
        vc = vc.sort_values(col, ascending=params["sort_ascending"])
    
    return vc


@op("bin", category="elt", description="分箱")
def op_bin(ctx, params) -> pd.DataFrame:
    """分箱"""
    df = ctx["last_result"]
    col = params["column"]
    nc = params.get("new_col", f"{col}_bin")
    
    if "bins" in params:
        bin_values = pd.cut(df[col], bins=params["bins"], labels=params.get("labels"), include_lowest=True)
    elif "q" in params:
        bin_values = pd.qcut(df[col], q=params["q"], duplicates="drop", labels=params.get("labels"))
    else:
        raise ValueError("必须指定 bins 或 q 参数")
    
    return df.assign(**{nc: bin_values})


@op("describe", category="elt", description="描述统计")
def op_describe(ctx, params) -> pd.DataFrame:
    """描述统计"""
    df = ctx["last_result"]
    
    if params.get("columns"):
        sub = df[params["columns"]]
    else:
        sub = df.select_dtypes(include="number")
    
    desc = sub.describe(percentiles=params.get("percentiles", [.25, .5, .75])).T.reset_index()
    desc.rename(columns={"index": "column"}, inplace=True)
    return desc


@op("cumulative", category="elt", description="累积计算")
def op_cumulative(ctx, params) -> pd.DataFrame:
    """累积计算"""
    df = ctx["last_result"]
    col = params["column"]
    nc = params.get("new_col", f"{col}_cum")
    gb = params.get("group_by")
    
    if params.get("sort_desc", True):
        df = df.sort_values(col, ascending=False)
    
    if gb:
        cum_values = df.groupby(gb)[col].cumsum()
        pct_values = df.groupby(gb)[nc].transform(lambda x: x / x.max())
    else:
        cum_values = df[col].cumsum()
        pct_values = cum_values / cum_values.iloc[-1]
    
    return df.assign(**{nc: cum_values, f"{nc}_pct": pct_values})


@op("head_tail", category="elt", description="头尾截取")
def op_head_tail(ctx, params) -> pd.DataFrame:
    """头尾截取"""
    df = ctx["last_result"]
    n = params.get("n", 10)
    mode = params.get("mode", "head")
    
    if mode == "head":
        return df.head(n)
    elif mode == "tail":
        return df.tail(n)
    else:
        return pd.concat([df.head(n), df.tail(n)]).drop_duplicates()


@op("select_resource", category="elt", description="弹出资源选择窗口")
def op_select_resource(ctx, params):
    """弹出资源选择窗口（文件或文件夹），支持动态配置缓存
    
    参数:
        mode: 选择模式，可选 "folder"(文件夹), "file"(文件)
        title: 对话框标题
        initial_dir: 初始目录
        file_types: 文件类型过滤，如 [["CSV", "*.csv"], ["All", "*.*"]]
        save_to: 选择结果保存到 ctx 的路径，如 "paths.source_file"
        config_name: 配置名称（用于缓存）
        config_note: 配置备注说明
        use_cache: 是否使用缓存，默认 true
        force_refresh: 强制刷新缓存，重新弹窗
        validate_exists: 验证文件/目录是否存在，不存在则重新弹窗，默认 true
        
    返回:
        返回当前数据流（不破坏数据流），路径保存在 ctx 中
    """
    mode = params.get("mode", "folder")
    title = params.get("title", "选择资源")
    initial_dir = params.get("initial_dir") or ctx.get("base_dir", DATA_DIR)
    file_types = params.get("file_types", [["All", "*.*"]])
    save_to = params.get("save_to")
    config_name = params.get("config_name")
    config_note = params.get("config_note", "")
    use_cache = params.get("use_cache", True)
    force_refresh = params.get("force_refresh", False)
    validate_exists = params.get("validate_exists", True)
    
    selected = None
    from_cache = False
    
    # 1. 检查环境变量（用于测试模式，最高优先级）
    if save_to:
        if save_to == "paths.source_file":
            selected = os.environ.get("MOCK_SOURCE_FILE")
        elif save_to == "paths.mapping_file":
            selected = os.environ.get("MOCK_MAPPING_FILE")
        elif save_to == "paths.output_dir":
            selected = os.environ.get("MOCK_OUTPUT_DIR")
    
    # 2. 检查动态配置缓存
    if selected is None and config_name and use_cache and not force_refresh:
        config_mgr = get_config_manager()
        cached_value = config_mgr.get_config_value(config_name)
        
        if cached_value:
            # 验证缓存值是否有效
            is_valid = True
            if validate_exists:
                if mode == "file" and not os.path.isfile(cached_value):
                    is_valid = False
                elif mode == "folder" and not os.path.isdir(cached_value):
                    is_valid = False
            
            if is_valid:
                selected = cached_value
                from_cache = True
                logger.info(f"使用缓存配置 [{config_name}]: {selected}")
            else:
                logger.info(f"缓存配置 [{config_name}] 已失效，需要重新选择")
    
    # 3. 如果没有获取到值，弹出GUI对话框
    if selected is None:
        if not os.path.exists(initial_dir):
            initial_dir = DATA_DIR
        
        root = Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        
        try:
            if mode == "folder":
                path = filedialog.askdirectory(title=title, initialdir=initial_dir)
                if path:
                    selected = path.replace('\\', '/')
            elif mode == "file":
                path = filedialog.askopenfilename(title=title, initialdir=initial_dir, filetypes=file_types)
                if path:
                    selected = path.replace('\\', '/')
        finally:
            root.destroy()
    
    # 4. 如果用户取消了选择，抛出异常终止 pipeline
    if selected is None:
        raise UserCancelledError(f"用户取消了选择: {title}")
    
    # 5. 保存到动态配置缓存
    if config_name and not from_cache:
        config_mgr = get_config_manager()
        config_mgr.save_config(config_name, selected, config_note)
        logger.info(f"保存配置 [{config_name}]: {selected}")
    
    # 6. 保存到 ctx
    if save_to:
        parts = save_to.split('.')
        current = ctx
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = selected
    
    # 保持数据流不变，返回当前结果
    return ctx.get("last_result")


@op("input_dialog", category="elt", description="弹出文本输入对话框")
def op_input_dialog(ctx, params):
    """弹出文本输入对话框，支持动态配置缓存
    
    参数:
        title: 对话框标题
        prompt: 输入提示文本
        default: 默认值
        save_to: 输入结果保存到 ctx 的路径
        config_name: 配置名称（用于缓存）
        config_note: 配置备注说明
        use_cache: 是否使用缓存，默认 true
        force_refresh: 强制刷新缓存，重新弹窗
        
    返回:
        返回当前数据流（不破坏数据流），输入值保存在 ctx 中
    """
    title = params.get("title", "输入")
    prompt = params.get("prompt", "请输入:")
    default = params.get("default", "")
    save_to = params.get("save_to")
    config_name = params.get("config_name")
    config_note = params.get("config_note", "")
    use_cache = params.get("use_cache", True)
    force_refresh = params.get("force_refresh", False)
    
    from tkinter import simpledialog
    
    value = None
    from_cache = False
    
    # 1. 检查环境变量（用于测试模式）
    if config_name:
        env_value = os.environ.get(f"MOCK_INPUT_{config_name.upper()}")
        if env_value:
            value = env_value
    
    # 2. 检查动态配置缓存
    if value is None and config_name and use_cache and not force_refresh:
        config_mgr = get_config_manager()
        cached_value = config_mgr.get_config_value(config_name)
        
        if cached_value is not None:
            value = cached_value
            from_cache = True
            logger.info(f"使用缓存配置 [{config_name}]: {value}")
    
    # 3. 如果没有获取到值，弹出输入对话框
    if value is None:
        root = Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        
        try:
            value = simpledialog.askstring(title, prompt, initialvalue=default)
        finally:
            root.destroy()
    
    # 4. 如果用户取消了输入，抛出异常终止 pipeline
    if value is None:
        raise UserCancelledError(f"用户取消了输入: {title}")
    
    # 5. 保存到动态配置缓存
    if config_name and not from_cache:
        config_mgr = get_config_manager()
        config_mgr.save_config(config_name, value, config_note)
        logger.info(f"保存配置 [{config_name}]: {value}")
    
    # 6. 保存到 ctx
    if save_to:
        parts = save_to.split('.')
        current = ctx
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value
    
    # 保持数据流不变，返回当前结果
    return ctx.get("last_result")


@op("clear_dynamic_config", category="elt", description="清除动态配置缓存")
def op_clear_dynamic_config(ctx, params):
    """清除动态配置缓存
    
    参数:
        config_name: 要清除的配置名称，如果不指定则清除所有配置
        
    返回:
        返回当前数据流
    """
    config_name = params.get("config_name")
    
    config_mgr = get_config_manager()
    
    if config_name:
        config_mgr.delete_config(config_name)
        logger.info(f"已清除配置: {config_name}")
    else:
        config_mgr.clear_all_configs()
        logger.info("已清除所有动态配置")
    
    return ctx.get("last_result")


@op("list_dynamic_configs", category="elt", description="列出所有动态配置")
def op_list_dynamic_configs(ctx, params):
    """列出所有动态配置
    
    参数:
        无
        
    返回:
        包含配置列表的 DataFrame
    """
    config_mgr = get_config_manager()
    configs = config_mgr.list_configs()
    
    if not configs:
        # 返回空 DataFrame
        return pd.DataFrame(columns=['config_name', 'note', 'updated_at'])
    
    return pd.DataFrame(configs)


@op("merge_excel_files", category="elt", description="合并多个 Excel 文件")
def op_merge_excel_files(ctx, params) -> pd.DataFrame:
    """合并多个 Excel 文件，自动去重标题
    
    参数:
        source_step: 文件扫描结果的步骤ID（默认使用 last_result）
        sheet: 工作表名称或索引，默认 0
        header_row: 表头行号，默认 1
        dedup_columns: 去重依据的列名列表，可选
        
    返回:
        合并后的 DataFrame
    """
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
            import zipfile
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


def run(config_path=None):
    """模块测试入口"""
    PipelineEngine.main(OpRegistry.get_op_map(), cfg=config_path, init_ctx=lambda: {"base_dir": DATA_DIR})


if __name__ == '__main__':
    run()
