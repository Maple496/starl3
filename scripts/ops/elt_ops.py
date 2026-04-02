"""
ELT (Extract, Load, Transform) 操作模块
数据读取、转换、输出操作
"""

import os
import zipfile
import pandas as pd
import numpy as np
from typing import Any
from tkinter import Tk, filedialog

from core.constants import DATA_DIR, DEFAULT_ENCODING
from core.pipeline_engine import UserCancelledError
from core.logger import get_logger
from core.registry import op
from core.safe_eval import eval_dataframe_expression, SafeEvalError
from core.utils import resolve_path
from app.config_store import get_config_manager
from core.utils import detect_encoding, clean_newlines

logger = get_logger("elt_ops")

# pandas 优化设置
pd.set_option('mode.copy_on_write', True)


# ==================== 数据导入（使用 file_ops 的别名） ====================

@op("read_excel", category="elt", description="读取 Excel 文件")
def op_read_excel(ctx, params) -> pd.DataFrame:
    """读取 Excel 文件"""
    from ops.file_ops import op_import_excel
    return op_import_excel(ctx, params)


@op("read_csv", category="elt", description="读取 CSV 文件")
def op_read_csv(ctx, params) -> pd.DataFrame:
    """读取 CSV 文件"""
    from ops.file_ops import op_import_csv
    return op_import_csv(ctx, params)


# ==================== 数据转换操作 ====================

@op("filter", category="elt", description="过滤数据")
def op_filter(ctx, params) -> pd.DataFrame:
    """过滤数据"""
    df = ctx["last_result"]
    col, op, val = params["column"], params["op"], params.get("value")
    
    ops = {
        ">": df[df[col] > val], ">=": df[df[col] >= val],
        "<": df[df[col] < val], "<=": df[df[col] <= val],
        "==": df[df[col] == val], "!=": df[df[col] != val],
        "notna": df[df[col].notna()], "isna": df[df[col].isna()],
        "isempty": df[df[col].isna() | (df[col].astype(str).str.strip() == "")],
        "notempty": df[df[col].notna() & (df[col].astype(str).str.strip() != "")]
    }
    return ops[op]


@op("sort", category="elt", description="排序数据")
def op_sort(ctx, params) -> pd.DataFrame:
    """排序数据"""
    df = ctx["last_result"]
    return df.sort_values(by=params["column"], ascending=params.get("ascending", True))


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
    new_col, formula = params["new_col"], params["formula"]
    
    if not new_col or not formula:
        raise ValueError("new_col 和 formula 不能为空")
    
    try:
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
    return df.groupby(params["by"]).agg(params["agg"]).reset_index()


@op("join", category="elt", description="合并数据")
def op_join(ctx, params) -> pd.DataFrame:
    """合并数据"""
    df = ctx["last_result"].copy()
    source_key = params.get("source")
    
    if not source_key or source_key not in ctx.get("results", {}):
        raise ValueError(f"源步骤 '{source_key}' 不存在")
    
    source = ctx["results"][source_key].copy()
    on = params.get("on")
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
    src, tgt = params["source_col"], params["target_col"]
    
    new_tgt = df[src].where(df[src].notna(), df[tgt])
    result = df.assign(**{tgt: new_tgt})
    
    if params.get("drop", True):
        result = result.drop(columns=[src])
    return result


@op("split_write", category="elt", description="分组写入")
def op_split_write(ctx, params) -> pd.DataFrame:
    """分组写入 - 调用 file_ops.export_split"""
    from ops.file_ops import op_export_split
    return op_export_split(ctx, params)


@op("fuzzy_override", category="elt", description="模糊匹配覆盖")
def op_fuzzy_override(ctx, params) -> pd.DataFrame:
    """模糊匹配覆盖"""
    df = ctx["last_result"]
    rules = ctx["results"][params["source"]]
    
    mc, tc, cc = params["match_col"], params["text_col"], params["contains_col"]
    sv, tv = params["source_val_col"], params["target_val_col"]
    
    result = df
    for _, rule in rules.iterrows():
        match_val, contain_val, source_val = rule[mc], str(rule[cc]), rule[sv]
        mask = (result[mc] == match_val) & result[tc].astype(str).str.contains(contain_val, na=False, regex=False)
        result.loc[mask, tv] = source_val
    
    return result


# ==================== 数据导出（使用 file_ops 的别名） ====================

@op("write_excel", category="elt", description="写入 Excel")
def op_write_excel(ctx, params) -> pd.DataFrame:
    """写入 Excel"""
    from ops.file_ops import op_export_excel
    return op_export_excel(ctx, params)


@op("write_csv", category="elt", description="写入 CSV")
def op_write_csv(ctx, params) -> pd.DataFrame:
    """写入 CSV"""
    from ops.file_ops import op_export_csv
    return op_export_csv(ctx, params)


# ==================== 统计分析操作 ====================

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


# ==================== 交互式操作 ====================

@op("select_resource", category="elt", description="弹出资源选择窗口")
def op_select_resource(ctx, params):
    """弹出资源选择窗口（文件或文件夹），支持动态配置缓存"""
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
        env_map = {
            "paths.source_file": "MOCK_SOURCE_FILE",
            "paths.mapping_file": "MOCK_MAPPING_FILE",
            "paths.output_dir": "MOCK_OUTPUT_DIR",
        }
        if save_to in env_map:
            selected = os.environ.get(env_map[save_to])
    
    # 2. 检查动态配置缓存
    if selected is None and config_name and use_cache and not force_refresh:
        config_mgr = get_config_manager()
        cached_value = config_mgr.get_config_value(config_name)
        
        if cached_value:
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
    return ctx.get("last_result") if "last_result" in ctx else None


@op("clear_dynamic_config", category="elt", description="清除动态配置缓存")
def op_clear_dynamic_config(ctx, params):
    """清除动态配置缓存"""
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
    """列出所有动态配置"""
    config_mgr = get_config_manager()
    configs = config_mgr.list_configs()
    
    if not configs:
        return pd.DataFrame(columns=['config_name', 'note', 'updated_at'])
    
    return pd.DataFrame(configs)


@op("merge_excel_files", category="elt", description="合并多个 Excel 文件")
def op_merge_excel_files(ctx, params) -> pd.DataFrame:
    """合并多个 Excel 文件，自动去重标题"""
    source_step = params.get("source_step")
    file_result = ctx.get("results", {}).get(source_step) if source_step else ctx.get("last_result")
    
    if not file_result or not isinstance(file_result, dict):
        raise ValueError("没有可用的文件列表")
    
    items = file_result.get("items", {})
    base_path = file_result.get("base_path", "")
    
    excel_files = [
        os.path.join(base_path, name)
        for name, info in items.items()
        if info.get("type") == "file" and name.lower().endswith((".xlsx", ".xls"))
    ]
    
    if not excel_files:
        logger.warning("没有找到 Excel 文件")
        return pd.DataFrame()
    
    logger.info(f"找到 {len(excel_files)} 个 Excel 文件待合并")
    
    sheet = params.get("sheet", 0)
    header = params.get("header_row", 1) - 1
    
    all_data, failed_files, empty_files = [], [], []
    
    for fp in excel_files:
        try:
            if fp.lower().endswith('.xlsx'):
                try:
                    with zipfile.ZipFile(fp, 'r') as z:
                        z.testzip()
                except zipfile.BadZipFile:
                    logger.error(f"文件损坏或格式错误: {os.path.basename(fp)}")
                    failed_files.append((fp, "文件损坏或不是有效的Excel文件"))
                    continue
            
            df = pd.read_excel(fp, sheet_name=sheet, header=header)
            
            if len(df) == 0:
                logger.warning(f"文件为空: {os.path.basename(fp)}")
                empty_files.append(fp)
                continue
            
            df["_source_file"] = os.path.basename(fp)
            all_data.append(df)
            logger.info(f"已读取: {os.path.basename(fp)} ({len(df)} 行)")
            
        except Exception as e:
            import traceback
            error_detail = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            logger.error(f"读取文件失败 {os.path.basename(fp)}: {error_detail}")
            failed_files.append((fp, str(e)))
    
    if not all_data:
        raise ValueError(f"没有成功读取任何 Excel 文件。失败: {len(failed_files)} 个, 空文件: {len(empty_files)} 个")
    
    logger.info(f"开始合并 {len(all_data)} 个文件的数据...")
    merged = pd.concat(all_data, ignore_index=True)
    logger.info(f"合并完成: 共 {len(merged)} 行, {len(merged.columns)} 列")
    
    dedup_cols = params.get("dedup_columns")
    if dedup_cols and all(col in merged.columns for col in dedup_cols):
        before_count = len(merged)
        merged = merged.drop_duplicates(subset=dedup_cols)
        dropped = before_count - len(merged)
        if dropped > 0:
            logger.info(f"去重完成: 移除 {dropped} 行重复数据")
    
    if "_source_file" not in params.get("keep_columns", []):
        merged = merged.drop(columns=["_source_file"])
    
    return merged


@op("merge_csv_by_manager", category="elt", description="按科长合并下属用户的CSV文件")
def op_merge_csv_by_manager(ctx, params) -> pd.DataFrame:
    """按科长合并下属用户的CSV文件
    
    参数:
        source_dir: 源目录（包含用户CSV文件的文件夹，如 PO 或 PR）
        mapping_file: 映射文件路径（Excel文件）
        mapping_sheet: 映射sheet名称（如"用户"）
        user_col: 用户列名（如"用户"）
        manager_col: 科长列名（如"科长"）
        output_dir: 输出目录（如果不指定则使用 source_dir）
        encoding: 文件编码（默认 gbk）
    """
    source_dir = resolve_path(params.get("source_dir", ""), ctx)
    mapping_file = resolve_path(params.get("mapping_file", ""), ctx)
    mapping_sheet = params.get("mapping_sheet", "用户")
    user_col = params.get("user_col", "用户")
    manager_col = params.get("manager_col", "科长")
    output_dir = resolve_path(params.get("output_dir", source_dir), ctx)
    encoding = params.get("encoding", "gbk")
    
    # 1. 读取映射文件，建立用户->科长的映射
    logger.info(f"读取映射文件: {mapping_file}, sheet: {mapping_sheet}")
    try:
        mapping_df = pd.read_excel(mapping_file, sheet_name=mapping_sheet)
    except Exception as e:
        raise ValueError(f"读取映射文件失败: {e}")
    
    # 检查必要的列是否存在
    if user_col not in mapping_df.columns or manager_col not in mapping_df.columns:
        available_cols = list(mapping_df.columns)
        raise ValueError(f"映射文件缺少必要列。需要: '{user_col}', '{manager_col}'，可用列: {available_cols}")
    
    # 建立用户->科长的映射（只包含有科长的记录）
    user_to_manager = {}
    for _, row in mapping_df.iterrows():
        user = str(row[user_col]).strip() if pd.notna(row[user_col]) else ""
        manager = str(row[manager_col]).strip() if pd.notna(row[manager_col]) else ""
        # 只处理有效的用户-科长关系（科长不为空且不等于"/"）
        if user and manager and manager != "/":
            user_to_manager[user] = manager
    
    logger.info(f"建立 {len(user_to_manager)} 个用户-科长映射关系")
    
    # 2. 扫描源目录中的CSV文件
    if not os.path.exists(source_dir):
        raise FileNotFoundError(f"源目录不存在: {source_dir}")
    
    csv_files = [
        f for f in os.listdir(source_dir)
        if f.lower().endswith('.csv') and os.path.isfile(os.path.join(source_dir, f))
    ]
    
    if not csv_files:
        logger.warning(f"源目录中没有CSV文件: {source_dir}")
        return pd.DataFrame()
    
    logger.info(f"找到 {len(csv_files)} 个CSV文件")
    
    # 3. 按科长分组，收集每个科长的下属用户文件
    # manager_files: {科长: [用户文件名列表]}
    manager_files = {}
    for csv_file in csv_files:
        # 去掉.csv后缀获取用户名
        user_name = csv_file[:-4]  # 移除 ".csv"
        if user_name in user_to_manager:
            manager = user_to_manager[user_name]
            if manager not in manager_files:
                manager_files[manager] = []
            manager_files[manager].append(csv_file)
        else:
            # 用户没有对应的科长，跳过
            logger.debug(f"用户 '{user_name}' 没有对应的科长映射，跳过")
    
    if not manager_files:
        logger.warning("没有找到可合并的文件（没有用户匹配到科长）")
        return pd.DataFrame()
    
    logger.info(f"找到 {len(manager_files)} 个科长需要合并文件")
    
    # 4. 为每个科长合并文件
    all_merged_data = []
    os.makedirs(output_dir, exist_ok=True)
    
    for manager, files in manager_files.items():
        logger.info(f"合并科长 '{manager}' 的文件: {files}")
        
        manager_data = []
        for file_name in files:
            file_path = os.path.join(source_dir, file_name)
            try:
                # 尝试不同编码读取
                for enc in [encoding, 'utf-8', 'utf-8-sig', 'gb18030']:
                    try:
                        df = pd.read_csv(file_path, encoding=enc)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    df = pd.read_csv(file_path, encoding='latin-1')
                
                # 添加来源标记
                df["_source_user"] = file_name[:-4]  # 去掉.csv
                manager_data.append(df)
                logger.info(f"  已读取: {file_name} ({len(df)} 行)")
                
            except Exception as e:
                logger.error(f"  读取文件失败 {file_name}: {e}")
        
        if manager_data:
            # 合并该科长的所有数据
            merged_df = pd.concat(manager_data, ignore_index=True)
            
            # 保存到文件
            output_file = os.path.join(output_dir, f"{manager}.csv")
            try:
                merged_df.to_csv(output_file, encoding=encoding, index=False)
                logger.info(f"  已保存: {output_file} ({len(merged_df)} 行)")
            except UnicodeEncodeError:
                merged_df.to_csv(output_file, encoding='utf-8-sig', index=False)
                logger.info(f"  已保存(utf-8-sig): {output_file} ({len(merged_df)} 行)")
            
            # 添加来源标记后收集
            merged_df["_manager"] = manager
            all_merged_data.append(merged_df)
    
    if all_merged_data:
        result = pd.concat(all_merged_data, ignore_index=True)
        logger.info(f"科长汇总完成: 共 {len(manager_files)} 个科长, {len(result)} 行数据")
        return result
    else:
        logger.warning("没有成功合并任何数据")
        return pd.DataFrame()



