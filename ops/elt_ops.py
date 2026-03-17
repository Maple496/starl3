#elt_ops.py

import os
import pandas as pd, numpy as np
from core.pipeline_engine import PipelineEngine
from main_starl3 import BASE_DIR

def op_read_excel(ctx, params):
    return pd.read_excel(os.path.join(ctx["base_dir"], params["file"]), sheet_name=params.get("sheet", 0), header=params.get("header_row", 1) - 1)

def op_read_csv(ctx, params):
    fp = os.path.join(ctx["base_dir"], params["file"])
    delim = params.get("delimiter", ",")
    enc = params.get("encoding", "utf-8")
    encs = [] if enc.strip().lower() == "auto" else [enc.strip()]
    for e in ["utf-8-sig", "gb18030", "utf-16", "latin-1"]:
        if e not in encs: encs.append(e)
    for e in encs:
        try:
            return pd.read_csv(fp, encoding=e, delimiter=delim)
        except (UnicodeDecodeError, UnicodeError):
            pass
    return pd.read_csv(fp, encoding="latin-1", delimiter=delim)

def op_filter(ctx, params):
    df, col, op, val = ctx["df"], params["column"], params["op"], params["value"]
    return {">": lambda: df[df[col]>val], ">=": lambda: df[df[col]>=val], "<": lambda: df[df[col]<val], "<=": lambda: df[df[col]<=val], "==": lambda: df[df[col]==val], "!=": lambda: df[df[col]!=val]}[op]()

def op_sort(ctx, params):
    return ctx["df"].sort_values(by=params["column"], ascending=params.get("ascending", True))

def op_rename(ctx, params):  return ctx["df"].rename(columns=params["map"])
def op_drop(ctx, params):    return ctx["df"].drop(columns=params["columns"])
def op_select(ctx, params):  return ctx["df"][params["columns"]]

def op_fill_null(ctx, params):
    df = ctx["df"]; df[params["column"]] = df[params["column"]].fillna(params["value"]); return df

_T2 = ("pd.", "np.", ".str.", ".dt.", ".astype(", ".apply(", ".map(", ".fillna(", ".shift(", ".cumsum(", ".rank(", "lambda ")

def _eval_t2(df, f):
    lv = {c: df[c] for c in df.columns}
    lv.update({"df": df, "pd": pd, "np": np, "str": str, "int": int, "float": float, "len": len})
    return eval(f, {"__builtins__": __builtins__}, lv)

def op_calc(ctx, params):
    df, nc, f = ctx["df"].copy(), params["new_col"], params["formula"]
    if any(m in f for m in _T2): df[nc] = _eval_t2(df, f)
    else:
        try: df[nc] = df.eval(f)
        except Exception: df[nc] = _eval_t2(df, f)
    ctx["df"] = df; return df

def op_group(ctx, params):   return ctx["df"].groupby(params["by"]).agg(params["agg"]).reset_index()
def op_join(ctx, params):    return ctx["df"].merge(ctx["results"][params["source"]], on=params["on"], how=params.get("how", "left"))
def op_pivot(ctx, params):   return ctx["df"].pivot_table(index=params["index"], columns=params["columns"], values=params["values"], aggfunc=params.get("agg", "sum")).reset_index()
def op_unpivot(ctx, params): return ctx["df"].melt(id_vars=params["id_cols"], var_name=params.get("var_col", "variable"), value_name=params.get("value_col", "value"))

def op_overlay(ctx, params):
    df, src, tgt = ctx["df"], params["source_col"], params["target_col"]
    df[tgt] = df[src].where(df[src].notna(), df[tgt])
    return df.drop(columns=[src]) if params.get("drop", True) else df

def op_split_write(ctx, params):
    df, gc, nc = ctx["df"], params["group_col"], params.get("name_col", params["group_col"])
    out = os.path.join(ctx["base_dir"], params["out_dir"]); os.makedirs(out, exist_ok=True)
    for _, g in df.groupby(nc):
        g.to_csv(os.path.join(out, str(g[nc].iloc[0]) + ".csv"), index=False, encoding=params.get("encoding", "utf-8-sig"))
    return df

def op_fuzzy_override(ctx, params):
    df, rules = ctx["df"].copy(), ctx["results"][params["source"]]
    mc, tc, cc, sv, tv = params["match_col"], params["text_col"], params["contains_col"], params["source_val_col"], params["target_val_col"]
    for _, r in rules.iterrows():
        mask = (df[mc] == r[mc]) & df[tc].astype(str).str.contains(str(r[cc]), na=False)
        df.loc[mask, tv] = r[sv]
    return df

def op_write_excel(ctx, params):
    fp = os.path.join(ctx["base_dir"], params["file"]); os.makedirs(os.path.dirname(fp), exist_ok=True)
    ctx["df"].to_excel(fp, sheet_name=params.get("sheet", "Sheet1"), index=False); return ctx["df"]

def op_write_csv(ctx, params):
    fp = os.path.join(ctx["base_dir"], params["file"])
    os.makedirs(os.path.dirname(fp), exist_ok=True)
    df = ctx["df"].copy()
    if params.get("clean_newlines", True):
        str_cols = df.select_dtypes(include=["object", "str"]).columns
        df[str_cols] = df[str_cols].apply(lambda col: col.str.replace(r'[\r\n]+', ' ', regex=True) if col.dtype == object else col)
    df.to_csv(fp, encoding=params.get("encoding", "utf-8-sig"), index=False, quoting=__import__('csv').QUOTE_NONNUMERIC)
    return df


def op_rank(ctx, params):
    df, col, nc = ctx["df"].copy(), params["column"], params.get("new_col", params["column"] + "_rank")
    asc, method, gb = params.get("ascending", True), params.get("method", "min"), params.get("group_by")
    df[nc] = df.groupby(gb)[col].rank(method=method, ascending=asc) if gb else df[col].rank(method=method, ascending=asc)
    return df[df[nc] <= params["top_n"]] if params.get("top_n") else df

def op_value_counts(ctx, params):
    vc = ctx["df"][params["column"]].value_counts(normalize=params.get("normalize", False), dropna=params.get("dropna", False)).reset_index()
    vc.columns = [params["column"], params.get("count_col", "count")]
    return vc.sort_values(params["column"], ascending=params["sort_ascending"]) if params.get("sort_ascending") is not None else vc

def op_bin(ctx, params):
    df, col, nc = ctx["df"].copy(), params["column"], params.get("new_col", params["column"] + "_bin")
    if "bins" in params: df[nc] = pd.cut(df[col], bins=params["bins"], labels=params.get("labels"), include_lowest=True)
    elif "q" in params:  df[nc] = pd.qcut(df[col], q=params["q"], duplicates="drop", labels=params.get("labels"))
    return df

def op_describe(ctx, params):
    sub = ctx["df"][params["columns"]] if params.get("columns") else ctx["df"].select_dtypes(include="number")
    desc = sub.describe(percentiles=params.get("percentiles", [.25,.5,.75])).T.reset_index()
    desc.rename(columns={"index": "column"}, inplace=True); return desc

def op_cumulative(ctx, params):
    df, col, nc, gb = ctx["df"].copy(), params["column"], params.get("new_col", params["column"]+"_cum"), params.get("group_by")
    if params.get("sort_desc", True): df = df.sort_values(col, ascending=False)
    if gb:
        df[nc] = df.groupby(gb)[col].cumsum(); df[nc+"_pct"] = df.groupby(gb)[nc].transform(lambda x: x/x.max())
    else:
        df[nc] = df[col].cumsum(); df[nc+"_pct"] = df[nc] / df[nc].iloc[-1]
    return df

def op_head_tail(ctx, params):
    df, n, mode = ctx["df"], params.get("n", 10), params.get("mode", "head")
    if mode == "head": return df.head(n)
    if mode == "tail": return df.tail(n)
    return pd.concat([df.head(n), df.tail(n)]).drop_duplicates()

OP_MAP = {
    "read_excel": op_read_excel, "read_csv": op_read_csv, "filter": op_filter,
    "sort": op_sort, "rename": op_rename, "select": op_select, "drop": op_drop,
    "fill_null": op_fill_null, "calc": op_calc, "group": op_group, "join": op_join,
    "pivot": op_pivot, "unpivot": op_unpivot, "write_excel": op_write_excel,
    "write_csv": op_write_csv, "overlay": op_overlay,
    "fuzzy_override": op_fuzzy_override, "split_write": op_split_write,
    "rank": op_rank, "value_counts": op_value_counts, "bin": op_bin,
    "describe": op_describe, "cumulative": op_cumulative, "head_tail": op_head_tail,
}

def _result_handler(ctx, sid, result, lg):
    if result is not None:
        ctx["df"] = result; ctx["results"][sid] = result
        lg.info(f"[{sid}] 完成 rows={len(result)}")

def run(config_path):
    PipelineEngine(
        OP_MAP, 
        init_ctx=lambda: {"df": None, "results": {}, "base_dir": BASE_DIR},
        eval_vars_fn=lambda ctx: {"row_count": len(ctx["df"]) if ctx["df"] is not None else 0},
        result_handler=_result_handler,
        done_fn=lambda ctx, lg: lg.info(f"执行完成 total_rows={len(ctx['df']) if ctx['df'] is not None else 0}")
    ).execute(config_path)
if __name__ == '__main__':
    run(None)