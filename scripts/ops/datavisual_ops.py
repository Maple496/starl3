"""
数据可视化操作模块 - 将表格数据转换为 HTML
"""
import json
import os
import re
from pathlib import Path
from typing import Dict, Any
import pandas as pd

from core.logger import get_logger
from core.registry import op

logger = get_logger("datavisual_ops")


def _resolve_path(path: str, ctx: dict) -> str:
    """解析路径中的变量引用"""
    return re.sub(r'\$\{([^}]+)\}', 
                  lambda m: str(_get_ctx_value(ctx, m.group(1)) or m.group(0)), 
                  path)


def _get_ctx_value(ctx: dict, var_path: str) -> Any:
    """获取上下文中的值"""
    parts = var_path.split('.')
    current = ctx
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current


def _df_to_html_table(df: pd.DataFrame, title: str = "数据展示") -> str:
    """将 DataFrame 转为交互式 HTML 表格"""
    if df is None or df.empty:
        return "<html><body><h3>无数据</h3></body></html>"
    
    df = df.reset_index(drop=True)
    df.insert(0, '序号', range(1, len(df) + 1))
    
    # 清理数据
    import math
    for col in df.columns:
        df[col] = df[col].apply(lambda x: '' if x is None or x is pd.NA or 
                                (isinstance(x, float) and math.isnan(x)) or
                                (isinstance(x, str) and x.lower() in ('nan', 'none', 'null', '')) 
                                else x)
    
    headers = df.columns.tolist()
    data_json = json.dumps(df.to_dict(orient='records'), ensure_ascii=False, allow_nan=False)
    headers_json = json.dumps(headers, ensure_ascii=False)
    
    return f"""<!DOCTYPE html>
<html lang='zh-CN'>
<head>
  <meta charset='UTF-8' />
  <meta name='viewport' content='width=device-width,initial-scale=1' />
  <title>{title}</title>
  <style>
    body {{font-family:'Microsoft YaHei',Arial,sans-serif;margin:0;padding:10px;background:#f5f5f5;}}
    .container {{height:100vh;display:flex;flex-direction:column;background:#fff;border-radius:4px;box-shadow:0 1px 3px rgba(0,0,0,.1);overflow:hidden;}}
    .search-section {{padding:10px;background:#f8f9fa;border-bottom:1px solid #e9ecef;flex-shrink:0;}}
    .search-box {{display:flex;gap:10px;align-items:center;}}
    .search-box input {{flex:1;padding:8px 12px;border:1px solid #ced4da;border-radius:4px;font-size:14px;}}
    .export-btn {{background:#07f;color:#fff;border:none;padding:8px 16px;border-radius:4px;cursor:pointer;font-size:14px;}}
    .export-btn:hover {{background:#0056b3;}}
    .main-content {{flex:1;display:flex;flex-direction:column;min-height:0;}}
    .table-container {{flex:1;overflow:auto;}}
    table {{width:100%;border-collapse:separate;border-spacing:0;border:1px solid #ddd;font-size:14px;}}
    th {{background:#000;position:sticky;top:0;z-index:10;border-bottom:2px solid #333;color:#fff;font-weight:700;font-size:16px;}}
    th,td {{padding:8px 6px;text-align:left;border-right:1px solid #ddd;border-bottom:1px solid #ddd;}}
    th:last-child,td:last-child {{border-right:none;}}
    th:nth-child(1),td:nth-child(1) {{max-width:45px;overflow:hidden;text-overflow:ellipsis;text-align:center;}}
    tr:hover {{background:#fff3cd;}}
    tr:nth-child(even) td {{background:#f8f9fa;}}
    tr:nth-child(even):hover td {{background:#fff3cd;}}
    .pagination {{padding:10px;background:#f8f9fa;text-align:center;flex-shrink:0;border-top:1px solid #e9ecef;}}
    .pagination-controls {{display:flex;justify-content:center;align-items:center;gap:8px;}}
    .pagination-btn {{padding:4px 8px;border:1px solid #ced4da;background:#fff;color:#495057;border-radius:4px;cursor:pointer;font-size:12px;}}
    .pagination-btn:hover {{background:#e9ecef;}}
    .pagination-btn:disabled {{opacity:.5;cursor:not-allowed;}}
    .page-input {{width:50px;padding:3px;text-align:center;border:1px solid #ced4da;border-radius:4px;}}
    .page-info {{font-size:12px;color:#6c757d;}}
    .stats {{padding:8px 15px;background:#e3f2fd;border-bottom:1px solid #bbdefb;font-size:13px;color:#1565c0;}}
  </style>
</head>
<body>
  <div class='container'>
    <div class='search-section'>
      <div class='search-box'>
        <input id='searchInput' placeholder='全局搜索(用空格分隔多个搜索词)' />
        <button class='export-btn' id='exportBtn'>导出CSV</button>
      </div>
    </div>
    <div class='stats' id='stats'></div>
    <div class='main-content'>
      <div class='table-container'>
        <table id='dataTable'>
          <thead id='tableHeader'></thead>
          <tbody id='tableBody'></tbody>
        </table>
      </div>
      <div class='pagination'>
        <div class='pagination-controls'>
          <button class='pagination-btn' id='firstPageBtn'>首页</button>
          <button class='pagination-btn' id='prevPageBtn'>上一页</button>
          <span class='page-info'>第<input type='number' class='page-input' id='pageInput' min='1' value='1'/>/<span id='totalPages'>1</span>页<span id='dataCount'></span></span>
          <button class='pagination-btn' id='nextPageBtn'>下一页</button>
          <button class='pagination-btn' id='lastPageBtn'>末页</button>
        </div>
      </div>
    </div>
  </div>
  <script>
    const pageTitleName = '{title}';
    const tableData = {data_json};
    const headers = {headers_json};
    let filteredData=[],currentPage=1;
    const itemsPerPage=1000;
    const $=(id)=>document.getElementById(id);
    const formatValue=(v)=>{{
      if(v===null||v===undefined||v==='NaN'||v==='nan'||v==='None')return '';
      if(typeof v==='number')return Number.isInteger(v)?v.toString():v.toFixed(2);
      return String(v);
    }};
    const createTableHeader=()=>{{
      const tr=document.createElement('tr');
      headers.forEach((key)=>{{const th=document.createElement('th');th.textContent=key;tr.appendChild(th);}});
      $('tableHeader').innerHTML='';$('tableHeader').appendChild(tr);
    }};
    const updateStats=()=>{{
      $('stats').textContent=`总记录: ${{tableData.length}} | 当前显示: ${{filteredData.length}}`;
    }};
    const updatePagination=(data)=>{{
      const totalPages=Math.max(1,Math.ceil(data.length/itemsPerPage));
      const startIndex=(currentPage-1)*itemsPerPage+1;
      const endIndex=Math.min(currentPage*itemsPerPage,data.length);
      $('totalPages').textContent=totalPages;
      $('dataCount').textContent='(共'+data.length+'条记录，显示'+startIndex+'-'+endIndex+'条)';
      $('pageInput').value=currentPage;
      $('firstPageBtn').disabled=$('prevPageBtn').disabled=currentPage===1;
      $('nextPageBtn').disabled=$('lastPageBtn').disabled=currentPage===totalPages;
    }};
    const renderTable=(data)=>{{
      const start=(currentPage-1)*itemsPerPage;
      const end=Math.min(start+itemsPerPage,data.length);
      const pageData=data.slice(start,end);
      $('tableBody').innerHTML='';
      pageData.forEach((row)=>{{
        const tr=document.createElement('tr');
        headers.forEach((key)=>{{const td=document.createElement('td');td.textContent=formatValue(row[key]);tr.appendChild(td);}});
        $('tableBody').appendChild(tr);
      }});
      updatePagination(data);
    }};
    const searchData=()=>{{
      const keyword=$('searchInput').value.trim();
      if(keyword){{
        const terms=keyword.split(/\\s+/).filter(Boolean);
        filteredData=tableData.filter((row)=>{{const text=Object.values(row).join(' ').toLowerCase();return terms.some((term)=>text.includes(term.toLowerCase()));}});
      }}else{{filteredData=[...tableData];}}
      currentPage=1;renderTable(filteredData);updateStats();
    }};
    const goToPage=(page)=>{{
      const totalPages=Math.max(1,Math.ceil(filteredData.length/itemsPerPage));
      if(page>=1&&page<=totalPages){{currentPage=page;renderTable(filteredData);}}else{{$('pageInput').value=currentPage;}}
    }};
    const exportToCSV=()=>{{
      if(!filteredData.length)return;
      const csvContent=[headers.join(','),...filteredData.map((row)=>headers.map((k)=>{{const v=row[k];return'"'+(v==null?'':String(v)).replace(/"/g,'""')+'"';}}).join(','))].join('\\n');
      const blob=new Blob(['\\ufeff'+csvContent],{{type:'text/csv;charset=utf-8;'}});
      const link=document.createElement('a');
      const baseName=decodeURIComponent(location.pathname.split('/').pop()||'').replace(/\\.[^.]*$/,'')||'data';
      const now=new Date();
      const pad2=(n)=>String(n).padStart(2,'0');
      const stamp=`${{now.getFullYear()}}${{pad2(now.getMonth()+1)}}${{pad2(now.getDate())}}_${{pad2(now.getHours())}}${{pad2(now.getMinutes())}}${{pad2(now.getSeconds())}}`;
      link.href=URL.createObjectURL(blob);link.download=`${{baseName}}_${{stamp}}.csv`;link.click();
    }};
    const init=()=>{{
      document.title=pageTitleName;
      filteredData=[...tableData];
      if(tableData.length){{createTableHeader();renderTable(filteredData);}}
      updateStats();
      $('searchInput').addEventListener('input',searchData);
      $('pageInput').addEventListener('change',()=>goToPage(parseInt($('pageInput').value,10)||1));
      $('firstPageBtn').addEventListener('click',()=>goToPage(1));
      $('prevPageBtn').addEventListener('click',()=>goToPage(currentPage-1));
      $('nextPageBtn').addEventListener('click',()=>goToPage(currentPage+1));
      $('lastPageBtn').addEventListener('click',()=>goToPage(Math.ceil(filteredData.length/itemsPerPage)||1));
      $('exportBtn').addEventListener('click',exportToCSV);
    }};
    document.addEventListener('DOMContentLoaded',init);
  </script>
</body>
</html>"""


@op("to_table_html", category="datavisual", description="将DataFrame转为交互式表格HTML")
def op_to_table_html(ctx, params):
    """将DataFrame转为交互式表格HTML"""
    df = ctx.get('last_result')
    if df is None:
        return "错误: 无数据"
    if not isinstance(df, pd.DataFrame):
        return f"错误: 当前结果不是 DataFrame"
    
    file_path = params.get('file', 'output.html')
    title = params.get('title', '数据展示')
    
    file_path = _resolve_path(file_path, ctx)
    out_path = Path(file_path) if os.path.isabs(file_path) else Path(ctx.get('base_dir', '.')) / file_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    html = _df_to_html_table(df, title)
    out_path.write_text(html, encoding='utf-8')
    
    logger.info(f"已生成表格HTML: {out_path} ({len(df)}行)")
    return df


@op("csv_to_html", category="datavisual", description="将CSV文件转为HTML表格")
def op_csv_to_html(ctx, params):
    """将CSV文件转为HTML表格"""
    input_path = params.get('input')
    if not input_path:
        return "错误: 未指定 input 参数"
    
    input_path = _resolve_path(input_path, ctx)
    csv_path = Path(input_path) if os.path.isabs(input_path) else Path(ctx.get('base_dir', '.')) / input_path
    
    if not csv_path.exists():
        return f"错误: 文件不存在 {csv_path}"
    
    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, encoding='gb18030')
    
    output = params.get('output')
    if output:
        out_path = Path(output) if os.path.isabs(output) else Path(ctx.get('base_dir', '.')) / output
    else:
        out_path = csv_path.with_suffix('.html')
    
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    html = _df_to_html_table(df, params.get('title', '数据展示'))
    out_path.write_text(html, encoding='utf-8')
    
    return f"已转换: {csv_path} -> {out_path}"


