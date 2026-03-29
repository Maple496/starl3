"""
数据可视化操作模块 - 将表格数据转换为 HTML
"""
import json
import os
import re
from pathlib import Path
from typing import Dict, Any, Optional, List
import pandas as pd

from core.logger import Logger, get_logger
from core.registry import op

logger = get_logger("datavisual_ops")


def _resolve_path(path: str, ctx: dict) -> str:
    """解析路径中的变量引用，如 ${paths.html_dir}"""
    def replace_var(match):
        var_path = match.group(1)
        parts = var_path.split('.')
        current = ctx
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return match.group(0)  # 变量不存在，保持原样
        return str(current) if current is not None else match.group(0)
    
    return re.sub(r'\$\{([^}]+)\}', replace_var, path)


def _df_to_html_table(df: pd.DataFrame, title: str = "数据展示") -> str:
    """将 DataFrame 转为交互式 HTML 表格"""
    if df is None or df.empty:
        return "<html><body><h3>无数据</h3></body></html>"
    
    # 重置索引，添加序号列
    df = df.reset_index(drop=True)
    df.insert(0, '序号', range(1, len(df) + 1))
    
    # 处理数据：将 NaN/None 替换为空字符串或 None，确保JSON兼容
    import math
    df = df.copy()
    
    def clean_value(x):
        """清理值：NaN/None/null -> 空字符串，其他保持原样"""
        if x is None or x is pd.NA:
            return ''
        if isinstance(x, float):
            if math.isnan(x):
                return ''
            return x
        if isinstance(x, str):
            if x.lower() in ('nan', 'none', 'null', ''):
                return ''
            return x
        return x
    
    for col in df.columns:
        df[col] = df[col].apply(clean_value)
    
    # 转换为 JSON 数据
    headers = df.columns.tolist()
    data = df.to_dict(orient='records')
    data_json = json.dumps(data, ensure_ascii=False, allow_nan=False)
    
    html = f"""<!DOCTYPE html>
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
    const headers = {json.dumps(headers, ensure_ascii=False)};
    let filteredData=[],currentPage=1;
    const itemsPerPage=1000;
    const $=(id)=>document.getElementById(id);
    const H=$('tableHeader'),B=$('tableBody'),S=$('searchInput'),P=$('pageInput');
    const T=$('totalPages'),D=$('dataCount'),F=$('firstPageBtn'),V=$('prevPageBtn');
    const N=$('nextPageBtn'),L=$('lastPageBtn'),E=$('exportBtn'),ST=$('stats');
    const formatValue=(v)=>{{
      // 处理 null/undefined/NaN 显示为空字符串
      if(v===null || v===undefined || v==='NaN' || v==='nan' || v==='None') return '';
      // 数值格式化
      if(typeof v==='number'){{
        return Number.isInteger(v)?v.toString():v.toFixed(2);
      }}
      return String(v);
    }};
    const createTableHeader=()=>{{
      const tr=document.createElement('tr');
      headers.forEach((key)=>{{
        const th=document.createElement('th');
        th.textContent=key;
        tr.appendChild(th);
      }});
      H.innerHTML='';
      H.appendChild(tr);
    }};
    const updateStats=()=>{{
      const total=tableData.length;
      const showing=filteredData.length;
      ST.textContent=`总记录: ${{total}} | 当前显示: ${{showing}}`;
    }};
    const updatePagination=(data)=>{{
      const totalItems=data.length;
      const totalPages=Math.max(1,Math.ceil(totalItems/itemsPerPage));
      const startIndex=(currentPage-1)*itemsPerPage+1;
      const endIndex=Math.min(currentPage*itemsPerPage,totalItems);
      T.textContent=totalPages;
      D.textContent='(共'+totalItems+'条记录，显示'+startIndex+'-'+endIndex+'条)';
      P.value=currentPage;
      const isFirstPage=currentPage===1,isLastPage=currentPage===totalPages;
      F.disabled=isFirstPage;V.disabled=isFirstPage;N.disabled=isLastPage;L.disabled=isLastPage;
    }};
    const renderTable=(data)=>{{
      const start=(currentPage-1)*itemsPerPage;
      const end=Math.min(start+itemsPerPage,data.length);
      const pageData=data.slice(start,end);
      B.innerHTML='';
      pageData.forEach((row)=>{{
        const tr=document.createElement('tr');
        headers.forEach((key)=>{{
          const td=document.createElement('td');
          td.textContent=formatValue(row[key]);
          tr.appendChild(td);
        }});
        B.appendChild(tr);
      }});
      updatePagination(data);
    }};
    const searchData=()=>{{
      const keyword=S.value.trim();
      if(keyword){{
        const terms=keyword.split(/\\s+/).filter(Boolean);
        filteredData=tableData.filter((row)=>{{
          const text=Object.values(row).join(' ').toLowerCase();
          return terms.some((term)=>text.includes(term.toLowerCase()));
        }});
      }}else{{filteredData=[...tableData];}}
      currentPage=1;
      renderTable(filteredData);
      updateStats();
    }};
    const goToPage=(page)=>{{
      const totalPages=Math.max(1,Math.ceil(filteredData.length/itemsPerPage));
      if(page>=1&&page<=totalPages){{currentPage=page;renderTable(filteredData);}}
      else{{P.value=currentPage;}}
    }};
    const exportToCSV=()=>{{
      if(!filteredData.length)return;
      const csvContent=[headers.join(','),...filteredData.map((row)=>headers.map((k)=>{{const v=row[k];return '"'+(v==null?'':String(v)).replace(/"/g,'""')+'"';}}).join(','))].join('\\n');
      const blob=new Blob(['\\ufeff'+csvContent],{{type:'text/csv;charset=utf-8;'}});
      const link=document.createElement('a');
      if(link.download!==undefined){{
        let baseName=decodeURIComponent(location.pathname.split('/').pop()||'')||'data';
        baseName=baseName.replace(/\\.[^.]*$/,'');
        const now=new Date();
        const pad2=(n)=>String(n).padStart(2,'0');
        const stamp=`${{now.getFullYear()}}${{pad2(now.getMonth()+1)}}${{pad2(now.getDate())}}_${{pad2(now.getHours())}}${{pad2(now.getMinutes())}}${{pad2(now.getSeconds())}}`;
        const url=URL.createObjectURL(blob);
        link.href=url;link.download=`${{baseName}}_${{stamp}}.csv`;
        link.style.display='none';document.body.appendChild(link);link.click();document.body.removeChild(link);
      }}
    }};
    const init=()=>{{
      document.title=pageTitleName;
      filteredData=[...tableData];
      if(tableData.length){{createTableHeader();renderTable(filteredData);}}
      updateStats();
      S.addEventListener('input',searchData);
      P.addEventListener('change',()=>{{const page=parseInt(P.value,10)||1;goToPage(page);}});
      F.addEventListener('click',()=>goToPage(1));
      V.addEventListener('click',()=>goToPage(currentPage-1));
      N.addEventListener('click',()=>goToPage(currentPage+1));
      L.addEventListener('click',()=>{{const lastPage=Math.ceil(filteredData.length/itemsPerPage)||1;goToPage(lastPage);}});
      E.addEventListener('click',exportToCSV);
    }};
    document.addEventListener('DOMContentLoaded',init);
  </script>
</body>
</html>"""
    return html


def _df_to_html_chart(df: pd.DataFrame, title: str = "数据图表", 
                     x_column: str = None, y_columns: List[str] = None,
                     chart_type: str = "line") -> str:
    """将 DataFrame 转为 ECharts 图表 HTML"""
    if df is None or df.empty:
        return "<html><body><h3>无数据</h3></body></html>"
    
    if x_column is None:
        x_column = df.columns[0]
    
    if y_columns is None:
        y_columns = [col for col in df.columns 
                     if col != x_column and pd.api.types.is_numeric_dtype(df[col])]
        if not y_columns:
            y_columns = [col for col in df.columns if col != x_column]
    
    # 准备数据
    x_data = df[x_column].astype(str).tolist()
    series_data = []
    
    for col in y_columns:
        values = []
        for v in df[col]:
            try:
                values.append(float(v))
            except (ValueError, TypeError):
                values.append(None)
        
        series_js = f"""{{
            name: '{col}',
            type: '{chart_type}',
            data: [{','.join('null' if v is None else str(v) for v in values)}],
            smooth: true,
            symbol: 'circle',
            symbolSize: 6
        }}"""
        series_data.append(series_js)
    
    # 读取 ECharts
    echarts_path = Path(__file__).parent.parent / 'html_script' / 'echarts.min.js'
    if echarts_path.exists():
        echarts_js = echarts_path.read_text(encoding='utf-8')
    else:
        echarts_js = ""
    
    cdn_script = '' if echarts_path.exists() else '<script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></script>'
    echarts_inline = f'<script>{echarts_js}</script>' if echarts_path.exists() else ''
    
    html = f"""<!DOCTYPE html>
<html lang='zh-CN'>
<head>
  <meta charset='UTF-8' />
  <meta name='viewport' content='width=device-width,initial-scale=1' />
  <title>{title}</title>
  <style>
    body {{font-family:'Microsoft YaHei',Arial,sans-serif;margin:0;padding:20px;background:#f5f5f5;}}
    .container {{background:#fff;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.1);padding:20px;}}
    .chart-container {{width:100%;height:600px;}}
    .controls {{margin-bottom:15px;padding:10px;background:#f8f9fa;border-radius:4px;}}
    .info {{color:#666;font-size:13px;margin-top:10px;}}
  </style>
  {cdn_script}
  {echarts_inline}
</head>
<body>
  <div class='container'>
    <div class='controls'>
      <strong>{title}</strong>
      <span class='info'>| X轴: {x_column} | 系列: {', '.join(y_columns)} | 类型: {chart_type}</span>
    </div>
    <div id='chart' class='chart-container'></div>
  </div>
  <script>
    const chart = echarts.init(document.getElementById('chart'));
    const option = {{
      title: {{text: '{title}', left: 'center', top: 10}},
      tooltip: {{trigger: 'axis', axisPointer: {{type: 'cross'}}}},
      legend: {{data: {json.dumps(y_columns, ensure_ascii=False)}, top: 40}},
      grid: {{left: '3%', right: '4%', bottom: '3%', top: 80, containLabel: true}},
      toolbox: {{
        feature: {{
          dataZoom: {{yAxisIndex: 'none'}},
          restore: {{}},
          saveAsImage: {{}},
          dataView: {{readOnly: false}},
          magicType: {{type: ['line', 'bar']}}
        }}
      }},
      xAxis: {{
        type: 'category',
        boundaryGap: {str(chart_type == 'bar').lower()},
        data: {json.dumps(x_data, ensure_ascii=False)},
        axisLabel: {{rotate: 45, interval: 'auto'}}
      }},
      yAxis: {{type: 'value', scale: true}},
      series: [{','.join(series_data)}]
    }};
    chart.setOption(option);
    window.addEventListener('resize', () => chart.resize());
  </script>
</body>
</html>"""
    return html


def _df_to_html_summary(df: pd.DataFrame, title: str = "数据汇总") -> str:
    """生成数据统计摘要 HTML 页面"""
    if df is None or df.empty:
        return "<html><body><h3>无数据</h3></body></html>"
    
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    stats = []
    
    for col in numeric_cols[:10]:
        col_stats = df[col].describe()
        stats.append({
            'column': col,
            'count': int(col_stats['count']),
            'mean': round(col_stats['mean'], 2),
            'std': round(col_stats['std'], 2),
            'min': round(col_stats['min'], 2),
            '25%': round(col_stats['25%'], 2),
            '50%': round(col_stats['50%'], 2),
            '75%': round(col_stats['75%'], 2),
            'max': round(col_stats['max'], 2)
        })
    
    null_cols = df.isnull().sum()
    null_cols = null_cols[null_cols > 0].head(10).to_dict()
    
    html = f"""<!DOCTYPE html>
<html lang='zh-CN'>
<head>
  <meta charset='UTF-8' />
  <title>{title}</title>
  <style>
    body {{font-family:'Microsoft YaHei',Arial,sans-serif;margin:0;padding:20px;background:#f5f5f5;}}
    .container {{max-width:1400px;margin:0 auto;}}
    .card {{background:#fff;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.1);margin-bottom:20px;padding:20px;}}
    h2 {{margin:0 0 15px 0;color:#333;border-bottom:2px solid #007bff;padding-bottom:10px;}}
    .metric-grid {{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:15px;}}
    .metric {{background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);color:#fff;padding:15px;border-radius:8px;}}
    .metric-value {{font-size:28px;font-weight:bold;margin:5px 0;}}
    .metric-label {{font-size:12px;opacity:0.9;}}
    table {{width:100%;border-collapse:collapse;margin-top:10px;}}
    th,td {{padding:10px;text-align:left;border-bottom:1px solid #ddd;}}
    th {{background:#f8f9fa;font-weight:600;color:#555;}}
    tr:hover {{background:#f5f5f5;}}
  </style>
</head>
<body>
  <div class='container'>
    <div class='card'>
      <h2>数据概览</h2>
      <div class='metric-grid'>
        <div class='metric'>
          <div class='metric-label'>总行数</div>
          <div class='metric-value'>{len(df):,}</div>
        </div>
        <div class='metric'>
          <div class='metric-label'>总列数</div>
          <div class='metric-value'>{len(df.columns)}</div>
        </div>
        <div class='metric'>
          <div class='metric-label'>数值列数</div>
          <div class='metric-value'>{len(numeric_cols)}</div>
        </div>
        <div class='metric'>
          <div class='metric-label'>内存占用</div>
          <div class='metric-value'>{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB</div>
        </div>
      </div>
    </div>
    
    <div class='card'>
      <h2>数值列统计</h2>
      {'<p>无数值列</p>' if not stats else f"""
      <table>
        <tr>
          <th>列名</th>
          <th>数量</th>
          <th>平均值</th>
          <th>标准差</th>
          <th>最小值</th>
          <th>25%</th>
          <th>中位数</th>
          <th>75%</th>
          <th>最大值</th>
        </tr>
        {''.join(f"<tr><td>{s['column']}</td><td>{s['count']}</td><td>{s['mean']}</td><td>{s['std']}</td><td>{s['min']}</td><td>{s['25%']}</td><td>{s['50%']}</td><td>{s['75%']}</td><td>{s['max']}</td></tr>" for s in stats)}
      </table>
      """}
    </div>
    
    <div class='card'>
      <h2>列信息</h2>
      <table>
        <tr><th>列名</th><th>数据类型</th><th>非空值</th><th>空值数</th></tr>
        {''.join(f"<tr><td>{col}</td><td>{str(dtype)}</td><td>{df[col].notna().sum()}</td><td>{df[col].isna().sum()}</td></tr>" for col, dtype in df.dtypes.items())}
      </table>
    </div>
    
    {f"""
    <div class='card'>
      <h2>缺失值统计 (前10列)</h2>
      <table>
        <tr><th>列名</th><th>缺失数</th><th>缺失率</th></tr>
        {''.join(f"<tr><td>{col}</td><td>{int(count)}</td><td>{count/len(df)*100:.1f}%</td></tr>" for col, count in null_cols.items())}
      </table>
    </div>
    """ if null_cols else ''}
  </div>
</body>
</html>"""
    return html


# ==================== Pipeline 操作函数 ====================

@op("to_table_html", category="datavisual", description="将DataFrame转为交互式表格HTML")
def op_to_table_html(ctx, params):
    """
    参数:
        - file: 输出HTML文件路径
        - title: 页面标题（可选，默认"数据展示"）
    """
    df = ctx.get('last_result')
    if df is None:
        return "错误: 无数据，请先读取数据"
    
    if not isinstance(df, pd.DataFrame):
        return f"错误: 当前结果不是 DataFrame，而是 {type(df).__name__}"
    
    file_path = params.get('file', 'output.html')
    title = params.get('title', '数据展示')
    
    # 解析变量并处理路径
    file_path = _resolve_path(file_path, ctx)
    if os.path.isabs(file_path):
        out_path = Path(file_path)
    else:
        out_path = Path(ctx.get('base_dir', '.')) / file_path
    
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    html = _df_to_html_table(df, title)
    out_path.write_text(html, encoding='utf-8')
    
    # 返回DataFrame以保持数据流，同时记录日志
    logger.info(f"已生成表格HTML: {out_path} ({len(df)}行)")
    return df


@op("to_chart_html", category="datavisual", description="将DataFrame转为ECharts图表HTML")
def op_to_chart_html(ctx, params):
    """
    参数:
        - file: 输出HTML文件路径
        - title: 图表标题（可选）
        - x_column: X轴列名（可选，默认第一列）
        - y_columns: Y轴列名列表，逗号分隔（可选，默认所有数值列）
        - chart_type: 图表类型 - line/bar/scatter（可选，默认line）
    """
    df = ctx.get('last_result')
    if df is None:
        return "错误: 无数据"
    
    if not isinstance(df, pd.DataFrame):
        return f"错误: 当前结果不是 DataFrame"
    
    file_path = params.get('file', 'chart.html')
    title = params.get('title', '数据图表')
    x_column = params.get('x_column')
    y_columns = params.get('y_columns')
    chart_type = params.get('chart_type', 'line')
    
    if y_columns:
        y_columns = [c.strip() for c in y_columns.split(',')]
    
    # 解析变量并处理路径
    file_path = _resolve_path(file_path, ctx)
    if os.path.isabs(file_path):
        out_path = Path(file_path)
    else:
        out_path = Path(ctx.get('base_dir', '.')) / file_path
    
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    html = _df_to_html_chart(df, title, x_column, y_columns, chart_type)
    out_path.write_text(html, encoding='utf-8')
    
    logger.info(f"已生成图表HTML: {out_path}")
    return df


@op("to_summary_html", category="datavisual", description="将DataFrame生成数据汇总报告HTML")
def op_to_summary_html(ctx, params):
    """
    参数:
        - file: 输出HTML文件路径
        - title: 页面标题（可选）
    """
    df = ctx.get('last_result')
    if df is None:
        return "错误: 无数据"
    
    if not isinstance(df, pd.DataFrame):
        return f"错误: 当前结果不是 DataFrame"
    
    file_path = params.get('file', 'summary.html')
    title = params.get('title', '数据汇总')
    
    # 解析变量并处理路径
    file_path = _resolve_path(file_path, ctx)
    if os.path.isabs(file_path):
        out_path = Path(file_path)
    else:
        out_path = Path(ctx.get('base_dir', '.')) / file_path
    
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    html = _df_to_html_summary(df, title)
    out_path.write_text(html, encoding='utf-8')
    
    logger.info(f"已生成汇总HTML: {out_path}")
    return df


@op("csv_to_html", category="datavisual", description="将CSV文件转为HTML表格（直接文件转换）")
def op_csv_to_html(ctx, params):
    """
    参数:
        - input: 输入CSV文件路径
        - output: 输出HTML文件路径（可选，默认同名.html）
        - title: 页面标题（可选）
    """
    input_path = params.get('input')
    if not input_path:
        return "错误: 未指定 input 参数"
    
    # 解析变量并处理路径
    input_path = _resolve_path(input_path, ctx)
    if os.path.isabs(input_path):
        csv_path = Path(input_path)
    else:
        csv_path = Path(ctx.get('base_dir', '.')) / input_path
    
    if not csv_path.exists():
        return f"错误: 文件不存在 {csv_path}"
    
    # 读取CSV
    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, encoding='gb18030')
    
    # 输出路径
    output = params.get('output')
    if output:
        if os.path.isabs(output):
            out_path = Path(output)
        else:
            out_path = Path(ctx.get('base_dir', '.')) / output
    else:
        out_path = csv_path.with_suffix('.html')
    
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    title = params.get('title', '数据展示')
    html = _df_to_html_table(df, title)
    out_path.write_text(html, encoding='utf-8')
    
    return f"已转换: {csv_path} -> {out_path}"


# 兼容旧接口
OP_MAP = {
    'to_table_html': op_to_table_html,
    'to_chart_html': op_to_chart_html,
    'to_summary_html': op_to_summary_html,
    'csv_to_html': op_csv_to_html,
}


def run(config_path: str):
    """独立运行模式（兼容旧版）"""
    pass
