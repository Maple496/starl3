import sys
import csv
import os
from datetime import datetime
from pathlib import Path

def read_csv_auto(csv_path):
    for enc in ['utf-8-sig', 'utf-8', 'gb18030', 'gbk', 'cp936', 'latin-1']:
        try:
            with open(csv_path, encoding=enc) as f:
                return list(csv.reader(f))
        except (UnicodeDecodeError, UnicodeError):
            pass
    with open(csv_path, encoding='utf-8', errors='replace') as f:
        return list(csv.reader(f))

def csv_to_html(csv_path, out_path=None):
    csv_path = Path(csv_path)
    rows = read_csv_auto(csv_path)

    now = datetime.now()
    title = f"数据展示({now.year}/{now.month}/{now.day})"

    csv_content = '\n'.join(','.join(row) for row in rows)
    csv_content = csv_content.replace('`', '\\`')

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
    const csvData = `{csv_content}`;
    let tableData=[],filteredData=[],currentPage=1;
    const itemsPerPage=1000;
    const $=(id)=>document.getElementById(id);
    const H=$('tableHeader'),B=$('tableBody'),S=$('searchInput'),P=$('pageInput');
    const T=$('totalPages'),D=$('dataCount'),F=$('firstPageBtn'),V=$('prevPageBtn');
    const N=$('nextPageBtn'),L=$('lastPageBtn'),E=$('exportBtn');
    const parseCSV=(csv)=>{{
      const lines=csv.trim().split('\\n');
      const headers=lines[0].split(',');
      const rows=[];
      for(let i=1;i<lines.length;i++){{
        const values=lines[i].split(',');
        const row={{}};
        headers.forEach((header,index)=>{{row[header.trim()]=values[index]?values[index].trim():'';}});
        rows.push(row);
      }}
      return rows;
    }};
    const createTableHeader=(data)=>{{
      const tr=document.createElement('tr');
      const indexTh=document.createElement('th');
      indexTh.textContent='序号';
      tr.appendChild(indexTh);
      Object.keys(data[0]).forEach((key)=>{{
        const th=document.createElement('th');
        th.textContent=key;
        tr.appendChild(th);
      }});
      H.innerHTML='';
      H.appendChild(tr);
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
      let index=start+1;
      pageData.forEach((row)=>{{
        const tr=document.createElement('tr');
        const indexTd=document.createElement('td');
        indexTd.textContent=index++;
        tr.appendChild(indexTd);
        Object.values(row).forEach((value)=>{{
          const td=document.createElement('td');
          td.textContent=value;
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
    }};
    const goToPage=(page)=>{{
      const totalPages=Math.max(1,Math.ceil(filteredData.length/itemsPerPage));
      if(page>=1&&page<=totalPages){{currentPage=page;renderTable(filteredData);}}
      else{{P.value=currentPage;}}
    }};
    const exportToCSV=()=>{{
      if(!filteredData.length)return;
      const keys=Object.keys(filteredData[0]);
      const csvContent=[keys.join(','),...filteredData.map((row)=>keys.map((k)=>row[k]).join(','))].join('\\n');
      const blob=new Blob([csvContent],{{type:'text/csv;charset=utf-8;'}});
      const link=document.createElement('a');
      if(link.download!==undefined){{
        let baseName=decodeURIComponent(location.pathname.split('/').pop()||'')||'index';
        baseName=baseName.replace(/\\.[^.]*$/,'');
        const now=new Date();
        const pad2=(n)=>String(n).padStart(2,'0');
        const stamp=`${{now.getFullYear()}}${{pad2(now.getMonth()+1)}}${{pad2(now.getDate())}}${{pad2(now.getHours())}}${{pad2(now.getMinutes())}}${{pad2(now.getSeconds())}}`;
        const url=URL.createObjectURL(blob);
        link.href=url;link.download=`${{baseName}}_${{stamp}}.csv`;
        link.style.display='none';document.body.appendChild(link);link.click();document.body.removeChild(link);
      }}
    }};
    const init=()=>{{
      document.title=pageTitleName;
      tableData=parseCSV(csvData);
      filteredData=[...tableData];
      if(tableData.length){{createTableHeader(tableData);renderTable(filteredData);}}
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

    out_path = Path(out_path) if out_path else csv_path.with_suffix('.html')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"已生成: {out_path}")

csv_file = sys.argv[1] if len(sys.argv) > 1 else Path(sys.argv[0]).parent / 'data.csv'
out_file = sys.argv[2] if len(sys.argv) > 2 else None
csv_to_html(csv_file, out_file)