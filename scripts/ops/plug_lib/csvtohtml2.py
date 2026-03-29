import csv, json
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
    if not rows or len(rows) < 2:
        print(f"文件 {csv_path} 行数不足，跳过处理")
        return
    titles = rows[0]
    legend = []
    series_data = []
    for row in rows[1:]:
        legend.append(row[0])
        values = []
        for v in row[1:]:
            try: values.append(float(v))
            except: values.append(None)
        series_data.append(values)
    time_points = titles[1:]

    series_js = []
    for i, name in enumerate(legend):
        data = [v if v is not None else 'null' for v in series_data[i]]
        series_js.append(f"""{{
            name: '{name}',
            type: 'line',
            data: [{','.join(str(d) for d in data)}]
        }}""")

    now = datetime.now()
    title = f"数据展示({now.year}/{now.month}/{now.day})"

    # 读取本地echarts.min.js文件内容，并内嵌
    echarts_js_path = Path(__file__).parent / 'html_script' / 'echarts.min.js'
    if not echarts_js_path.exists():
        print(f"错误: 找不到本地的 {echarts_js_path}，请确保echarts.min.js文件存在")
        return
    echarts_js = echarts_js_path.read_text(encoding='utf-8')

    html = f"""<!DOCTYPE html>
<html lang='zh-CN'>
<head>
<meta charset="utf-8">
<title>{title}</title>
<style> #chart {{width:100%;height:600px;}} </style>
<script>{echarts_js}</script>
</head>
<body>
<div id="chart"></div>
<script>
var chart = echarts.init(document.getElementById('chart'));
chart.setOption({{
    title: {{text: '{title}'}},
    tooltip: {{trigger: 'axis'}},
    legend: {{data: {json.dumps(legend, ensure_ascii=False)}}},
    xAxis: {{
        type: 'category',
        data: {json.dumps(time_points)}
    }},
    yAxis: {{
        type: 'value'
    }},
    series: [{','.join(series_js)}]
}});
</script>
</body>
</html>"""
    out_path = Path(out_path) if out_path else csv_path.with_suffix('.html')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"已生成: {out_path}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
        out_file = sys.argv[2] if len(sys.argv) > 2 else None
        csv_to_html(csv_file, out_file)
    else:
        print("请提供CSV文件路径")