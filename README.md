# StarL3 - 可视化数据流程编排引擎

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.8%2B-blue" alt="Python 3.8+">
  <img src="https://img.shields.io/badge/License-MIT-green" alt="MIT License">
  <img src="https://img.shields.io/badge/Platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey" alt="Platform">
</p>

StarL3 是一个**低代码数据流程编排引擎**，通过 JSON 配置即可实现复杂的数据处理流程。内置 Web 可视化编辑器，支持 Excel/CSV 数据处理、数据可视化、网络爬虫、定时任务等多种功能。

## ✨ 核心特性

### 🎯 低代码配置
- 使用 JSON 配置文件定义完整数据处理流程
- 无需编写代码，通过 Web 界面拖拽配置
- 支持变量、条件分支、循环等高级特性

### 📊 强大的数据处理能力
- **ETL/ELT 操作**：读取、转换、合并、拆分 Excel/CSV
- **数据清洗**：过滤、排序、重命名、计算字段
- **智能映射**：支持多表关联、模糊匹配、动态分工

### 🖥️ 可视化编辑器
- 内置 Web 编辑器（基于浏览器）
- 虚拟滚动支持万级数据流畅编辑
- 实时预览、撤销/重做、批量操作
- 自动路径检测，开箱即用

### 🔧 丰富的操作模块

| 模块 | 功能 |
|------|------|
| **elt_ops** | Excel/CSV 读写、数据转换、合并拆分 |
| **datavisual_ops** | HTML 报表生成、ECharts 图表、数据汇总 |
| **crawler_ops** | HTTP 请求、HTML 解析、递归爬取、会话管理 |
| **file_ops** | 文件扫描、筛选、批量重命名/删除/复制 |
| **email_ops** | 邮件发送、邮件监听触发 |
| **scheduler_ops** | 定时任务、延迟执行 |
| **ai_ops** | AI 接口调用（支持多种模型）|

### 🚀 企业级特性
- **配置缓存**：常用路径自动记忆
- **增量处理**：支持大文件分批处理
- **错误处理**：详细的日志记录和错误追踪
- **路径安全**：防止目录遍历攻击
- **表达式引擎**：安全的 Python 表达式求值

## 📦 快速开始

### 安装

```bash
# 克隆仓库
git clone https://github.com/maple496/starl3.git
cd starl3

# 创建虚拟环境（推荐）
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/macOS
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 启动编辑器

```bash
# 启动 Web 编辑器
python scripts/main_edit.py

# 浏览器自动打开 http://127.0.0.1:5001
```

### 运行配置文件

```bash
# 直接运行 JSON 配置文件
python scripts/main_starl3.py configs/my_pipeline.json
```

## 📖 使用示例

### 示例 1：采购数据处理（PR/PO 分离）

```json
[
  ["【初始化】", "0", "print", "{}", "Y", "=== 采购数据处理 ==="],
  ["选择源目录", "10", "select_resource", 
   {"mode":"folder","title":"选择数据目录","save_to":"paths.source_dir"}, "Y", ""],
  ["扫描文件", "20", "scan_directory", 
   {"folder_path":"${paths.source_dir}"}, "Y", ""],
  ["合并Excel", "30", "merge_excel_files", 
   {"sheet":0,"header_row":1}, "Y", ""],
  ["输出PR", "40", "write_csv", 
   {"file":"${paths.output_dir}/313.csv","encoding":"gbk"}, "Y", ""]
]
```

### 示例 2：数据爬虫

```json
[
  ["登录网站", "10", "session_login", 
   {"url":"https://example.com/login","data":{"username":"admin"}}, "Y", ""],
  ["爬取数据", "20", "crawl_pages", 
   {"urls":["https://example.com/page1"],"selector":".data"}, "Y", ""],
  ["生成报表", "30", "to_table_html", 
   {"file":"output/report.html","title":"数据报表"}, "Y", ""]
]
```

## 🏗️ 项目结构

```
starl3/
├── scripts/
│   ├── core/              # 核心引擎
│   │   ├── pipeline_engine.py   # 流程执行引擎
│   │   ├── registry.py          # 操作注册中心
│   │   ├── safe_eval.py         # 安全表达式求值
│   │   └── ...
│   ├── ops/               # 操作模块
│   │   ├── elt_ops.py           # ETL 数据处理
│   │   ├── datavisual_ops.py    # 数据可视化
│   │   ├── crawler_ops.py       # 网络爬虫
│   │   ├── file_ops.py          # 文件操作
│   │   └── ...
│   ├── edit/              # Web 编辑器
│   │   ├── main_edit.py         # 编辑器入口
│   │   ├── templates.py         # 前端模板
│   │   └── config.py            # 编辑器配置
│   ├── main_starl3.py     # CLI 入口
│   └── configs/           # 配置示例
├── tests/                 # 测试用例
├── configs/               # 默认配置目录
└── README.md
```

## 🛠️ 开发指南

### 自定义操作

```python
from core.registry import op

@op("my_operation", category="custom", description="我的自定义操作")
def my_operation(ctx, params):
    """
    自定义操作
    
    Args:
        ctx: 上下文对象，包含 last_result、results 等
        params: 配置参数
        
    Returns:
        操作结果，会被保存到上下文中
    """
    data = ctx.get("last_result")
    # 处理逻辑...
    return processed_data
```

### 配置说明

| 字段 | 说明 | 示例 |
|------|------|------|
| `step_id` | 步骤标识 | "read_excel" |
| `step_order` | 执行顺序 | "10" |
| `op_type` | 操作类型 | "read_excel", "write_csv" |
| `params_json` | 参数(JSON) | `{"file":"data.xlsx"}` |
| `enabled` | 是否启用 | "Y" / "N" |
| `note` | 备注说明 | "读取主数据" |

## 📝 常用操作速查

### ETL 操作

| 操作 | 功能 | 示例参数 |
|------|------|---------|
| `read_excel` | 读取 Excel | `{"file":"data.xlsx","sheet":0}` |
| `read_csv` | 读取 CSV | `{"file":"data.csv","encoding":"utf-8"}` |
| `write_excel` | 写入 Excel | `{"file":"output.xlsx","sheet":"Sheet1"}` |
| `write_csv` | 写入 CSV | `{"file":"output.csv","encoding":"gbk"}` |
| `filter` | 过滤数据 | `{"column":"status","op":"==","value":"active"}` |
| `sort` | 排序 | `{"column":"date","ascending":false}` |
| `join` | 关联表 | `{"source":"step_id","on":"key","how":"left"}` |
| `merge_excel_files` | 合并多个 Excel | `{"sheet":0,"header_row":1}` |
| `split_write` | 分组输出 | `{"group_col":"dept","name_col":"name"}` |

### 文件操作

| 操作 | 功能 | 示例参数 |
|------|------|---------|
| `scan_directory` | 扫描目录 | `{"folder_path":"/data"}` |
| `filter_files` | 筛选文件 | `{"conditions":[{"field":"name","operator":"~","value":".*\\.xlsx"}]}` |
| `batch_rename` | 批量重命名 | `{"prefix":"new_"}` |
| `batch_delete` | 批量删除 | `{}` |
| `select_resource` | 弹窗选择 | `{"mode":"folder","title":"选择目录"}` |

### 数据可视化

| 操作 | 功能 | 示例参数 |
|------|------|---------|
| `to_table_html` | 表格 HTML | `{"file":"report.html","title":"数据表"}` |
| `to_chart_html` | 图表 HTML | `{"file":"chart.html","type":"bar"}` |
| `to_summary_html` | 汇总报表 | `{"file":"summary.html"}` |

## 🤝 贡献指南

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add amazing feature'`)
4. 推送分支 (`git push origin feature/amazing-feature`)
5. 创建 Pull Request

## 📄 许可证

本项目基于 [MIT License](LICENSE) 开源。

## 🙏 致谢

感谢所有为 StarL3 做出贡献的开发者！

---

<p align="center">
  <b>StarL3</b> - 让数据处理更简单
</p>
