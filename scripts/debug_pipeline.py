#!/usr/bin/env python
"""调试 Pipeline 执行过程"""
import os
import sys
import json
import pandas as pd

# 设置环境变量
os.environ['STARL3_DATA_DIR'] = r'F:\Projects\starl3\tests'
sys.path.insert(0, '.')

from core.registry import auto_discover, OpRegistry
from core.pipeline_engine import PipelineEngine
from core.context import create_context

# 重新加载 ops
OpRegistry._registry = {}
auto_discover()

# 加载配置
config_path = r'F:\Projects\starl3\tests\projects\1_PR\test_data\test_config.json'

# 创建上下文
ctx = create_context(base_dir=r'F:\Projects\starl3\tests').to_dict()
ctx['results'] = {}
ctx['last_result'] = None

# 手动执行前几步
print("=== 步骤1: 加载物料组分工 ===")
mapping_file = r'F:\Projects\starl3\tests\projects\1_PR\test_data\mapping.xlsx'
df1 = pd.read_excel(mapping_file, sheet_name='分工_物料组')
print(f"物料组 dtype: {df1['物料组'].dtype}")
print(df1.head())

ctx['results']['加载物料组分工'] = df1
ctx['last_result'] = df1

print("\n=== 步骤2: 物料组分工筛选 ===")
df2 = df1[['物料组', '资源开发']]
print(f"筛选后物料组 dtype: {df2['物料组'].dtype}")
ctx['results']['物料组分工筛选'] = df2
ctx['last_result'] = df2

print("\n=== 步骤3: 加载采购申请数据 ===")
purchase_file = r'F:\Projects\starl3\tests\projects\1_PR\test_data\source\purchase_test.xlsx'
df3 = pd.read_excel(purchase_file)
df3 = df3[['请求项目', '采购申请', '短文本', '物料', '项目文本', '申请数量', '单位', '工厂', '采购组', '物料组', '申请者', '已创建的', '现有库存', '需求日期', '是否驳回', '采购订单', 'PO日期']]
print(f"采购数据物料组 dtype: {df3['物料组'].dtype}")
print(df3[['物料组', '采购申请']].head())
ctx['results']['加载采购申请数据'] = df3
ctx['last_result'] = df3

print("\n=== 步骤4: 执行 join 操作 ===")
# 获取 join 操作
join_op = OpRegistry.get_op_map()['join']

params = {
    "source": "物料组分工筛选",
    "on": "物料组",
    "how": "left"
}

print(f"Join 前 - 主数据物料组 dtype: {ctx['last_result']['物料组'].dtype}")
print(f"Join 前 - source 物料组 dtype: {ctx['results']['物料组分工筛选']['物料组'].dtype}")

result = join_op(ctx, params)

print(f"\nJoin 后资源开发列:")
print(result['资源开发'].value_counts(dropna=False))
