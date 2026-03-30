"""
StarL3 ELT Pipeline 压力测试和健壮性测试
"""
import pandas as pd
import os
import numpy as np
from datetime import datetime

source_dir = r'F:\Projects\starl3\tests\projects\1_PR\test_data\source'

def clean_source_dir():
    """清理测试目录"""
    for f in os.listdir(source_dir):
        if f.endswith('.xlsx'):
            try:
                os.remove(os.path.join(source_dir, f))
            except:
                pass

def create_base_data(rows=100):
    """创建基础数据"""
    data = {
        '请求项目': list(range(1, rows+1)),
        '采购申请': [f'PR2026{i:06d}' for i in range(rows)],
        '短文本': [f'Test物料_{i}' for i in range(rows)],
        '物料': [f'M{i:03d}' for i in range(rows)],
        '项目文本': [f'Desc_{i}' for i in range(rows)],
        '申请数量': np.random.randint(1, 1000, rows),
        '单位': ['PC'] * rows,
        '工厂': ['1000'] * rows,
        '采购组': [f'{i%5+1:03d}' for i in range(rows)],
        '物料组': [f'{i%5+1}000' for i in range(rows)],
        '申请者': [f'User{i%10}' for i in range(rows)],
        '已创建的': ['20240301'] * rows,
        '现有库存': np.random.randint(0, 100, rows),
        '需求日期': [f'2024{(i%12)+1:02d}{(i%28)+1:02d}' for i in range(rows)],
        '是否驳回': ['No'] * rows,
        '采购订单': [f'PO{i:06d}' if i % 3 == 0 else '' for i in range(rows)],
        'PO日期': [f'2024-{(i%12)+1:02d}-{(i%28)+1:02d}' if i % 3 == 0 else '' for i in range(rows)],
    }
    return pd.DataFrame(data)

# ==================== 测试场景 ====================

print("=== 压力测试和健壮性测试 ===")
print(f"Time: {datetime.now()}")
print()

# 1. 超大文件测试 (接近 20000KB 上限)
print("[Test 1] 超大文件测试 (接近20000KB)")
clean_source_dir()
df = create_base_data(50000)  # 50000行应该接近20000KB
df.to_excel(os.path.join(source_dir, 'purchase_applyInfo_huge.xlsx'), index=False)
os.utime(os.path.join(source_dir, 'purchase_applyInfo_huge.xlsx'), None)
size = os.path.getsize(os.path.join(source_dir, 'purchase_applyInfo_huge.xlsx')) / 1024
print(f"Created: {size:.1f} KB, {len(df)} rows")

# 2. 多个中等文件测试
print("\n[Test 2] 多个文件测试 (10个文件)")
clean_source_dir()
for i in range(10):
    df = create_base_data(100)
    fname = f'purchase_applyInfo_multi_{i:02d}.xlsx'
    df.to_excel(os.path.join(source_dir, fname), index=False)
    os.utime(os.path.join(source_dir, fname), None)
print(f"Created: 10 files")

# 3. 空数据测试（表头但无数据行）
print("\n[Test 3] 空数据测试")
df_empty = create_base_data(0)  # 只有表头
# 添加大量空列使文件达到100KB
for col in range(1000):
    df_empty[f'EmptyCol_{col}'] = []
df_empty.to_excel(os.path.join(source_dir, 'purchase_applyInfo_empty_data.xlsx'), index=False)
# 填充到100KB
with open(os.path.join(source_dir, 'purchase_applyInfo_empty_data.xlsx'), 'ab') as f:
    f.write(b' ' * (105*1024))
os.utime(os.path.join(source_dir, 'purchase_applyInfo_empty_data.xlsx'), None)
size = os.path.getsize(os.path.join(source_dir, 'purchase_applyInfo_empty_data.xlsx')) / 1024
print(f"Created: {size:.1f} KB, 0 data rows")

# 4. 极端列数测试
print("\n[Test 4] 极端列数测试 (1000列)")
df_many_cols = create_base_data(100)
for col in range(1000):
    df_many_cols[f'ExtraCol_{col}'] = f'Value_{col}'
df_many_cols.to_excel(os.path.join(source_dir, 'purchase_applyInfo_many_cols.xlsx'), index=False)
os.utime(os.path.join(source_dir, 'purchase_applyInfo_many_cols.xlsx'), None)
size = os.path.getsize(os.path.join(source_dir, 'purchase_applyInfo_many_cols.xlsx')) / 1024
print(f"Created: {size:.1f} KB, {len(df_many_cols.columns)} columns")

# 5. 重复数据测试
print("\n[Test 5] 重复数据测试")
df_dup = create_base_data(100)
# 创建完全重复的行
df_dup = pd.concat([df_dup, df_dup, df_dup, df_dup, df_dup], ignore_index=True)
df_dup.to_excel(os.path.join(source_dir, 'purchase_applyInfo_dup.xlsx'), index=False)
os.utime(os.path.join(source_dir, 'purchase_applyInfo_dup.xlsx'), None)
size = os.path.getsize(os.path.join(source_dir, 'purchase_applyInfo_dup.xlsx')) / 1024
print(f"Created: {size:.1f} KB, {len(df_dup)} rows (many duplicates)")

# 6. 特殊字符混合测试
print("\n[Test 6] 特殊字符混合测试")
df_special = create_base_data(500)
df_special['短文本'] = [
    f'Test_{i}\nLine2\tTab\"Quote"🔥Emoji' 
    for i in range(len(df_special))
]
df_special.to_excel(os.path.join(source_dir, 'purchase_applyInfo_special_chars.xlsx'), index=False)
os.utime(os.path.join(source_dir, 'purchase_applyInfo_special_chars.xlsx'), None)
size = os.path.getsize(os.path.join(source_dir, 'purchase_applyInfo_special_chars.xlsx')) / 1024
print(f"Created: {size:.1f} KB with special chars")

# 7. 数值异常测试
print("\n[Test 7] 数值异常测试")
df_num = create_base_data(105)  # 改为105行匹配数据
df_num['申请数量'] = [999999999, -1, 0, None, float('inf'), 1e10, -1e10] * 15  # 异常数值
df_num.to_excel(os.path.join(source_dir, 'purchase_applyInfo_num_anomaly.xlsx'), index=False)
os.utime(os.path.join(source_dir, 'purchase_applyInfo_num_anomaly.xlsx'), None)
print(f"Created: numeric anomaly test file")

# 8. 日期格式混乱测试
print("\n[Test 8] 日期格式混乱测试")
df_date = create_base_data(90)  # 90行
df_date['需求日期'] = [
    '2024-01-01', '01/01/2024', '20240101', 'Jan 1 2024', 
    'Invalid', '', None, '24/13/45', '2024-02-30'
] * 10  # 90个元素
df_date.to_excel(os.path.join(source_dir, 'purchase_applyInfo_date_mess.xlsx'), index=False)
os.utime(os.path.join(source_dir, 'purchase_applyInfo_date_mess.xlsx'), None)
print(f"Created: date format chaos test file")

# 9. 超长字符串测试
print("\n[Test 9] 超长字符串测试")
df_long = create_base_data(50)
df_long['短文本'] = ['A' * 1000 for _ in range(len(df_long))]  # 1000字符
df_long['项目文本'] = ['B' * 5000 for _ in range(len(df_long))]  # 5000字符
df_long.to_excel(os.path.join(source_dir, 'purchase_applyInfo_long_text.xlsx'), index=False)
os.utime(os.path.join(source_dir, 'purchase_applyInfo_long_text.xlsx'), None)
print(f"Created: long text test file")

# 10. 文件大小边界测试
print("\n[Test 10] 文件大小边界测试")
# 99KB (刚好低于100KB下限)
df_small = create_base_data(50)
df_small.to_excel(os.path.join(source_dir, 'purchase_applyInfo_99KB.xlsx'), index=False)
size = os.path.getsize(os.path.join(source_dir, 'purchase_applyInfo_99KB.xlsx')) / 1024
print(f"Created: {size:.1f} KB (should be filtered)")

# 101KB (刚好高于100KB下限)
with open(os.path.join(source_dir, 'purchase_applyInfo_101KB.xlsx'), 'wb') as f:
    f.write(b' ' * 10)  # Placeholder
df_small.to_excel(os.path.join(source_dir, 'purchase_applyInfo_101KB.xlsx'), index=False)
with open(os.path.join(source_dir, 'purchase_applyInfo_101KB.xlsx'), 'ab') as f:
    f.write(b'X' * int(101*1024 - os.path.getsize(os.path.join(source_dir, 'purchase_applyInfo_101KB.xlsx'))))
os.utime(os.path.join(source_dir, 'purchase_applyInfo_101KB.xlsx'), None)
size = os.path.getsize(os.path.join(source_dir, 'purchase_applyInfo_101KB.xlsx')) / 1024
print(f"Created: {size:.1f} KB (should pass filter)")

print("\n=== 所有测试文件创建完成 ===")
print(f"Total files: {len([f for f in os.listdir(source_dir) if f.endswith('.xlsx')])}")
