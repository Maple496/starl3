"""大文件压力测试"""
import pandas as pd
import os
from datetime import datetime

source_dir = r'F:\Projects\starl3\tests\projects\1_PR\test_data\source'

def clean_source_dir():
    for f in os.listdir(source_dir):
        if f.endswith('.xlsx'):
            try:
                os.remove(os.path.join(source_dir, f))
            except:
                pass

# 清理
clean_source_dir()

# 创建基础数据
data = {
    '请求项目': list(range(1, 101)),
    '采购申请': [f'PR{i:05d}' for i in range(100)],
    '短文本': [f'Test_{i}' for i in range(100)],
    '物料': [f'M{i:03d}' for i in range(100)],
    '项目文本': [f'Desc_{i}' for i in range(100)],
    '申请数量': list(range(100)),
    '单位': ['PC'] * 100,
    '工厂': ['1000'] * 100,
    '采购组': [f'{i%5+1:03d}' for i in range(100)],
    '物料组': [1000, 2000, 3000, 4000, 5000] * 20,
    '申请者': ['张三', '李四', '王五', '赵六', '钱七'] * 20,
    '已创建的': ['20240301'] * 100,
    '现有库存': [0] * 100,
    '需求日期': ['20240315'] * 100,
    '是否驳回': ['No'] * 100,
    '采购订单': [f'PO{i:03d}' if i % 3 == 0 else '' for i in range(100)],
    'PO日期': [f'2024-03-{10+i%10:02d}' if i % 3 == 0 else '' for i in range(100)],
}
import pandas as pd
df_base = pd.DataFrame(data)

print("=== 大文件压力测试 ===")

# 1. 5000行 x 10个文件 = 50000行
print("\n[Test 1] 大文件批处理测试 (10个文件，每个约5000行)")
for i in range(10):
    df = df_base.copy()
    # 复制到5000行
    for _ in range(6):
        df = pd.concat([df, df], ignore_index=True)
    df = df.head(5000)
    # 重新编号
    for idx in range(len(df)):
        df.loc[idx, '采购申请'] = f'PR2026B{i:02d}{idx:04d}'
    
    fname = f'purchase_applyInfo_batch_{i:02d}.xlsx'
    df.to_excel(os.path.join(source_dir, fname), index=False)
    os.utime(os.path.join(source_dir, fname), None)
    size = os.path.getsize(os.path.join(source_dir, fname)) / 1024
    print(f"  {fname}: {size:.1f} KB, {len(df)} rows")

# 2. 超大文件测试（接近20000KB）
print("\n[Test 2] 超大文件测试（接近20000KB上限）")
df_huge = df_base.copy()
# 大量复制以达到接近20000KB
for i in range(12):  # 2^12 = 4096倍
    df_huge = pd.concat([df_huge, df_huge], ignore_index=True)
    if len(df_huge) > 200000:  # 限制最大行数
        break

for idx in range(len(df_huge)):
    df_huge.loc[idx, '采购申请'] = f'PR2026H{idx:06d}'

df_huge.to_excel(os.path.join(source_dir, 'purchase_applyInfo_huge.xlsx'), index=False)
os.utime(os.path.join(source_dir, 'purchase_applyInfo_huge.xlsx'), None)
size = os.path.getsize(os.path.join(source_dir, 'purchase_applyInfo_huge.xlsx')) / 1024
print(f"  purchase_applyInfo_huge.xlsx: {size:.1f} KB, {len(df_huge)} rows")

print("\n=== 大文件测试准备完成 ===")
print(f"Total files: {len([f for f in os.listdir(source_dir) if f.endswith('.xlsx')])}")
