import pandas as pd
import os
from datetime import datetime

source_dir = r'F:\Projects\starl3\tests\projects\1_PR\test_data\source'

# Clean directory
for f in os.listdir(source_dir):
    if f.endswith('.xlsx'):
        try:
            os.remove(os.path.join(source_dir, f))
        except:
            pass

# Create test data with Emoji (GBK cannot encode)
data = {
    '请求项目': list(range(1, 6)),
    '采购申请': [f'PR2026{i:05d}' for i in range(5)],
    '短文本': [
        'Normal text',
        'Chinese: 中文测试',
        'Emoji test: 😀 🎉 ✅ ❌',  # GBK cannot encode
        'Mixed: 中文 + 😀 + English',
        'Special: 🔥 💯 ⭐'
    ],
    '物料': [f'M{i:03d}' for i in range(1, 6)],
    '项目文本': [
        'Test 1',
        'Test 2',
        'Test with emoji: 🎊 🎁',
        'Test 4',
        'Test 5'
    ],
    '申请数量': [10, 20, 30, 40, 50],
    '单位': ['PC'] * 5,
    '工厂': ['1000'] * 5,
    '采购组': ['001', '002', '001', '002', '001'],
    '物料组': [1000, 2000, 1000, 2000, 1000],
    '申请者': ['张三', '李四', '王五', '赵六', '钱七'],
    '已创建的': ['20240301'] * 5,
    '现有库存': [0] * 5,
    '需求日期': ['20240315'] * 5,
    '是否驳回': ['No'] * 5,
    '采购订单': ['PO001', '', 'PO003', '', 'PO005'],
    'PO日期': ['2024-03-10', '', '2024-03-12', '', '2024-03-14']
}

df = pd.DataFrame(data)

# Replicate to reach 100KB+ (need ~400x for 100KB)
df_large = pd.concat([df] * 400, ignore_index=True)
for i in range(len(df_large)):
    df_large.loc[i, '采购申请'] = f'PR2026{i:06d}'

filepath = os.path.join(source_dir, 'purchase_applyInfo_emoji.xlsx')
df_large.to_excel(filepath, index=False)
os.utime(filepath, None)

print(f'Created: purchase_applyInfo_emoji.xlsx')
print(f'Size: {os.path.getsize(filepath)/1024:.1f} KB')
print(f'Rows: {len(df_large)}')
print('Contains Emoji: 😀 🎉 ✅ ❌ 🔥 💯 ⭐ (GBK cannot encode)')
