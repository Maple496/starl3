import pandas as pd
import os
from datetime import datetime
import numpy as np

source_dir = r'F:\Projects\starl3\tests\projects\1_PR\test_data\source'

# Clean directory
for f in os.listdir(source_dir):
    if f.endswith('.xlsx'):
        try:
            os.remove(os.path.join(source_dir, f))
        except:
            pass

# Create test data with special characters
data = {
    '请求项目': list(range(1, 11)),
    '采购申请': [f'PR2026{i:05d}' for i in range(10)],
    '短文本': [
        'Normal text',
        'Chinese: 中文测试',
        'New line here\nTest text',
        'Tab here\tTest text',
        'Quote "test" here',
        'Backslash \\ test',
        'HTML: <test> & more',
        'Unicode: αβγ δ ε',
        'Special: !@#$%^&*()',
        'Mixed: 中文\nEnglish\tTab'
    ],
    '物料': [f'M{i:03d}' for i in range(1, 11)],
    '项目文本': [
        'Line1\nLine2\nLine3',
        'Quote: "Hello" and \'World\'',
        'Backslash: C:\\Users\\Test',
        'HTML tags: <div>content</div>',
        'SQL injection: DROP TABLE; --',
        'JSON: {"key": "value"}',
        'CSV comma, and "quote"',
        'Tab\tseparated\tvalues',
        'Multiple\r\nlines\r\nhere',
        '中文和English混合'
    ],
    '申请数量': [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
    '单位': ['PC'] * 10,
    '工厂': ['1000'] * 10,
    '采购组': ['001', '002', '003', '004', '005', '001', '002', '003', '004', '005'],
    '物料组': [1000, 2000, 3000, 4000, 5000, 1000, 2000, 3000, 4000, 5000],
    '申请者': [
        '张三',
        'Li Si',
        'Wang Wu',
        'User\nName',
        'Test User',
        '中文用户',
        'Mixed 中文 English',
        'User "Nickname"',
        'Back\\Slash',
        'Final User'
    ],
    '已创建的': ['20240301'] * 10,
    '现有库存': [0] * 10,
    '需求日期': ['20240315'] * 10,
    '是否驳回': ['No', 'Yes', 'No', 'No\nPending', 'No', 'Yes', 'No', 'No', 'No', 'Yes'],
    '采购订单': ['PO001', '', 'PO003', '', 'PO005', 'PO006', '', 'PO008', '', 'PO010'],
    'PO日期': ['2024-03-10', '', '2024-03-12', '', '2024-03-14', '2024-03-15', '', '2024-03-17', '', '2024-03-19']
}

df = pd.DataFrame(data)

# Replicate to reach 100KB+ (need about 200x for 100KB)
df_large = pd.concat([df] * 300, ignore_index=True)
for i in range(len(df_large)):
    df_large.loc[i, '采购申请'] = f'PR2026{i:06d}'

filepath = os.path.join(source_dir, 'purchase_applyInfo_special.xlsx')
df_large.to_excel(filepath, index=False)
os.utime(filepath, None)

print(f'Created: purchase_applyInfo_special.xlsx')
print(f'Size: {os.path.getsize(filepath)/1024:.1f} KB')
print(f'Rows: {len(df_large)}')
print('\nSample special characters in data:')
print(df[['短文本', '项目文本']].head(5).to_string())
