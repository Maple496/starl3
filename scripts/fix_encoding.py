import json
import re

# 以二进制方式读取
with open(r'C:\Users\Administrator\Music\elt_purchase.json', 'rb') as f:
    data = f.read()

# 移除 BOM
if data.startswith(b'\xef\xbb\xbf'):
    data = data[3:]

# 解码，忽略错误
text = data.decode('utf-8', errors='ignore')

# 移除所有控制字符（保留 \n, \r, \t）
def clean_control_chars(s):
    result = []
    for ch in s:
        code = ord(ch)
        if code < 32:
            if code in (9, 10, 13):  # tab, LF, CR
                result.append(ch)
            # 其他控制字符丢弃
        else:
            result.append(ch)
    return ''.join(result)

text = clean_control_chars(text)

# 尝试解析
try:
    parsed = json.loads(text)
    print(f'成功解析，共有 {len(parsed)} 个步骤')
    
    # 写回干净的文件
    with open(r'C:\Users\Administrator\Music\elt_purchase.json', 'w', encoding='utf-8') as f:
        json.dump(parsed, f, ensure_ascii=False, indent=2)
    print('已保存干净的JSON文件')
    
except Exception as e:
    print(f'解析错误: {e}')
    # 打印前500字符查看
    print(repr(text[:500]))
