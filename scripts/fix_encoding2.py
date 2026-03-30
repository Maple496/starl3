import json

# 以二进制方式读取
with open(r'C:\Users\Administrator\Music\elt_purchase.json', 'rb') as f:
    data = f.read()

print(f'文件大小: {len(data)} bytes')
print(f'原始前100字节: {data[:100]}')
print()

# 检查控制字符
print('检查控制字符:')
for i, b in enumerate(data[:100]):
    if b < 32:
        char = chr(b) if b >= 9 else "???"
        print(f'  位置 {i}: 0x{b:02x} ({char})')

# 手动清理
# 移除 BOM
if data.startswith(b'\xef\xbb\xbf'):
    data = data[3:]

# 只保留可打印字符和允许的空白字符
allowed = set([9, 10, 13])  # tab, LF, CR
cleaned = bytearray()
for b in data:
    if b >= 32 or b in allowed:
        cleaned.append(b)

# 解码
text = cleaned.decode('utf-8', errors='ignore')
print(f'\n清理后长度: {len(text)}')

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
    # 找到第3行
    lines = text.split('\n')
    if len(lines) >= 3:
        print(f'第3行: {repr(lines[2])}')
