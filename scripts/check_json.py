with open(r'C:\Users\Administrator\Music\elt_purchase.json', 'rb') as f:
    data = f.read()

print(f'文件大小: {len(data)} bytes')
print(f'前200字节 (hex): {data[:200].hex()}')
print()

# 检查是否有控制字符
for i, b in enumerate(data[:500]):
    if b < 32 and b not in (9, 10, 13):  # 不是tab, LF, CR
        print(f'发现控制字符: 0x{b:02x} 在位置 {i}')

# 尝试解码
print()
print('--- 尝试用 utf-8-sig 解码 ---')
try:
    text = data.decode('utf-8-sig')
    lines = text.split('\n')
    for i in range(min(10, len(lines))):
        print(f'Line {i+1}: {repr(lines[i][:100])}')
except Exception as e:
    print(f'错误: {e}')
