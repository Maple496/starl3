import json

with open(r'C:\Users\Administrator\Music\elt_purchase.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Find and fix filter_files row
for i, row in enumerate(data):
    if 'filter_files' in row:
        params = json.loads(row[3])
        for cond in params['conditions']:
            if cond['field'] == 'name':
                print(f"Old pattern: {cond['value']}")
                # Fix: \\. should be \. (match literal dot, not backslash)
                cond['value'] = r'purchase.+\.xlsx$'
                print(f"New pattern: {cond['value']}")
        row[3] = json.dumps(params, ensure_ascii=False)
        break

# Write back
with open(r'C:\Users\Administrator\Music\elt_purchase.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print('Fixed!')
