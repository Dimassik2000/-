import json
import msgpack
import os

file = r'products_34.json' # укажите свой путь
file_output = r'Output3' # укажите свой путь

with open(file, 'r') as f:
    data = json.load(f)

result = {}
for item in data:
    name = item['name']
    price = item['price']
    if name in result:
        result[name]['count'] += 1
        result[name]['total_price'] += price
        if price > result[name]['max_price']:
            result[name]['max_price'] = price
        if price < result[name]['min_price']:
            result[name]['min_price'] = price
    else:
        result[name] = {
            'count': 1,
            'total_price': price,
            'max_price': price,
            'min_price': price
        }

for name, stats in result.items():
    stats['avg_price'] = stats['total_price'] / stats['count']

with open(file_output + r'\result.json', 'w', encoding='utf-8') as f:
    json.dump(result, f, ensure_ascii=False, indent=4)

with open(file_output + r'\result.msgpack', 'wb') as f:
    packed = msgpack.packb(result)
    f.write(packed)

print("Размер файла result.json:", os.path.getsize(file_output + r'\result.json'), "байт")
print("Размер файла result.msgpack:", os.path.getsize(file_output + r'\result.msgpack'), "байт")