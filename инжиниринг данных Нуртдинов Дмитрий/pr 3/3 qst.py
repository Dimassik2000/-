from bs4 import BeautifulSoup
import json
import numpy as np
from collections import Counter
def handle_file(file_name):
    with open(file_name, encoding='utf-8') as file:
        text = ''
        for row in file.readlines():
            text += row

        site = BeautifulSoup(text, 'xml')
        item = dict()
        item['name'] = site.find_all('name')[0].get_text().strip()
        item['constellation'] = site.find_all('constellation')[0].get_text().strip()
        item['spectral-class'] = site.find_all('spectral-class')[0].get_text().strip()
        item['radius'] = site.find_all('radius')[0].get_text().strip()
        item['rotation'] = site.find_all('rotation')[0].get_text().strip()
        item['age'] = site.find_all('age')[0].get_text().strip()
        item['distance'] = site.find_all('distance')[0].get_text().strip()
        item['absolute-magnitude'] = site.find_all('absolute-magnitude')[0].get_text().strip()

        return item

handle_file('3 задание/1.xml')

items = []
for i in range(1,500):
    file_name = f'3 задание/{i}.xml'
    result = handle_file(file_name)
    items.append(result)
    if i<100:
        print(result)

items = sorted (items, key=lambda x: x['absolute-magnitude'], reverse=True)

with open('result_all_3.json', 'w', encoding='utf-8') as f:
    f.write(json.dumps(items))

filtered_items = []
for star in items:
    if int(star['radius']) >= 30000:
        filtered_items.append(star)

print(len(items))
print(len(filtered_items))

dtype = [('radius', 'U10'),('name', 'U10')]

star_name = [item['name'] for item in items]
star_name_freq = Counter(star_name)

items = np.array([int(item['radius']) for item in items] ,dtype=np.int32)

max_value = np.nanmax(items)
min_value = np.nanmin(items)
mean_value = round(np.nanmean(items), 2)
sum_value = np.nansum(items)
std_value = round(np.nanstd(items), 1)

print('Максимальные значения:', max_value)
print('Минимальные значения:', min_value)
print('Среднее арифметическое:', mean_value)
print('Сумма:', sum_value)
print('Стандартное отклонение:', std_value)


print('\nЧастота встречаемости слов в поле book-wrapper:')
print(star_name_freq)