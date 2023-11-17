from bs4 import BeautifulSoup
import json
import numpy as np
from collections import Counter
def handle_file(file_name):
    items = list()
    with open(file_name, encoding='utf-8') as file:
        text = ''
        for row in file.readlines():
            text += row

        site = BeautifulSoup(text, 'html.parser')
        products = site.find_all('div', attrs={'class': 'product-item'})
        product = products[0]
        # print(len(products))

        for product in products:
            item = dict()
            item['id'] = product.a['data-id']
            item['link'] = product.find_all('a')[1]['href']
            item['img_url'] = product.find_all('img')[0]['src']
            item['title'] = product.find_all('span')[0].get_text().strip()
            item['price'] = int(product.price.get_text().replace('₽', '').replace(' ', '').strip())
            item['bonus'] = int(product.strong.get_text().replace('+ начислим ', '').replace(' бонусов', ''))

            props = product.ul.find_all('li')
            for prop in props:
                item[prop['type']] = prop.get_text().strip()

            items.append(item)
    return  items

handle_file('2 задание/1.html')

items = []
for i in range(1,91):
    file_name = f'2 задание/{i}.html'
    items += handle_file(file_name)

items = sorted(items, key=lambda x: x['price'], reverse=True)

with open('result_all_2.json', 'w', encoding='utf-8') as f:
    f.write(json.dumps(items))

filtered_items = []
for phone in items:
    if phone ['price'] >= int('12000'):
        filtered_items.append(phone)

print(len(items))
print(len(filtered_items))

dtype = [('id', 'U10'),('title', 'U10')]

title_phone = [item['title'] for item in items]
title_phone_freq = Counter(title_phone)

items = np.array([int(item['id']) for item in items] ,dtype=np.int32)

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


print('\nЧастота встречаемости слов в поле title:')
print(title_phone_freq)
