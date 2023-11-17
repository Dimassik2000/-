from bs4 import BeautifulSoup
import re
import json
import numpy as np
from collections import Counter
def handle_file(file_name):
    with open (file_name, encoding='utf-8') as file:
        text = ''
        for row in file.readlines():
            text += row

        site = BeautifulSoup(text, 'html.parser')

        item = dict()

        item['book-wrapper'] = site.find_all('span', string=re.compile('Категория:'))[0].get_text().split(':')[1].strip()
        item['title'] = site.find_all('h1')[0].get_text().split(':')[0].strip()
        author = site.find_all('p')[0].get_text().split('\n')
        item['author'] = author[0].strip()
        item['pages'] = site.find_all('span', attrs={'class':'pages'})[0].get_text().split(':')[1].strip()
        item['year'] = int(site.find_all('span', attrs={'class': 'year'})[0].get_text().replace('Издано в', '').strip())
        item['ISBN'] = site.find_all('span', string=re.compile('ISBN:') )[0].get_text().split(':')[1].strip()
        item['image'] =site.find_all('img')[0]['src']
        item['rate'] = float(site.find_all('span', string=re.compile('Рейтинг:'))[0].get_text().split(':')[1].strip())
        item['views'] = int(site.find_all('span', string=re.compile('Просмотры:'))[0].get_text().split(':')[1].strip())

        return item

handle_file('1 задание/2.html')

items = []
for i in range(1,999):
    file_name = f'1 задание/{i}.html'
    result = handle_file(file_name)
    items.append(result)
    if i<100:
        print(result)

items = sorted (items, key=lambda x: x['views'], reverse=True)

with open('result_all_1.json', 'w', encoding='utf-8') as f:
    f.write(json.dumps(items))

filtered_items = []
for book in items:
    if book['book-wrapper'] != 'фэнтези':
        filtered_items.append(book)

print(len(items))
print(len(filtered_items))

dtype = [('year', 'U10'),('book-wrapper', 'U10')]

book_wrappers = [item['book-wrapper'] for item in items]
book_wrapper_freq = Counter(book_wrappers)

items = np.array([int(item['year']) for item in items] ,dtype=np.int32)

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
print(book_wrapper_freq)
