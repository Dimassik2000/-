import requests
from bs4 import BeautifulSoup

html = requests.get('https://armisgun.ru/427-pcp').text

soup = BeautifulSoup(html)

for product in soup.find_all('div',class_='product-container'):
    print('Название: ', product.find('a', class_='product-name').text,
          end=' Цена: ')
    print(product.find('span',class_='price product-price').text)
