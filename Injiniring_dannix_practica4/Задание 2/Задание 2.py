import sqlite3
import json

conn = sqlite3.connect('baza_dannix.db')
cursor = conn.cursor()

filename = r'task_2_var_34_subitem.json'
with open(filename, 'r', encoding='utf-8') as file:
    data = json.load(file)

    cursor.execute('''CREATE TABLE IF NOT EXISTS subitems
                      (title TEXT REFERENCES baza_dannix (title), price INTEGER, place TEXT, date TEXT)''')

    for item in data:
        title = item['title']
        price = item['price']
        place = item['place']
        date = item['date']

        cursor.execute("INSERT INTO subitems VALUES (?, ?, ?, ?)", (title, price, place, date))

conn.commit()

cursor.execute("SELECT baza_dannix.title, subitems.price, subitems.place FROM baza_dannix JOIN subitems ON baza_dannix.title = subitems.title")
results = cursor.fetchall()

for result in results:
    print("Книга:", result[0])
    print("Цена:", result[1])
    print("Место продажи:", result[2])
    print("\n")

# Запрос 2: Вывод средней цены книг и количество их продаж по категориям
cursor.execute("SELECT baza_dannix.genre, AVG(subitems.price), COUNT(subitems.price) FROM baza_dannix JOIN subitems ON baza_dannix.title = subitems.title GROUP BY baza_dannix.genre")
results = cursor.fetchall()

for result in results:
    print("Категория:", result[0])
    print("Средняя цена:", result[1])
    print("Количество продаж:", result[2])
    print("\n")

# Запрос 3: Вывод информации о самой дорогой и самой дешевой книгах
cursor.execute("SELECT baza_dannix.title, subitems.price FROM baza_dannix JOIN subitems ON baza_dannix.title = subitems.title ORDER BY subitems.price ASC LIMIT 1")
cheapest_book = cursor.fetchone()

cursor.execute("SELECT baza_dannix.title, subitems.price FROM baza_dannix JOIN subitems ON baza_dannix.title = subitems.title ORDER BY subitems.price DESC LIMIT 1")
most_expensive_book = cursor.fetchone()

print("Самая дешевая книга:", cheapest_book[0])
print("Цена:", cheapest_book[1])
print("\n")
print("Самая дорогая книга:", most_expensive_book[0])
print("Цена:", most_expensive_book[1])

conn.close()

