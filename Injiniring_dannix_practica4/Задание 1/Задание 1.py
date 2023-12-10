import sqlite3
import msgpack
import json
from tabulate import tabulate

conn = sqlite3.connect('baza_dannix.db')
cursor = conn.cursor()

cursor.execute('''CREATE TABLE IF NOT EXISTS baza_dannix
                  (author TEXT, genre TEXT, isbn TEXT, pages INTEGER, published_year INTEGER,
                   rating REAL, title TEXT, views INTEGER)''')

filename = r'task_1_var_34_item.msgpack'
with open(filename, 'rb') as file:
    data = msgpack.load(file, raw=False)

    for baza_dannix in data:
        author = baza_dannix['author']
        genre = baza_dannix['genre']
        isbn = baza_dannix['isbn']
        pages = baza_dannix['pages']
        published_year = baza_dannix['published_year']
        rating = baza_dannix['rating']
        title = baza_dannix['title']
        views = baza_dannix['views']

        cursor.execute("INSERT INTO baza_dannix VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (author, genre, isbn, pages, published_year, rating, title, views))

conn.commit()
conn.close()

conn = sqlite3.connect('baza_dannix.db')
cursor = conn.cursor()

cursor.execute("SELECT * FROM baza_dannix")
rows = cursor.fetchall()

headers = [description[0] for description in cursor.description]

print(tabulate(rows, headers=headers, tablefmt="fancy_grid"))

# 1. Выполнение SQL-запроса для выборки VAR+10 отсортированных строк
var = 34
num_field = 'pages'
cursor.execute(f"SELECT * FROM baza_dannix ORDER BY {num_field} LIMIT {var + 10}")
rows = cursor.fetchall()

headers = [description[0] for description in cursor.description]

data = []
for row in rows:
    data.append(dict(zip(headers, row)))

with open(r'output.json', 'w', encoding='utf-8') as file:
    json.dump(data, file, indent=4, ensure_ascii=False)

conn.close()

# Сумма, минимум, максимум, среднее
print("Сумма, минимум, максимум, среднее: ")
conn = sqlite3.connect('baza_dannix.db')
cursor = conn.cursor()

num_field = 'pages'
cursor.execute(f"SELECT SUM({num_field}), MIN({num_field}), MAX({num_field}), AVG({num_field}) FROM baza_dannix")
result = cursor.fetchone()

print(f"Сумма: {result[0]}")
print(f"Минимум: {result[1]}")
print(f"Максимум: {result[2]}")
print(f"Среднее: {result[3]}")
print("\n")

conn.close()

# Частота встречаемости для категориального поля
print("Частота встречаемости для категориального поля:")

conn = sqlite3.connect('baza_dannix.db')
cursor = conn.cursor()

cat_field = 'genre'  # категориального поля
cursor.execute(f"SELECT {cat_field}, COUNT({cat_field}) FROM baza_dannix GROUP BY {cat_field}")
results = cursor.fetchall()

for result in results:
    print(f"{result[0]}: {result[1]}")

# Отфильтрованные данные
var = 34
num_field = 'pages'
filter_predicate = f"{num_field} > 100"
cursor.execute(f"SELECT * FROM baza_dannix WHERE {filter_predicate} ORDER BY {num_field} LIMIT {var + 10}")
rows = cursor.fetchall()

headers = [description[0] for description in cursor.description]

data = []
for row in rows:
    data.append(dict(zip(headers, row)))

with open(r'filtered_output.json', 'w', encoding='utf-8') as file:
    json.dump(data, file, indent=4, ensure_ascii=False)
