import sqlite3
import json
import csv

conn = sqlite3.connect('baza_dannix.db')
cursor = conn.cursor()

cursor.execute('''CREATE TABLE IF NOT EXISTS songs
                  (artist TEXT, song TEXT, duration_ms INTEGER, year INTEGER, tempo REAL, genre TEXT, energy REAL, key INTEGER, loudness REAL)''')

filename1 = r'task_3_var_34_part_1.csv'
with open(filename1, 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';')
    for row in reader:
        artist = row['artist']
        song = row['song']
        duration_ms = int(row['duration_ms'])
        year = int(row['year'])
        tempo = float(row['tempo'])
        genre = row['genre']
        energy = float(row['energy'])
        key = int(row['key'])
        loudness = float(row['loudness'])

        cursor.execute("INSERT INTO songs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       (artist, song, duration_ms, year, tempo, genre, energy, key, loudness))

# Запись данных из файла task_3_var_34_part_2.json
filename2 = r'task_3_var_34_part_2.json'
with open(filename2, 'r', encoding='utf-8') as jsonfile:
    data = json.load(jsonfile)
    for item in data:
        artist = item['artist']
        song = item['song']
        duration_ms = int(item['duration_ms'])
        year = int(item['year'])
        tempo = float(item['tempo'])
        genre = item['genre']
        energy = 0.0
        key = 0
        loudness = 0.0

        cursor.execute("INSERT INTO songs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       (artist, song, duration_ms, year, tempo, genre, energy, key, loudness))

conn.commit()
conn.close()

conn = sqlite3.connect('baza_dannix.db')
cursor = conn.cursor()

# Запрос 1: вывод первых (VAR+10) отсортированных строк из таблицы в файл формата json
VAR = 34
sort_field = 'duration_ms'  # Произвольное числовое поле

query1 = f"SELECT * FROM songs ORDER BY {sort_field} LIMIT {VAR + 10}"
cursor.execute(query1)
rows1 = cursor.fetchall()

headers1 = [description[0] for description in cursor.description]

data1 = []
for row in rows1:
    data1.append(dict(zip(headers1, row)))

filename1 = r"output1.json"
with open(filename1, 'w', encoding='utf-8') as outfile:
    json.dump(data1, outfile, indent=4, ensure_ascii=False)

# Запрос 2: вывод (сумму, мин, макс, среднее) по произвольному числовому полю
print("Сумма, минимум, максимум, среднее: ")
numeric_field = 'tempo'  # Произвольное числовое поле

query2 = f"SELECT SUM({numeric_field}), MIN({numeric_field}), MAX({numeric_field}), AVG({numeric_field}) FROM songs"
cursor.execute(query2)
result2 = cursor.fetchone()

sum_value, min_value, max_value, avg_value = result2

print("Сумма:", sum_value)
print("Минимум:", min_value)
print("Максимум:", max_value)
print("Среднее:", avg_value)
print("\n")
# Запрос 3: вывод частоты встречаемости для категориального поля
categorical_field = 'genre'  # Произвольное категориальное поле

query3 = f"SELECT {categorical_field}, COUNT({categorical_field}) AS frequency FROM songs GROUP BY {categorical_field}"
cursor.execute(query3)
rows3 = cursor.fetchall()

print("Частота встречаемости для категориального поля:")
for row in rows3:
    print(row[0], ":", row[1])

# Запрос 4: вывод первых (VAR+15) отфильтрованных отсортированных строк из таблицы в файл формата json
VAR = 34
filter_predicate = "energy > 0.5"  # Произвольный предикат
sort_field = 'year'  # Произвольное числовое поле

query4 = f"SELECT * FROM songs WHERE {filter_predicate} ORDER BY {sort_field} LIMIT {VAR + 15}"
cursor.execute(query4)
rows4 = cursor.fetchall()

headers4 = [description[0] for description in cursor.description]

data4 = []
for row in rows4:
    data4.append(dict(zip(headers4, row)))

filename4 = r"output4.json"
with open(filename4, 'w', encoding='utf-8') as outfile:
    json.dump(data4, outfile, indent=4, ensure_ascii=False)

conn.close()
