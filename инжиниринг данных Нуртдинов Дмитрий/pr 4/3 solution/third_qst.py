import json
import pprint
import pickle
import sqlite3
def load_json_data(file_name):

    with open(file_name, 'r', encoding='utf-8', newline='\n') as file:
        text = file.readlines()
        json_text = ""
        for row in text:
            json_text += row

        items = json.loads(json_text)
        for item in items:
            item.pop('danceability')
            item.pop('explicit')


        return items

def load_pkl_data(file_name):
    with open(file_name, 'rb') as file:
        data = pickle.load(file)
    return data

def remove_fields_from_data(data, fields_to_remove):
    for item in data:
        for field in fields_to_remove:
            item.pop(field, None)
    return data

def connect_to_db(file_name):
    connection = sqlite3.connect(file_name)
    connection.row_factory = sqlite3.Row
    return connection

def insert_comment_data(db, data):
    cursor = db.cursor()
    cursor.executemany("""
           INSERT INTO music (artist, song, duration_ms, year, tempo, genre, popularity)
           VALUES(
               :artist, :song, :duration_ms, :year, :tempo, :genre, :popularity
           )""", data)
    db.commit()

def write_sorted_data_to_json(db, field, file_name):
    cursor = db.cursor()
    cursor.execute(f"SELECT * FROM music ORDER BY {field} LIMIT 30")
    rows = cursor.fetchall()
    data = []
    for row in rows:
        item = dict(row)
        data.append(item)

    with open(file_name, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

def write_field_statistics(db, field):
    cursor = db.cursor()
    cursor.execute(f"SELECT SUM({field}), MIN({field}), MAX({field}), AVG({field}) FROM music")
    result = cursor.fetchone()
    statistics = {
        "sum": result[0],
        "min": result[1],
        "max": result[2],
        "avg": result[3]
    }

    pprint.pprint(statistics)

def write_categorical_field_frequency(db, field):
    cursor = db.cursor()
    cursor.execute(f"SELECT {field}, COUNT(*) as count FROM music GROUP BY {field}")
    rows = cursor.fetchall()
    frequencies = {}
    for row in rows:
        frequencies[row[0]] = row[1]

    pprint.pprint(frequencies)

def write_filtered_sorted_data_to_json(db, field, predicate, file_name):
    cursor = db.cursor()
    cursor.execute(f"SELECT * FROM music WHERE {predicate} ORDER BY {field} LIMIT 35")
    rows = cursor.fetchall()
    data = []
    for row in rows:
        item = dict(row)
        data.append(item)

    with open(file_name, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)


fields_to_remove = ['acousticness', 'energy']

pkl_data = load_pkl_data('task_3_var_20_part_1.pkl')

modified_data = remove_fields_from_data(pkl_data, fields_to_remove)

items = modified_data + load_json_data("task_3_var_20_part_2.json")

db = connect_to_db('third_db.db')
insert_comment_data(db, items)


# Задача 1: Вывод первых 30 отсортированных строк в файл формата json
write_sorted_data_to_json(db, "genre", "sorted_data.json")

# Задача 2: Вывод статистики по произвольному числовому полю
write_field_statistics(db, "duration_ms")

# Задача 3: Вывод частоты встречаемости для категориального поля
write_categorical_field_frequency(db, "artist")

# Задача 4: Вывод первых 35 отфильтрованных и отсортированных строк в файл формата json
write_filtered_sorted_data_to_json(db, "year", "year > 2000", "filtered_sorted_data.json")






