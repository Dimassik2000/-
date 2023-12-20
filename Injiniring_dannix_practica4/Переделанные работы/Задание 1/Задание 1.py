import sqlite3
import msgpack
import json
from tabulate import tabulate


def create_database():
    conn = sqlite3.connect('baza_dannix.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS baza_dannix
                      (author TEXT, genre TEXT, isbn TEXT, pages INTEGER, published_year INTEGER,
                       rating REAL, title TEXT, views INTEGER)''')
    conn.commit()
    conn.close()


def populate_database_from_msgpack(filename):
    conn = sqlite3.connect('baza_dannix.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    with open(filename, 'rb') as file:
        data = msgpack.load(file, raw=False)
        sql_query = "INSERT INTO baza_dannix VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        cursor.executemany(sql_query, [(book['author'], book['genre'], book['isbn'], book['pages'],
                                        book['published_year'], book['rating'], book['title'],
                                        book['views']) for book in data])
        conn.commit()
    conn.close()


def display_database_contents():
    conn = sqlite3.connect('baza_dannix.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM baza_dannix")
    rows = cursor.fetchall()
    headers = rows[0].keys() if rows else []
    print(tabulate(rows, headers=headers, tablefmt="fancy_grid"))
    conn.close()


def export_to_json(var):
    conn = sqlite3.connect('baza_dannix.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    num_field = 'pages'
    cursor.execute(f"SELECT * FROM baza_dannix ORDER BY {num_field} LIMIT {var + 10}")
    rows = cursor.fetchall()
    headers = rows[0].keys() if rows else []
    data = [dict(row) for row in rows]
    with open(r'output.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
    conn.close()


def calculate_aggregates_and_export_to_json():
    conn = sqlite3.connect('baza_dannix.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    num_field = 'pages'
    cursor.execute(f"SELECT SUM({num_field}), MIN({num_field}), MAX({num_field}), AVG({num_field}) FROM baza_dannix")
    result = cursor.fetchone()
    data = {
        "Sum": result[0],
        "Min": result[1],
        "Max": result[2],
        "Average": result[3]
    }
    with open(r'aggregates_output.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
    conn.close()


def categorical_field_frequency_and_export_to_json(cat_field):
    conn = sqlite3.connect('baza_dannix.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(f"SELECT {cat_field}, COUNT({cat_field}) FROM baza_dannix GROUP BY {cat_field}")
    results = cursor.fetchall()
    data = {row[cat_field]: row['COUNT({})'.format(cat_field)] for row in results}
    with open(r'categorical_frequency_output.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
    conn.close()


def export_filtered_data_to_json(var, filter_predicate):
    conn = sqlite3.connect('baza_dannix.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    num_field = 'pages'
    cursor.execute(f"SELECT * FROM baza_dannix WHERE {filter_predicate} ORDER BY {num_field} LIMIT {var + 10}")
    rows = cursor.fetchall()
    headers = rows[0].keys() if rows else []
    data = [dict(row) for row in rows]
    with open(r'filtered_output.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
    conn.close()


create_database()
populate_database_from_msgpack('task_1_var_34_item.msgpack')
display_database_contents()
export_to_json(34)
calculate_aggregates_and_export_to_json()
categorical_field_frequency_and_export_to_json('genre')
export_filtered_data_to_json(34, 'pages > 100')
