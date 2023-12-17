import csv
import sqlite3
import json


def load_csv(filename):
    items = list()
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=';')
        next(reader)
        for row in reader:
            if len(row) == 0:
                continue
            item = {
                'id': row[0],
                'name': row[1],
                'city': row[2],
                'begin': row[3],
                'system': row[4],
                'tours_count': int(row[5]),
                'min_rating': int(row[6]),
                'time_on_game': int(row[7])
            }
            items.append(item)
    return items


def connect_to_db(filename):
    connection = sqlite3.connect(filename)
    connection.row_factory = sqlite3.Row
    return connection


def insert_data(db, data):
    cursor = db.cursor()

    cursor.executemany("""
        INSERT INTO building (name, city, begin, system, tours_count, min_rating, time_on_game)
        VALUES(
            :name, :city, :begin, :system,
            :tours_count, :min_rating, :time_on_game
        )
    """, data)

    db.commit()


def get_top_by_time_on_range(db, limit):
    cursor = db.cursor()

    res = cursor.execute("SELECT name, city, system, time_on_game FROM building ORDER BY time_on_game DESC LIMIT ?",
                         (limit,))
    data = []
    for row in res.fetchall():
        data.append(dict(row))

    return data


def get_stat_by_time_on_game(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            SUM(time_on_game) as sum,
            AVG(time_on_game) as avg,
            MIN(time_on_game) as min,
            MAX(time_on_game) as max
        FROM building
    """)

    row = res.fetchone()
    result = dict(row)

    cursor.close()
    return result


def get_freq_by_tours_count(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            COUNT(*) as count,
            tours_count 
        FROM building
        GROUP BY tours_count
    """)

    result = []
    for row in res.fetchall():
        row_dict = dict(row)
        result.append(row_dict)

    cursor.close()
    return result


def filter_by_tours_count(db, min_tours, limit=30):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT tours_count
        FROM building
        WHERE tours_count > ?
        ORDER BY time_on_game DESC
        LIMIT ?
    """, (min_tours, limit))

    rows = cursor.fetchall()
    results = [dict(row) for row in rows]

    cursor.close()

    return results


items = load_csv('task_1_var_20_item (1).csv')
db = connect_to_db("first.db")
insert_data(db, items)

result_top = get_top_by_time_on_range(db, 30)
with open('top_results.json', 'w', encoding='utf-8') as file:
    json.dump(result_top, file, indent=4, ensure_ascii=False)

result_stat = get_stat_by_time_on_game(db)
with open('stat_results.json', 'w', encoding='utf-8') as file:
    json.dump(result_stat, file, indent=4, ensure_ascii=False)

result_freq = get_freq_by_tours_count(db)
with open('freq_results.json', 'w', encoding='utf-8') as file:
    json.dump(result_freq, file, indent=4, ensure_ascii=False)

result_filter = filter_by_tours_count(db, 10)
with open('filter_results.json', 'w', encoding='utf-8') as file:
    json.dump(result_filter, file, indent=4, ensure_ascii=False)
