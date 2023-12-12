import csv
import sqlite3
import json


def load_csv(filename):
    items = list()
    with open(filename, 'r', encoding = 'utf-8') as file:
        reader = csv.reader(file, delimiter=';')
        reader.__next__()
        for row in reader:
            if len(row) == 0: continue
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

    res = cursor.execute("SELECT name, city, system, time_on_game FROM building ORDER BY time_on_game DESC LIMIT 30")
    data1 = []
    for row in res.fetchall():
        item = {
            'name': row[0],
            'city': row[1],
            'system': row[2],
            'time_on_game': row[3]
        }
        data1.append(item)
    return data1


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
    result = {
        'sum': row[0],
        'avg': round(row[1], 2),
        'min': row[2],
        'max': row[3]
    }

    cursor.close()
    return result



def get_freq_by_tours_count(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            COUNT(*),
            tours_count 
        FROM building
        GROUP BY tours_count
    """)

    column_names = [description[0] for description in cursor.description]

    result = []
    for row in res.fetchall():
        row_dict = dict(zip(column_names, row))
        result.append(row_dict)
        print(row_dict)


    cursor.close()
    return []

def filter_by_tours_count(db, min_tours, limit=30):
    cursor = db.cursor()
    res = cursor.execute("""
                SELECT tours_count
                FROM building
                WHERE tours_count > ?
                ORDER BY time_on_game DESC
                LIMIT ?
                """, [min_tours, limit])

    rows = cursor.fetchall()
    results = []
    for row in res.fetchall():
        results.append({'tours_count': row[0]})

    cursor.close()

    return results

items = load_csv('task_1_var_20_item (1).csv')
db = connect_to_db("first.db")
insert_data(db,items)
get_top_by_time_on_range(db,30)
result = get_top_by_time_on_range(db,30)

with open ('result.json', 'w', encoding='utf-8') as file1:
    json.dump(result, file1, indent=4, ensure_ascii=False)


stat_result = get_stat_by_time_on_game(db)
print(stat_result)

freq_result = get_freq_by_tours_count(db)
print(freq_result)



