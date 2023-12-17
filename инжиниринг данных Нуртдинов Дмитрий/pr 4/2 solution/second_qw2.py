
import json
import sqlite3

def load_data(file_name):
    with open(file_name, encoding='utf-8') as file:
        items = json.load(file)
    return items

def connect_to_db(file_name):
    connection = sqlite3.connect(file_name)
    connection.row_factory = sqlite3.Row
    return connection

def insert_comment_data(db, data):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT INTO comment (game_id, place, prise)
        VALUES(
            (SELECT id FROM games WHERE name = :name),
            :place,
            :prise
        )
    """, data)
    db.commit()

def first_query(db, name):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT * 
        FROM comment 
        WHERE game_id = (SELECT id FROM games WHERE name = ?)
    """, [name])
    items = []
    for row in res.fetchall():
        item = dict(row)
        items.append(item)
    cursor.close()
    return items

def second_query(db, name):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            AVG(place) as avg_place,
            AVG(prise) as avg_prise
        FROM comment 
        WHERE game_id = (SELECT id FROM games WHERE name = ?)
    """, [name])
    result = dict(res.fetchone())
    cursor.close()
    return result

def third_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            name,
            (SELECT COUNT(*) FROM comment WHERE id = game_id) as count
        FROM games
        ORDER BY count DESC
        LIMIT 10
    """)
    results = []
    for row in res.fetchall():
        result = dict(row)
        results.append(result)
    cursor.close()
    return results

db = connect_to_db('help.db')

result_first = first_query(db, 'Пойковский 1970')
result_second = second_query(db, 'Пойковский 1970')
result_third = third_query(db)

file_first = open('first_query_results.json', 'w', encoding='utf-8')
file_second = open('second_query_results.json', 'w', encoding='utf-8')
file_third = open('third_query_results.json', 'w', encoding='utf-8')

json.dump(result_first, file_first, ensure_ascii=False, indent=4)
json.dump(result_second, file_second, ensure_ascii=False, indent=4)
json.dump(result_third, file_third, ensure_ascii=False, indent=4)

file_first.close()
file_second.close()
file_third.close()
