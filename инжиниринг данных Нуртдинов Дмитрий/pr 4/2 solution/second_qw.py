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
           )""", data)
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
        print(item)

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
    print(dict(res.fetchone()))

    cursor.close()
    return []

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
    print(dict(res.fetchone()))

    cursor.close()
    return []


# items = load_data('task_2_var_20_subitem.json')
db = connect_to_db('help.db')
first_query(db, 'Пойковский 1970')
second_query(db, 'Пойковский 1970')
third_query(db)
# insert_comment_data(db, items)
