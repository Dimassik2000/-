import csv
import sqlite3


def load_data_csv(file_name):
    items = list()
    with open(file_name, newline='\n', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=';')
        fields = next(reader)
        for row in reader:
            item = {
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

def connect_to_db(file_name):
    connection = sqlite3.connect(file_name)
    connection.row_factory = sqlite3.Row
    return connection

def insert_comment_data(db, data):
    cursor = db.cursor()
    cursor.executemany("""
           INSERT INTO comment (name,city,begin,system,tours_count,min_rating,time_on_game)
           VALUES(
                :name,:city,:begin,:system,:tours_count,:min_rating,:time_on_game
           )""", data)
    db.commit()

items = load_data_csv('task_1_var_20_item (1).csv')
db = connect_to_db('help.db')
insert_comment_data(db, items)