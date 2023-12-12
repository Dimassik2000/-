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



fields_to_remove = ['acousticness', 'energy']

pkl_data = load_pkl_data('task_3_var_20_part_1.pkl')

modified_data = remove_fields_from_data(pkl_data, fields_to_remove)

items = modified_data + load_json_data("task_3_var_20_part_2.json")

db = connect_to_db('third_db.db')
insert_comment_data(db, items)



