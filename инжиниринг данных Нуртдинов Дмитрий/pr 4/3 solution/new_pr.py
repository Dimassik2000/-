import json
import pickle
import sqlite3

def create_table(db):
    cursor = db.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS music_data (
            artist TEXT,
            song TEXT,
            duration_ms INTEGER,
            year INTEGER,
            tempo REAL,
            genre TEXT,
            popularity INTEGER
        )
    """)
    db.commit()
    cursor.close()

def insert_music_data(db, data):
    cursor = db.cursor()
    try:
        cursor.executemany("""
            INSERT INTO music_data (artist, song, duration_ms, year, tempo, genre, popularity)
            VALUES (?,?,?,?,?,?,?)
        """, data)
        db.commit()
    except sqlite3.Error as e:
        print(f"Error occurred: {e}")
    cursor.close()

def load_data_from_pkl(file_name):
    with open(file_name, 'rb') as file:
        data = pickle.load(file)
    return [data]

def load_data_from_json(file_name):
    with open(file_name, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

def main():
    db = sqlite3.connect('music1.db')
    create_table(db)

    pkl_data = load_data_from_pkl('task_3_var_20_part_1.pkl')
    json_data = load_data_from_json('task_3_var_20_part_2.json')

    combined_data = []
    for item in pkl_data:
        if len(item) == 7:
            combined_data.append(
                (item['artist'], item['song'], item['duration_ms'], item['year'], item['tempo'],
                 item['genre'], item['popularity'])
            )
    for item in json_data:
        if len(item) == 7:
            combined_data.append(
                (item['artist'], item['song'], item['duration_ms'], item['year'], item['tempo'],
                 item['genre'], item['popularity'])
            )

    insert_music_data(db, combined_data)

    db.close()


main()
