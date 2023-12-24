import csv
from pymongo import MongoClient
import json


def connect():
    client = MongoClient()
    db = client['test-database']
    return db.person


def get_from_csv(filename):
    items = list()
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        next(reader)
        for row in reader:
            item = {
                'job': row[0],
                'salary': int(row[1]),
                'id': row[2],
                'city': row[3],
                'years': int(row[4]),
                'age': int(row[5])
            }
            items.append(item)
        return items


def insert_many(collection, data):
    collection.insert_many(data)


def sort_by_salary(collection):
    with open('sort_by_salary.json', 'w') as file:
        for person in collection.find().limit(10).sort('salary', -1):
            person['_id'] = str(person['_id'])  # Преобразование ObjectId в строку
            json.dump(person, file)
            file.write('\n')


def filter_by_age(collection):
    with open("filter_by_age.json", "w") as file:
        for person in collection.find({'age': {"$lt": 30}}).limit(15).sort("salary", -1):
            person['_id'] = str(person['_id'])
            json.dump(person, file)
            file.write("\n")


def complex_filter_by_city_and_job(collection):
    with open('complex_filter_by_city_and_job.json', 'w') as file:
        for person in (collection
                .find({'city': 'Санкт-Петербург',
                       'job': {'$in': ['Строитель', 'Учитель', 'Программист']}
                       }, limit=10)
                .sort({'age': 1})):
                    person['_id'] = str(person['_id'])
                    json.dump(person, file)
                    file.write('\n')


def count_obj(collection):
    result = collection.count_documents({
        'age': {'$gt': 25, '$lt': 35},
        'year': {'$in': [2019, 2020, 2021, 2022]},
        '$or': [
            {'salary': {'$gt': 50000, '$lte': 75000}},
            {'salary': {'$gt': 125000, '$lt': 150000}}
        ]
    })
    with open('count_obj.json', 'w') as file:
        json.dump({'count': result}, file)


data = get_from_csv('task_1_item.csv')
insert_many(connect(), data)
sort_by_salary(connect())
filter_by_age(connect())
complex_filter_by_city_and_job(connect())
count_obj(connect())