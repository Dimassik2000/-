import csv
import json
import pymongo
from pymongo import MongoClient


def connect_to_mongodb():
    client = MongoClient('localhost', 27017)
    db = client['mydatabase']
    collection = db['mycollection']
    return collection


def read_data_from_csv(file_path):
    data = []
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=';')
        for row in reader:
            data.append(row)
    return data


def write_data_to_json(data, file_path):
    data_serializable = []
    for item in data:
        item_serializable = {**item}
        if '_id' in item_serializable:
            item_serializable['_id'] = str(item_serializable['_id'])  # Преобразование ObjectId в строку
        data_serializable.append(item_serializable)

    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data_serializable, file, ensure_ascii=False, indent=4)


def task_1(collection):
    collection.delete_many({
        '$or': [
            {'salary': {'$lt': 25000}},
            {'salary': {'$gt': 175000}}
        ]
    })


def task_2(collection):
    collection.update_many({}, [{'$set': {'age': {'$toInt': '$age'}}}])
    collection.update_many({}, {'$inc': {'age': 1}})


def task_3(collection):
    collection.update_many({}, [{'$set': {'salary': {'$toDouble': '$salary'}}}])
    collection.update_many({'job': {'$in': ['Программист', 'Аналитик']}}, {'$mul': {'salary': 1.05}})


def task_4(collection):
    collection.update_many({'city': {'$in': ['Вроцлав', 'Сеговия']}}, {'$mul': {'salary': 1.07}})


def task_5(collection):
    collection.update_many(
        {
            'city': 'Москва',
            'job': {'$in': ['Программист', 'Аналитик', 'Дизайнер']},
            'age': {'$gte': 25, '$lte': 40}
        },
        {'$mul': {'salary': 1.10}}
    )


def task_6(collection):
    collection.delete_many({'job': 'Менеджер'})


file_path = 'task_3_item.csv'
data = read_data_from_csv(file_path)

collection = connect_to_mongodb()
collection.insert_many(data)

task_1(collection)
task_2(collection)
task_3(collection)
task_4(collection)
task_5(collection)
task_6(collection)

result = list(collection.find())
write_data_to_json(result, 'output.json')
