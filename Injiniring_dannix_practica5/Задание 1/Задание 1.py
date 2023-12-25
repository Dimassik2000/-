import json
import pymongo
from pymongo import MongoClient


def connect_to_mongodb():
    # Подключение к MongoDB
    client = MongoClient('localhost', 27017)
    db = client['mydatabase']
    collection = db['mycollection']
    return collection


def read_data_from_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data


def write_data_to_json(data, file_path):
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4, default=str)


def task_1(collection):
    # Вывод первых 10 записей, отсортированных по убыванию по полю salary
    data = list(collection.find().sort('salary', pymongo.DESCENDING).limit(10))
    write_data_to_json(data, 'task_1_output.json')


def task_2(collection):
    # Вывод первых 15 записей, отфильтрованных по предикату age < 30, отсортированных по убыванию по полю salary
    data = list(collection.find({'age': {'$lt': 30}}).sort('salary', pymongo.DESCENDING).limit(15))
    write_data_to_json(data, 'task_2_output.json')


def task_3(collection):
    # Вывод первых 10 записей, отфильтрованных по сложному предикату
    data = list(collection.find(
        {'city': 'Аликанте', 'job': {'$in': ['Учитель', 'Оператор call-центра', 'Менеджер']}}).sort('age',
                                                                                                    pymongo.ASCENDING).limit(
        10))
    write_data_to_json(data, 'task_3_output.json')


def task_4(collection):
    # Вывод количества записей, получаемых в результате фильтрации
    count = collection.count_documents({'$and': [{'age': {'$gt': 20}}, {'age': {'$lt': 40}},
                                                 {'year': {'$in': [2019, 2020]}}, {
                                                     '$or': [{'salary': {'$gt': 50000, '$lte': 75000}},
                                                             {'salary': {'$gt': 125000, '$lt': 150000}}]}]})
    write_data_to_json({'count': count}, 'task_4_output.json')


collection = connect_to_mongodb()
file_path = r'task_1_item.json'

data = read_data_from_file(file_path)
collection.insert_many(data)

task_1(collection)
task_2(collection)
task_3(collection)
task_4(collection)
