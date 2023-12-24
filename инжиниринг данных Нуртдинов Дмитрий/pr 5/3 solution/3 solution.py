from pymongo import MongoClient
import json

def connect():
    client = MongoClient()
    db = client['test-database']
    return db.person

def read_data(file_name):
    items = []
    with open(file_name, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        item = dict()
        for line in lines:
            if line == '=====\n':
                items.append(item)
                item = dict()
            else:
                line = line.strip()
                splitted = line.split('::')

                if splitted[0] in ['salary', 'year', 'age']:
                    item[splitted[0]] = int(splitted[1])
                elif splitted == 'id':
                    continue
                else:
                    item[splitted[0]] = splitted[1]

    return items

read_data('task_3_item.text')
data = read_data('task_3_item.text')

def insert_many(collection, data):
    # collection.insert_many(data)
    for item in data:
        collection.replace_one({'id': item['id']}, item, upsert=True)

def delete_by_salary(collection):
    result = collection.delete_many({
        '$or': [
            {'salary': {'$lt': 25_000}},
            {'salary': {'$gt': 175_000}}
        ]
    })
    return result

db = connect()
data = read_data('task_3_item.text')
insert_many(db, data)

result = delete_by_salary(db)

with open('delete_by_salary.json', 'w') as f:
    json.dump(result.raw_result, f)


def update_age(collection):
    result = collection.update_many({}, {
        '$inc': {
            'age': 1
        }
    })


    with open('update_age.json.json', 'w') as file:
        json.dump(result.raw_result, file)


def increase_salary(collection):
    filter = {
        'job': {'$in': ['Бухгалтер','Архитектор','Менеджер']}

    }
    update = {
        '$mul': {
            'salary': 1.05
        }
    }


    result = collection.update_many(filter, update)

    with open('increase_salary.json', 'w') as file:
        json.dump(result.raw_result, file)


def increase_salary_by_city(collection):
    filter = {
        'city': {'$in': ['Бланес','Санкт-Петербург','Тирана']}

    }
    update = {
        '$mul': {
            'salary': 1.07
        }
    }


    result = collection.update_many(filter, update)

    with open('increase_salary_by_city.json', 'w') as file:
        json.dump(result.raw_result, file)


def increase_salary_by_city_job_age(collection):
    filter = {
        'city': {'$in': ['Бланес','Санкт-Петербург','Тирана']},
        'age': {'$nin': ['22','45','56']},
        'job': {'$in': ['Менеджер','IT-специалис','Бухгалтер']},
                }


    update = {
        '$mul': {
            'salary': 1.1
        }
    }


    result = collection.update_many(filter, update)

    with open('increase_salary_by_city_job_age.json', 'w') as file:
        json.dump(result.raw_result, file)


def delete_by_salary1(collection):
    result = collection.delete_many({
        '$or': [
            {'salary': {'$lt': 27_000}},
            {'salary': {'$gt': 180_000}}
        ]
    })
    return result

db = connect()
data = read_data('task_3_item.text')
insert_many(db, data)

result = delete_by_salary(db)

with open('delete_by_salary1.json', 'w') as f:
    json.dump(result.raw_result, f)

collection = db
collection.insert_many(data, ordered=False)
delete_by_salary(collection)
update_age(collection)
increase_salary(connect())
increase_salary_by_city(connect())
increase_salary_by_city_job_age(connect())
delete_by_salary1(collection)



