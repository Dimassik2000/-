import pickle
from pymongo import MongoClient
import json

def connect():
    client = MongoClient()
    db = client['test-database']
    return db.person


def read_pickle_file(filename):
    with open(filename, 'rb') as file:
        data = pickle.load(file)
    return data


filename = 'task_2_item.pkl'
data = read_pickle_file(filename)


def insert_many(collection, data):
    collection.insert_many(data)


def get_stat_by_salary(collection):
    q = [
        {
            '$group': {
                '_id': "result",
                'max': {'$max': '$salary'},
                'min': {'$min': '$salary'},
                'avg': {'$avg': '$salary'}
            }
        }
    ]

    result = []
    for stat in collection.aggregate(q):
        result.append(stat)

    with open('get_stat_by_salary.json', 'w') as file:
        json.dump(result,file)


def get_stat_by_job(collection):
    q = [
        {
            '$group': {
                '_id': "$job",
                'max': {'$max': '$salary'},
                'min': {'$min': '$salary'},
                'avg': {'$avg': '$salary'}
            }
        }
    ]

    result = []
    for stat in collection.aggregate(q):
        result.append(stat)

    with open('get_stat_by_job.json', 'w', encoding='utf-8') as file:
        json.dump(result,file, indent=4, ensure_ascii=False)


def get_freq_by_job(collection):
    q = [
        {
            '$group': {
                '_id': "$job",
                'count': {'$sum': 1}
            }
        },
        {
            '$sort' : {
                'count': -1
            }
        }
    ]

    result = []
    for stat in collection.aggregate(q):
        result.append(stat)

    with open('get_freq_by_job.json', 'w', encoding='utf-8') as file:
        json.dump(result,file, indent=2, ensure_ascii=False)


def get_salary_stat_by_column(collection, column_name):
    q = [
        {
            '$group': {
                '_id': f"${column_name}",
                'max': {'$max': '$salary'},
                'min': {'$min': '$salary'},
                'avg': {'$avg': '$salary'}
            }
        }
    ]

    result = []
    for stat in collection.aggregate(q):
        result.append(stat)

    with open('get_salary_stat_by_column_'+column_name+'.json', 'w', encoding='utf-8') as file:
        json.dump(result, file, indent=4, ensure_ascii=False)



def get_age_stat_by_column_city(collection, column_name):
    q = [
        {
            '$group': {
                '_id': f"${column_name}",
                'max': {'$max': '$age'},
                'min': {'$min': '$age'},
                'avg': {'$avg': '$age'}
            }
        }
    ]

    result = []
    for stat in collection.aggregate(q):
        result.append(stat)

    with open('get_age_stat_by_column_city.json', 'w', encoding='utf-8') as file:
        json.dump(result, file, indent=4, ensure_ascii=False)


def get_age_stat_by_column_job(collection, column_name):
    q = [
        {
            '$group': {
                '_id': f"${column_name}",
                'max': {'$max': '$age'},
                'min': {'$min': '$age'},
                'avg': {'$avg': '$age'}
            }
        }
    ]

    result = []
    for stat in collection.aggregate(q):
        result.append(stat)

    with open('get_age_stat_by_column_job.json', 'w', encoding='utf-8') as file:
        json.dump(result, file, indent=4, ensure_ascii=False)


def max_salary_by_min_age(collection):
    q = [
        {
            '$group': {
                '_id': "$age",
                "max_salary": {'$max': '$salary'}
            }
        },
        {
            '$group': {
                '_id': "result",
                "min_age": {'$min': '$_id'},
                'max_salary': {'$max': '$max_salary'}
            }
        }
    ]

    result = []

    for stat in collection.aggregate(q):
        result.append(stat)

    with open('max_salary_by_min_age.json', 'w', encoding='utf-8') as file:
        json.dump(result, file, indent=4, ensure_ascii=False)


def min_salary_by_max_age(collection):
    q = [
        {
            '$group': {
                '_id': "$age",
                "min_salary": {'$min': '$salary'}
            }
        },
        {
            '$group': {
                '_id': "result",
                "max_age": {'$max': '$_id'},
                'min_salary': {'$min': '$max_salary'}
            }
        }
    ]

    result = []

    for stat in collection.aggregate(q):
        result.append(stat)

    with open('min_salary_by_max_age.json', 'w', encoding='utf-8') as file:
        json.dump(result, file, indent=4, ensure_ascii=False)


def big_query(collection):
    q = [
        {
            '$match': {
                'salary': {'$gt': 50_000}
            }
        },
        {
            '$group': {
                '_id': '$city',
                'min': {'$min': '$age'},
                'max': {'$max': '$age'},
                'avg': {'$avg': '$age'},
            }
        },
        {
            '$sort': {
                'avg': -1
            }
        }
    ]

    result = []

    for stat in collection.aggregate(q):
        result.append(stat)

    with open('big_query.json', 'w', encoding='utf-8') as file:
        json.dump(result, file, indent=4, ensure_ascii=False)


def big_query_2(collection):
    q = [
        {
            '$match': {
                'city': {'$in': ['Баку','Прага','Луарка','Навалькарнеро']},
                'job': {'$in': ['Инженер','Менеджер','Психолог']},
                '$or': [
                    {'age': {'$gt': 18, '$lt': 25}},
                    {'age': {'$gt': 50, '$lt': 65}}
                ]
            }
        },
        {
            '$group': {
                '_id': 'result',
                'min': {'$min': '$salary'},
                'max': {'$max': '$salary'},
                'avg': {'$avg': '$salary'},
            }
        }
    ]

    result = []

    for stat in collection.aggregate(q):
        result.append(stat)

    with open('big_query_2.json', 'w', encoding='utf-8') as file:
        json.dump(result, file, indent=4, ensure_ascii=False)


def big_query_3(collection):
    q = [
        {
            '$match': {
                'salary': {'$gt': 70_000}
            }
        },
        {
            '$group': {
                '_id': 'city',
                'min': {'$min': '$age'},
                'max': {'$max': '$age'}
            }
        },
        {
            '$sort': {
                'avg': 1
            }
        }
    ]

    result = []

    for stat in collection.aggregate(q):
        result.append(stat)

    with open('big_query_3.json', 'w', encoding='utf-8') as file:
        json.dump(result, file, indent=4, ensure_ascii=False)


insert_many(connect(), data)
get_stat_by_salary(connect())
get_stat_by_job(connect())
get_freq_by_job(connect())
get_salary_stat_by_column(connect(),'city')
get_salary_stat_by_column(connect(),'job')
get_age_stat_by_column_city(connect(), 'city')
get_age_stat_by_column_job(connect(), 'job')
max_salary_by_min_age(connect())
min_salary_by_max_age(connect())
big_query(connect())
big_query_2(connect())
big_query_3(connect())