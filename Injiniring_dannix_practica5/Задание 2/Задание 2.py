import json
import pymongo
from pymongo import MongoClient


def connect_to_mongodb():
    client = MongoClient('localhost', 27017)
    db = client['mydatabase']
    collection = db['mycollection']
    return collection


def write_data_to_json(data, file_path):
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


def query_min_avg_max_salary(collection):
    result = collection.aggregate([
        {
            '$group': {
                '_id': None,
                'min_salary': {'$min': '$salary'},
                'avg_salary': {'$avg': '$salary'},
                'max_salary': {'$max': '$salary'}
            }
        }
    ])
    # Получение результата из курсора
    data = next(result, None)
    return data


def query_professions_count(collection):
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$job',
                'count': {'$sum': 1}
            }
        }
    ])

    output = {}
    for doc in result:
        output[doc['_id']] = doc['count']

    return output


def query_min_avg_max_salary_by_city(collection):
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$city',
                'min_salary': {'$min': '$salary'},
                'avg_salary': {'$avg': '$salary'},
                'max_salary': {'$max': '$salary'}
            }
        }
    ])

    output = {}
    for doc in result:
        output[doc['_id']] = {
            'min_salary': doc['min_salary'],
            'avg_salary': doc['avg_salary'],
            'max_salary': doc['max_salary']
        }

    return output


def query_min_avg_max_salary_by_profession(collection):
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$job',
                'min_salary': {'$min': '$salary'},
                'avg_salary': {'$avg': '$salary'},
                'max_salary': {'$max': '$salary'}
            }
        }
    ])

    output = {}
    for doc in result:
        output[doc['_id']] = {
            'min_salary': doc['min_salary'],
            'avg_salary': doc['avg_salary'],
            'max_salary': doc['max_salary']
        }

    return output


def query_min_avg_max_age_by_city(collection):
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$city',
                'min_age': {'$min': '$age'},
                'avg_age': {'$avg': '$age'},
                'max_age': {'$max': '$age'}
            }
        }
    ])

    output = {}
    for doc in result:
        output[doc['_id']] = {
            'min_age': doc['min_age'],
            'avg_age': doc['avg_age'],
            'max_age': doc['max_age']
        }

    return output


def query_min_avg_max_age_by_profession(collection):
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$job',
                'min_age': {'$min': '$age'},
                'avg_age': {'$avg': '$age'},
                'max_age': {'$max': '$age'}
            }
        }
    ])

    output = {}
    for doc in result:
        output[doc['_id']] = {
            'min_age': doc['min_age'],
            'avg_age': doc['avg_age'],
            'max_age': doc['max_age']
        }

    return output


def query_max_salary_min_age(collection):
    result = collection.aggregate([
        {
            '$group': {
                '_id': None,
                'max_salary': {
                    '$max': '$salary'
                },
                'min_age': {
                    '$min': '$age'
                }
            }
        },
        {
            '$project': {
                '_id': 0,
                'max_salary': 1,
                'min_age': 1
            }
        }
    ])

    for doc in result:
        return doc


def query_min_salary_max_age(collection):
    result = collection.aggregate([
        {
            '$group': {
                '_id': None,
                'min_salary': {
                    '$min': '$salary'
                },
                'max_age': {
                    '$max': '$age'
                }
            }
        },
        {
            '$project': {
                '_id': 0,
                'min_salary': 1,
                'max_age': 1
            }
        }
    ])

    for doc in result:
        return doc


def query_min_avg_max_age_by_city_salary_gt_50000(collection):
    result = collection.aggregate([
        {
            '$match': {
                'salary': {'$gt': 50000}
            }
        },
        {
            '$group': {
                '_id': '$city',
                'min_age': {'$min': '$age'},
                'avg_age': {'$avg': '$age'},
                'max_age': {'$max': '$age'}
            }
        },
        {
            '$sort': {
                '_id': 1
            }
        }
    ])

    output = {}
    for doc in result:
        output[doc['_id']] = {
            'min_age': doc['min_age'],
            'avg_age': doc['avg_age'],
            'max_age': doc['max_age']
        }

    return output


def query_min_avg_max_age_by_city_salary_gt_50000_sorted_by_field(collection, field):
    result = collection.aggregate([
        {
            '$match': {
                'salary': {'$gt': 50000}
            }
        },
        {
            '$group': {
                '_id': '$city',
                'min_age': {'$min': '$age'},
                'avg_age': {'$avg': '$age'},
                'max_age': {'$max': '$age'}
            }
        },
        {
            '$sort': {
                field: 1
            }
        }
    ])

    output = {}
    for doc in result:
        output[doc['_id']] = {
            'min_age': doc['min_age'],
            'avg_age': doc['avg_age'],
            'max_age': doc['max_age']
        }

    return output


def query_min_avg_max_salary_by_city_profession_age_ranges(collection):
    result = collection.aggregate([
        {
            '$match': {
                '$or': [
                    {'age': {'$gt': 18, '$lt': 25}},
                    {'age': {'$gt': 50, '$lt': 65}}
                ]
            }
        },
        {
            '$group': {
                '_id': {
                    'city': '$city',
                    'job': '$job',
                    'age_range': {
                        '$switch': {
                            'branches': [
                                {'case': {'$and': [{'$gt': ['$age', 18]}, {'$lt': ['$age', 25]}]}, 'then': '18-25'},
                                {'case': {'$and': [{'$gt': ['$age', 50]}, {'$lt': ['$age', 65]}]}, 'then': '50-65'}
                            ],
                            'default': 'Unknown'
                        }
                    }
                },
                'min_salary': {'$min': '$salary'},
                'avg_salary': {'$avg': '$salary'},
                'max_salary': {'$max': '$salary'}
            }
        },
        {
            '$sort': {
                '_id.city': 1,
                '_id.job': 1,
                '_id.age_range': 1
            }
        }
    ])

    output = {}
    for doc in result:
        if doc['_id']['city'] not in output:
            output[doc['_id']['city']] = {}
        if doc['_id']['job'] not in output[doc['_id']['city']]:
            output[doc['_id']['city']][doc['_id']['job']] = {}

        output[doc['_id']['city']][doc['_id']['job']][doc['_id']['age_range']] = {
            'min_salary': doc['min_salary'],
            'avg_salary': doc['avg_salary'],
            'max_salary': doc['max_salary']
        }

    return output


def query_custom(collection):
    result = collection.aggregate([
        {
            '$match': {
                'job': 'Оператор call-центра'
            }
        },
        {
            '$group': {
                '_id': '$city',
                'count': {'$sum': 1}
            }
        },
        {
            '$sort': {
                '_id': 1
            }
        }
    ])

    output = {}
    for doc in result:
        output[doc['_id']] = doc['count']

    return output


# Подключение к MongoDB
collection = connect_to_mongodb()

# Задание 1: Вывод минимальной, средней, максимальной зарплаты
task_1_output = query_min_avg_max_salary(collection)
write_data_to_json(task_1_output, 'task_1_output.json')

# Задание 2: Вывод количества данных по профессиям
task_2_output = query_professions_count(collection)
write_data_to_json(task_2_output, 'task_2_output.json')

# Задание 3: Вывод минимальной, средней, максимальной зарплаты по городу
task_3_output = query_min_avg_max_salary_by_city(collection)
write_data_to_json(task_3_output, 'task_3_output.json')

# Задание 4: Вывод минимальной, средней, максимальной зарплаты по профессии
task_4_output = query_min_avg_max_salary_by_profession(collection)
write_data_to_json(task_4_output, 'task_4_output.json')

# Задание 5: Вывод минимального, среднего, максимального возраста по городу
task_5_output = query_min_avg_max_age_by_city(collection)
write_data_to_json(task_5_output, 'task_5_output.json')

# Задание 6: Вывод минимального, среднего, максимального возраста по профессии
task_6_output = query_min_avg_max_age_by_profession(collection)
write_data_to_json(task_6_output, 'task_6_output.json')

# Задание 7: Вывод максимальной зарплаты при минимальном возрасте
task_7_output = query_max_salary_min_age(collection)
write_data_to_json(task_7_output, 'task_7_output.json')

# Задание 8: Вывод минимальной зарплаты при максимальном возрасте
task_8_output = query_min_salary_max_age(collection)
write_data_to_json(task_8_output, 'task_8_output.json')

# Задание 9: Вывод минимального, среднего, максимального возраста по городу, при условии, что заработная плата больше 50 000, отсортировать вывод по любому полю
task_9_output = query_min_avg_max_age_by_city_salary_gt_50000(collection)
write_data_to_json(task_9_output, 'task_9_output.json')

# Задание 10: Вывод минимальной, средней, максимальной salary в произвольно заданных диапазонах по городу, профессии, и возрасту: 18<age<25 & 50<age<65
task_10_output = query_min_avg_max_salary_by_city_profession_age_ranges(collection)
write_data_to_json(task_10_output, 'task_10_output.json')

# Задание 11: Произвольный запрос с $match, $group, $sort
task_11_output = query_custom(collection)
write_data_to_json(task_11_output, 'task_11_output.json')
