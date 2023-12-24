import json
import csv
from pymongo import MongoClient

def connect():
    client = MongoClient()
    db = client['student-grades']
    return db.person

def get_from_csv(filename):
    items = list()
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=',')
        for row in reader:
            items.append(dict(row))
        return items

def load_json_data(file_name):
    with open(file_name, 'r', encoding='utf-8', newline='\n') as file:
        items = json.load(file)
    return items

def insert_many(collection, data):
    collection.insert_many(data)


def sort_by_age(collection):
    with open('sort_by_age.json', 'w') as file:
        for person in collection.find().limit(10).sort('Age', -1):
            person['_id'] = str(person['_id'])  # Преобразование ObjectId в строку
            json.dump(person, file)
            file.write('\n')

def filter_by_age(collection):
    with open("filter_by_age.json", "w") as file:
        for person in collection.find({'Age': {"$lt": 15}}).limit(15).sort("distance to university (km)", 1):
            person['_id'] = str(person['_id'])
            json.dump(person, file)
            file.write("\n")

def complex_filter_by_age_and_POLAR4_Quintile(collection):
    with open('complex_filter_by_age_pl_q.json', 'w') as file:
        for person in (collection
                .find({'Age': '0',
                       'POLAR4 Quintile': {'$in': ['4', '3', '2']}
                       }, limit=10)
                .sort({'Gender': 1})):
                    person['_id'] = str(person['_id'])
                    json.dump(person, file)
                    file.write('\n')

def count_obj(collection):
    result = collection.count_documents({
        'Age': {'$gt': 25, '$lt': 35},
        '# of Absence': {'$in': ['0', '2', '3', '4']},
        '$or': [
            {'TUNDRA MSOA Quintile': {'$gt': '2', '$lte': '5'}}
        ]
    })
    with open('count_obj.json', 'w') as file:
        json.dump({'count': result}, file)

def sort_by_Tundra(collection):
    with open('sort_by_Tundra.json', 'w') as file:
        for person in collection.find().limit(10).sort('TUNDRA MSOA Quintile', -1):
            person['_id'] = str(person['_id'])  # Преобразование ObjectId в строку
            json.dump(person, file)
            file.write('\n')

def get_stat_by_age(collection):
    q = [
        {
            '$group': {
                '_id': "result",
                'max': {'$max': '$Age'},
                'min': {'$min': '$Age'},
                'avg': {'$avg': '$Age'}
            }
        }
    ]

    result = []
    for stat in collection.aggregate(q):
        result.append(stat)

    with open('get_stat_by_age.json', 'w') as file:
        json.dump(result,file)

def get_stat_by_Percent_Attended(collection):
    q = [
        {
            '$group': {
                '_id': "$Percent Attended",
                'max': {'$max': '$POLAR4 Quintile'},
                'min': {'$min': '$POLAR4 Quintile'},
                'avg': {'$avg': '$POLAR4 Quintile'}
            }
        }
    ]

    result = []
    for stat in collection.aggregate(q):
        result.append(stat)

    with open('get_stat_by_Percent_Attended.json', 'w', encoding='utf-8') as file:
        json.dump(result,file, indent=4, ensure_ascii=False)

def get_freq_by_gender(collection):
    q = [
        {
            '$group': {
                '_id': "$Gender",
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

    with open('get_freq_by_gender.json', 'w', encoding='utf-8') as file:
        json.dump(result,file, indent=2, ensure_ascii=False)


def get_age_stat_by_column_Adult_HE_2001_Quintile(collection, column_name):
    q = [
        {
            '$group': {
                '_id': f"${column_name}",
                'max': {'$max': '$Age'},
                'min': {'$min': '$Age'},
                'avg': {'$avg': '$Age'}
            }
        }
    ]

    result = []
    for stat in collection.aggregate(q):
        result.append(stat)

    with open('get_age_stat_by_column_Adult_HE_2001_Quintile.json', 'w', encoding='utf-8') as file:
        json.dump(result, file, indent=4, ensure_ascii=False)


def max_POLAR3_Quintile_by_min_age(collection):
    q = [
        {
            '$group': {
                '_id': "$Age",
                "max_POLAR3_Quintile": {'$max': '$POLAR3 Quintile'}
            }
        },
        {
            '$group': {
                '_id': "result",
                "min_age": {'$min': '$_id'},
                'max_POLAR3_Quintile': {'$max': '$max_POLAR3_Quintile'}
            }
        }
    ]

    result = []

    for stat in collection.aggregate(q):
        result.append(stat)

    with open('max_POLAR3_Quintile_by_min_age.json', 'w', encoding='utf-8') as file:
        json.dump(result, file, indent=4, ensure_ascii=False)

def increase_POLAR3_Quintile(collection):
    filter = {
        'Adult HE 2001 Quintile': {'$in': ['1', '2', '3']},
        'POLAR3 Quintile': {'$type': 'number'}
    }
    update = {
        '$mul': {'POLAR3 Quintile': 1.50}
    }

    result = collection.update_many(filter, update)

    with open('increase_POLAR3_Quintile.json', 'w') as file:
        json.dump(result.raw_result, file)


def increase_POLAR3_Quintile_by_TUNDRA_MSOA_Quintile(collection):
    filter = {
        'TUNDRA MSOA Quintile': {'$in': ['2','3']},
        'POLAR3 Quintile': {'$type': 'number'}
    }
    update = {
        '$mul': {
            'POLAR3 Quintile': 1.75
        }
    }

    result = collection.update_many(filter, update)

    with open('increase_POLAR3_Quintile_by_TUNDRA_MSOA_Quintile.json', 'w') as file:
        json.dump(result.raw_result, file)

def increase_POLAR3(collection):
    filter = {
        'Age': {'$in': ['20','16','17']},
        'POLAR3 Quintile': {'$type': 'number'}
    }
    update = {
        '$mul': {
            'POLAR3 Quintile': 1.50
        }
    }


    result = collection.update_many(filter, update)

    with open('increase_POLAR3.json', 'w') as file:
        json.dump(result.raw_result, file)

def increase_Gaps(collection):
    filter = {
        'Age': {'$in': ['25','18','17']},
        'Gaps GCSE Quintile': {'$type': 'number'}
    }
    update = {
        '$mul': {
            'Gaps GCSE Quintile': 1.50
        }
    }


    result = collection.update_many(filter, update)

    with open('increase_Gaps.json', 'w') as file:
        json.dump(result.raw_result, file)

def increase_Adult2001(collection):
    filter = {
        'Age': {'$in': ['19','25','17']},
        'Adult HE 2001 Quintile': {'$type': 'number'}
    }
    update = {
        '$mul': {
            'Adult HE 2001 Quintile': 1.50
        }
    }


    result = collection.update_many(filter, update)

    with open('increase_Adult2001.json', 'w') as file:
        json.dump(result.raw_result, file)

data = get_from_csv('test.csv')
db = load_json_data('test.json')
items = data + db
insert_many(connect(), items)
sort_by_age(connect())
filter_by_age(connect())
complex_filter_by_age_and_POLAR4_Quintile(connect())
count_obj(connect())
sort_by_Tundra(connect())
get_stat_by_age(connect())
get_stat_by_Percent_Attended(connect())
get_freq_by_gender(connect())
get_age_stat_by_column_Adult_HE_2001_Quintile(connect(), 'Adult HE 2001 Quintile')
max_POLAR3_Quintile_by_min_age(connect())
increase_POLAR3_Quintile(connect())
increase_POLAR3_Quintile_by_TUNDRA_MSOA_Quintile(connect())
increase_POLAR3(connect())
increase_Gaps(connect())
increase_Adult2001(connect())