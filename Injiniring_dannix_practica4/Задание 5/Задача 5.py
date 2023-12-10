import sqlite3
import json
import csv
from datetime import datetime

# Предметная область "Продажа автомобилей"
# файлы исходных данных
db_file = r'baza_dannix.db'
json_file = r'car_sales.json'
csv_file = r'car_sales1.csv'

conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Скрипт для создания 4 таблиц
cursor.execute('''CREATE TABLE IF NOT EXISTS table1
                  (Manufacturer TEXT, Model TEXT, Sales_in_thousands REAL, __year_resale_value REAL)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS table2
                  (Vehicle_type TEXT, Price_in_thousands REAL, Engine_size REAL, Horsepower REAL)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS table3
                  (Wheelbase REAL, Width REAL, Length REAL, Curb_weight REAL)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS table4
                  (Fuel_capacity REAL, Fuel_efficiency REAL, Latest_Launch TEXT, Power_perf_factor REAL)''')

# Скрипт для загрузки данных из файлов в базу данных
with open(json_file, 'r', encoding='utf-8') as file:
    json_data = json.load(file)
    cursor.executemany('INSERT INTO table1 VALUES (?, ?, ?, ?)',
                       [(data.get('Manufacturer'), data.get('Model'), data.get('Sales_in_thousands') or None,
                         data.get('__year_resale_value') or None) for data in json_data])

with open(csv_file, 'r', newline='', encoding='utf-8') as file:
    csv_data = csv.reader(file)
    next(csv_data)
    for row in csv_data:
        horsepower = int(row[3]) if row[3] else None
        wheelbase = float(row[4]) if row[4] else None
        width = float(row[5]) if row[5] else None
        length = float(row[6]) if row[6] else None
        curb_weight = float(row[7]) if row[7] else None
        fuel_capacity = float(row[8]) if row[8] else None
        fuel_efficiency = float(row[9]) if row[9] else None
        latest_launch = datetime.strptime(row[10], '%m/%d/%Y').date() if row[10] else None
        power_perf_factor = float(row[11]) if row[11] else None

        if row[1] and row[2]:
            cursor.executemany('INSERT INTO table2 VALUES (?, ?, ?, ?)',
                               [(row[0], float(row[1]), float(row[2]), horsepower)])
        cursor.executemany('INSERT INTO table3 VALUES (?, ?, ?, ?)', [(wheelbase, width, length, curb_weight)])
        cursor.executemany('INSERT INTO table4 VALUES (?, ?, ?, ?)',
                           [(fuel_capacity, fuel_efficiency, latest_launch, power_perf_factor)])

conn.commit()
conn.close()

# Скрипт с выполнением запросов к базе данных
# Запрос 1: выборка с простым условием
conn = sqlite3.connect(db_file)
cursor = conn.cursor()
cursor.execute("SELECT * FROM table1 WHERE Sales_in_thousands > 200")
rows = cursor.fetchall()
result1 = [{'Manufacturer': row[0], 'Model': row[1], 'Sales_in_thousands': row[2], '__year_resale_value': row[3]} for
           row in rows]

# Запрос 2: сортировка + ограничение количества
cursor.execute("SELECT * FROM table2 ORDER BY Price_in_thousands DESC LIMIT 5")
rows = cursor.fetchall()
result2 = [{'Vehicle_type': row[0], 'Price_in_thousands': row[1], 'Engine_size': row[2], 'Horsepower': row[3]} for row
           in rows]

# Запрос 3: подсчет объектов по условию, а также другие функции агрегации
cursor.execute("SELECT COUNT(*), Vehicle_type FROM table2 GROUP BY Vehicle_type")
rows = cursor.fetchall()
result3 = [{'Count': row[0], 'Vehicle_type': row[1]} for row in rows]

# Запрос 4: группировка
cursor.execute("SELECT Manufacturer, AVG(Sales_in_thousands) FROM table1 GROUP BY Manufacturer")
rows = cursor.fetchall()
result4 = [{'Manufacturer': row[0], 'Average_Sales_in_thousands': row[1]} for row in rows]

# Запрос 5: выборка автомобилей с максимальным объемом двигателя
cursor.execute("SELECT * FROM table2 WHERE Engine_size = (SELECT MAX(Engine_size) FROM table2)")
rows = cursor.fetchall()
result5 = [{'Vehicle_type': row[0], 'Price_in_thousands': row[1], 'Engine_size': row[2], 'Horsepower': row[3]} for row in rows]

# Запрос 6: обновление данных
cursor.execute("UPDATE table1 SET Sales_in_thousands = 300 WHERE Manufacturer = 'Toyota'")

# Описание запроса 6
description_query6 = "Выборка автомобилей с длиной более 180"

cursor.execute("SELECT * FROM table3 WHERE Length > 180")
rows = cursor.fetchall()
result6 = [{'Wheelbase': row[0], 'Width': row[1], 'Length': row[2], 'Curb_weight': row[3]} for row in rows]

conn.commit()
conn.close()

# Сохранение результатов в JSON-файлы
with open('result1-выборка.json', 'w') as json_file:
    json.dump(result1, json_file, indent=2)

with open('result2-сортировка.json', 'w') as json_file:
    json.dump(result2, json_file, indent=2)

with open('result3-агрегация.json', 'w') as json_file:
    json.dump(result3, json_file, indent=2)

with open('result4-группировка.json', 'w') as json_file:
    json.dump(result4, json_file, indent=2)

with open('result5-выборка_макс_объем.json', 'w') as json_file:
    json.dump(result5, json_file, indent=2)

with open('result6-обновление.json', 'w') as json_file:
    json.dump(result6, json_file, indent=2)