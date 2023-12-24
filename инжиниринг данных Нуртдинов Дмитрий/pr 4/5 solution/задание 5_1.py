import sqlite3
import json
import csv

# Подключение к базе данных
conn = sqlite3.connect('clothing_sales.db')
c = conn.cursor()

# Создание таблиц
c.execute('''CREATE TABLE IF NOT EXISTS SalesData (
                Area_Code INTEGER,
                State TEXT,
                Market TEXT,
                Market_Size TEXT,
                Profit REAL,
                Margin REAL,
                Sales REAL,
                COGS REAL,
                Total_Expenses REAL,
                Marketing REAL
            )''')

c.execute('''CREATE TABLE IF NOT EXISTS BudgetData (
                Inventory REAL,
                Budget_Profit REAL,
                Budget_COGS REAL,
                Budget_Margin REAL,
                Budget_Sales REAL,
                ProductId INTEGER,
                Date TEXT,
                Product_Type TEXT,
                Product TEXT,
                Type TEXT
            )''')


c.execute('''CREATE TABLE IF NOT EXISTS ProductData (
                ProductId INTEGER PRIMARY KEY,
                ProductName TEXT,
                ProductDescription TEXT
            )''')


with open('sales_data1.json') as json_file:  # файла исходных данных нет в коде
    data = json.load(json_file)
    for item in data:
        c.execute("INSERT INTO SalesData (Area_Code, State, Market, Market_Size, Profit, Margin, Sales, COGS, Total_Expenses, Marketing) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
            int(item['Area Code']), item['State'], item['Market'], item['Market Size'],
            float(item['Profit']), float(item['Margin']), float(item['Sales']),
            float(item['COGS']), float(item['Total Expenses']), float(item['Marketing'])
        ))

with open('sales_data2.csv', newline='') as csv_file: # файл исходных данных
    csv_reader = csv.reader(csv_file)
    next(csv_reader)
    for row in csv_reader:
        c.execute("INSERT INTO BudgetData VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
            float(row[0]), float(row[1]), float(row[2]), float(row[3]),
            float(row[4]), int(row[5]), row[6], row[7], row[8], row[9]
        ))

conn.commit()
conn.close()

# Повторное подключение к базе данных
conn = sqlite3.connect('clothing_sales.db')
c = conn.cursor()

# Выполнение запросов к базе данных

# Запрос 1: выборка с простым условием + сортировка + ограничение количества
c.execute("SELECT * FROM Table1 WHERE State = 'Connecticut' ORDER BY Profit DESC LIMIT 5")
query1_result = c.fetchall()
with open('result_query1.json', 'w') as file:
    json.dump(query1_result, file)

# Запрос 2: подсчет объектов по условию, а также другие функции агрегации
c.execute("SELECT State, COUNT(*) AS Count, AVG(Profit) AS AvgProfit FROM Table1 GROUP BY State")
query2_result = c.fetchall()
with open('result_query2.json', 'w') as file:
    json.dump(query2_result, file)

# Запрос 3: Получение средней прибыли по каждому рынку
c.execute("SELECT Market, AVG(Profit) AS AvgProfit FROM Table1 GROUP BY Market")
query3_result = c.fetchall()
with open('result_query3.json', 'w') as file:
    json.dump(query3_result, file)

# Запрос 4: обновление данных
c.execute("UPDATE Table3 SET Inventory = 1000 WHERE Budget_Profit > 500")

# Запрос 5: произвольный запрос (счёт общей прибыли)
c.execute("SELECT Market, SUM(Profit) AS TotalProfit FROM Table1 GROUP BY Market")
query5_result = c.fetchall()
with open('result_query5.json', 'w') as file:
    json.dump(query5_result, file)

# Запрос 6: произвольный запрос (счёт средней прибыли)
c.execute("SELECT Market, AVG(Profit) AS AvgProfit FROM Table1 GROUP BY Market")
query6_result = c.fetchall()
with open('result_query6.json', 'w') as file:
    json.dump(query6_result, file)

# Закрытие соединения с базой данных
conn.close()
