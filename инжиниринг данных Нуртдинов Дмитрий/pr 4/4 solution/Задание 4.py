import pandas as pd
import sqlite3
import msgpack

csv_file_path = 'task_4_var_20_product_data.csv'
data = []
with open(csv_file_path, 'r', encoding='utf-8') as file:
    next(file)
    for line in file:
        parts = line.strip().split(';')
        if len(parts) == 6:
            data.append(parts)
df = pd.DataFrame(data, columns=['name', 'price', 'quantity', 'fromCity', 'isAvailable', 'views'])


msgpack_file_path = 'task_4_var_20_update_data.msgpack'
with open(msgpack_file_path, 'rb') as f:
    update_data = msgpack.unpack(f, raw=False)


conn = sqlite3.connect('product_data.db')
c = conn.cursor()

c.execute('DROP TABLE IF EXISTS products')


c.execute('''CREATE TABLE products
             (id INTEGER PRIMARY KEY,
              name TEXT,
              price REAL,
              quantity INTEGER,
              fromCity TEXT,
              isAvailable TEXT,
              views INTEGER)''')


for index, row in df.iterrows():
    is_available = 1 if row['isAvailable'] == 'True' else 0  # Преобразуем в 1 или 0
    c.execute('INSERT INTO products (name, price, quantity, fromCity, isAvailable, views) VALUES (?, ?, ?, ?, ?, ?)',
              (
              row['name'], float(row['price']), int(row['quantity']), row['fromCity'], is_available, int(row['views'])))


for update in update_data:
    product_name = update['name']
    if 'method' in update and 'param' in update:
        method = update['method']
        param = update['param']
        if method == 'price_abs':
            new_price = float(param)
            if new_price >= 0:
                c.execute('UPDATE products SET price = ? WHERE name = ?', (new_price, product_name))
        elif method == 'available':
            new_availability = 1 if param else 0
            c.execute('UPDATE products SET isAvailable = ? WHERE name = ?', (new_availability, product_name))


top_10_query = '''SELECT name, COUNT(*) AS update_count
                  FROM products
                  GROUP BY name
                  ORDER BY update_count DESC
                  LIMIT 10'''

c.execute(top_10_query)
top_10_products = c.fetchall()
print("Топ-10 самых обновляемых товаров:")
for row in top_10_products:
    print(row[0], "- количество обновлений:", row[1])
print()


price_analysis_query = '''SELECT name, SUM(price) AS total_price, MIN(price) AS min_price,
                          MAX(price) AS max_price, AVG(price) AS avg_price, COUNT(*) AS product_count
                          FROM products
                          GROUP BY name'''

c.execute(price_analysis_query)
price_analysis_results = c.fetchall()
print("Анализ цен товаров:")
for row in price_analysis_results:
    print("Товар:", row[0])
    print("Сумма цен товаров:", row[1])
    print("Минимальная цена:", row[2])
    print("Максимальная цена:", row[3])
    print("Средняя цена:", row[4])
    print("Количество товаров в группе:", row[5])
    print()


quantity_analysis_query = '''SELECT name, SUM(quantity) AS total_quantity, MIN(quantity) AS min_quantity,
                             MAX(quantity) AS max_quantity, AVG(quantity) AS avg_quantity, COUNT(*) AS product_count
                             FROM products
                             GROUP BY name'''

c.execute(quantity_analysis_query)
quantity_analysis_results = c.fetchall()
print("Анализ остатков товаров:")
for row in quantity_analysis_results:
    print("Товар:", row[0])
    print("Сумма остатков товаров:", row[1])
    print("Минимальный остаток:", row[2])
    print("Максимальный остаток:", row[3])
    print("Средний остаток:", row[4])
    print("Количество товаров в группе:", row[5])
    print()


custom_query = '''SELECT fromCity, COUNT(*) AS product_count
                  FROM products
                  GROUP BY fromCity
                  HAVING COUNT(*) > 1
                  ORDER BY product_count DESC'''

c.execute(custom_query)
custom_results = c.fetchall()
print("Произвольный запрос:")
for row in custom_results:
    print(row[0], "- количество товаров:", row[1])
print()

conn.commit()
conn.close()
