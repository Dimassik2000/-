import pandas as pd
import sqlite3
import msgpack

def load_csv_data(csv_file_path):
    data = []
    with open(csv_file_path, 'r', encoding='utf-8') as file:
        next(file)
        for line in file:
            parts = line.strip().split(';')
            if len(parts) == 6:
                data.append(parts)
    return data

def load_msgpack_data(msgpack_file_path):
    with open(msgpack_file_path, 'rb') as f:
        update_data = msgpack.unpack(f, raw=False)
    return update_data

def create_table(conn, cursor):
    cursor.execute('DROP TABLE IF EXISTS products')

    cursor.execute('''CREATE TABLE products
                     (id INTEGER PRIMARY KEY,
                      name TEXT,
                      price REAL,
                      quantity INTEGER,
                      fromCity TEXT,
                      isAvailable TEXT,
                      views INTEGER)''')
    conn.commit()

def insert_data(conn, cursor, data):
    cursor.executemany('INSERT INTO products (name, price, quantity, fromCity, isAvailable, views) VALUES (?, ?, ?, ?, ?, ?)', data)
    conn.commit()

def apply_updates(conn, cursor, update_data):
    for update in update_data:
        product_name = update['name']
        if 'method' in update and 'param' in update:
            method = update['method']
            param = update['param']
            if method == 'price_abs':
                new_price = float(param)
                if new_price >= 0:
                    cursor.execute('UPDATE products SET price = ? WHERE name = ?', (new_price, product_name))
            elif method == 'available':
                new_availability = 1 if param else 0
                cursor.execute('UPDATE products SET isAvailable = ? WHERE name = ?', (new_availability, product_name))
    conn.commit()

def get_top_10_products(cursor):
    top_10_query = '''SELECT name, COUNT(*) AS update_count
                      FROM products
                      GROUP BY name
                      ORDER BY update_count DESC
                      LIMIT 10'''
    cursor.execute(top_10_query)
    top_10_products = cursor.fetchall()
    return top_10_products

def get_price_analysis_results(cursor):
    price_analysis_query = '''SELECT name, SUM(price) AS total_price, MIN(price) AS min_price,
                              MAX(price) AS max_price, AVG(price) AS avg_price, COUNT(*) AS product_count
                              FROM products
                              GROUP BY name'''
    cursor.execute(price_analysis_query)
    price_analysis_results = cursor.fetchall()
    return price_analysis_results

def get_quantity_analysis_results(cursor):
    quantity_analysis_query = '''SELECT name, SUM(quantity) AS total_quantity, MIN(quantity) AS min_quantity,
                                 MAX(quantity) AS max_quantity, AVG(quantity) AS avg_quantity, COUNT(*) AS product_count
                                 FROM products
                                 GROUP BY name'''
    cursor.execute(quantity_analysis_query)
    quantity_analysis_results = cursor.fetchall()
    return quantity_analysis_results

def get_custom_results(cursor):
    custom_query = '''SELECT fromCity, COUNT(*) AS product_count
                      FROM products

                      GROUP BY fromCity
                      HAVING COUNT(*) > 1
                      ORDER BY product_count DESC'''
    cursor.execute(custom_query)
    custom_results = cursor.fetchall()
    return custom_results

def process_data(csv_file_path, msgpack_file_path, db_file_path):
    data = load_csv_data(csv_file_path)
    updates = load_msgpack_data(msgpack_file_path)

    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    create_table(conn, cursor)
    insert_data(conn, cursor, data)
    apply_updates(conn, cursor, updates)

    top_10_products = get_top_10_products(cursor)
    with open(query_result_file_path, 'w') as file:
        file.write('Топ-10 самых обновляемых товаров:\n')
        for row in top_10_products:
            file.write(f'{row[0]} - количество обновлений: {row[1]}\n')
        file.write('\n')

        price_analysis_results = get_price_analysis_results(cursor)
        file.write('Анализ цен товаров:\n')
        for row in price_analysis_results:
            file.write(f'Товар: {row[0]}\n')
            file.write(f'Сумма цен товаров: {row[1]}\n')
            file.write(f'Минимальная цена: {row[2]}\n')
            file.write(f'Максимальная цена: {row[3]}\n')
            file.write(f'Средняя цена: {row[4]}\n')
            file.write(f'Количество товаров в группе: {row[5]}\n')
            file.write('\n')

        quantity_analysis_results = get_quantity_analysis_results(cursor)
        file.write('Анализ остатков товаров:\n')
        for row in quantity_analysis_results:
            file.write(f'Товар: {row[0]}\n')
            file.write(f'Сумма остатков товаров: {row[1]}\n')
            file.write(f'Минимальный остаток: {row[2]}\n')
            file.write(f'Максимальный остаток: {row[3]}\n')
            file.write(f'Средний остаток: {row[4]}\n')
            file.write(f'Количество товаров в группе: {row[5]}\n')
            file.write('\n')

        custom_results = get_custom_results(cursor)
        file.write('Произвольный запрос:\n')
        for row in custom_results:
            file.write(f'{row[0]} - количество товаров: {row[1]}\n')
        file.write('\n')

    conn.close()


# Пример использования функции process_data
csv_file_path = 'task_4_var_20_product_data.csv'
msgpack_file_path = 'task_4_var_20_update_data.msgpack'
db_file_path = 'product_data.db'
query_result_file_path = 'query_results.txt'

process_data(csv_file_path, msgpack_file_path, db_file_path)



