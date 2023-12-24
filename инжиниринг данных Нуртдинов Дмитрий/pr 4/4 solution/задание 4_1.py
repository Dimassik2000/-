import sqlite3
import msgpack
import json

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
                cursor.execute('UPDATE products SET price = ? WHERE name = ?', (new_price, product_name))
            elif method == 'available':
                new_availability = 'В наличии' if param else 'Нет в наличии'
                cursor.execute('UPDATE products SET isAvailable = ? WHERE name = ?', (new_availability, product_name))
            elif method == 'views_increment':
                cursor.execute('UPDATE products SET views = views + 1 WHERE name = ?', (product_name))
            elif method == 'change_city':
                new_city = param
                cursor.execute('UPDATE products SET fromCity = ? WHERE name = ?', (new_city, product_name))
            elif method == 'toggle_availability':
                cursor.execute('SELECT isAvailable FROM products WHERE name = ?', (product_name))
                current_availability = cursor.fetchone()
                if current_availability:
                    new_availability = 'Нет в наличии' if current_availability[0] == 'В наличии' else 'В наличии'
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

def process_data_and_save_results(csv_file_path, msgpack_file_path, db_file_path, query_result_file_path):
    data = load_csv_data(csv_file_path)
    updates = load_msgpack_data(msgpack_file_path)

    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    create_table(conn, cursor)
    insert_data(conn, cursor, data)
    apply_updates(conn, cursor, updates)

    top_10_products = get_top_10_products(cursor)
    price_analysis_results = get_price_analysis_results(cursor)
    quantity_analysis_results = get_quantity_analysis_results(cursor)
    custom_results = get_custom_results(cursor)

    # Сохранение результатов в JSON
    # results = {
    #     "top_10_products": [{"name": row[0], "update_count": row[1]} for row in top_10_products],
    #     "price_analysis_results": [
    #         {"name": row[0], "total_price": row[1], "min_price": row[2], "max_price": row[3], "avg_price": row[4], "product_count": row[5]}
    #         for row in price_analysis_results
    #     ],
    #     "quantity_analysis_results": [
    #         {"name": row[0], "total_quantity": row[1], "min_quantity": row[2], "max_quantity": row[3], "avg_quantity": row[4], "product_count": row[5]}
    #         for row in quantity_analysis_results
    #     ],
    #     "custom_results": [{"fromCity": row[0], "product_count": row[1]} for row in custom_results]
    # }

    results = {
        "top_10_products": [dict(zip(["name", "update_count"], row)) for row in top_10_products],
        "price_analysis_results": [
            dict(zip(["name", "total_price", "min_price", "max_price", "avg_price", "product_count"], row))
            for row in price_analysis_results
        ],
        "quantity_analysis_results": [
            dict(zip(["name", "total_quantity", "min_quantity", "max_quantity", "avg_quantity", "product_count"], row))
            for row in quantity_analysis_results
        ],
        "custom_results": [dict(zip(["fromCity", "product_count"], row)) for row in custom_results]
    }

    with open(query_result_file_path, 'w', encoding='utf-8') as file:
        json.dump(results, file, indent=4, ensure_ascii=False)

    conn.close()

# Пример использования функции process_data_and_save_results
csv_file_path = 'task_4_var_20_product_data.csv'
msgpack_file_path = 'task_4_var_20_update_data.msgpack'
db_file_path = 'product_data.db'
query_result_file_path = 'query_results.json'  # Изменил расширение файла на .json

process_data_and_save_results(csv_file_path, msgpack_file_path, db_file_path, query_result_file_path)



