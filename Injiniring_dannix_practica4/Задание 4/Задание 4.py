import json
import msgpack
import sqlite3


def apply_changes(cursor, changes):
    for change in changes:
        name = change['name']
        method = change['method']
        param = change['param']

        if method == 'available':
            cursor.execute("UPDATE products SET isAvailable = ? WHERE name = ?", (param, name))
        elif method == 'price_percent':
            cursor.execute("SELECT price FROM products WHERE name = ?", (name,))
            current_price = cursor.fetchone()[0]
            new_price = max(current_price * (1 + param), 0)  # Проверяем, чтобы цена не стала отрицательной
            cursor.execute("UPDATE products SET price = ? WHERE name = ?", (new_price, name))
        elif method == 'price_abs':
            cursor.execute("SELECT price FROM products WHERE name = ?", (name,))
            current_price = cursor.fetchone()[0]
            new_price = max(current_price + param, 0)  # Проверяем, чтобы цена не стала отрицательной
            cursor.execute("UPDATE products SET price = ? WHERE name = ?", (new_price, name))


def main():
    conn = sqlite3.connect('baza_dannix.db')
    cursor = conn.cursor()

    product_filename = r'task_4_var_34_product_data.text'
    with open(product_filename, 'r', encoding='utf-8') as product_file:
        product_data = product_file.read()

        products = product_data.split('=====\n')

        cursor.execute('''CREATE TABLE IF NOT EXISTS products
                          (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, price REAL, quantity INTEGER, category TEXT, fromCity TEXT, isAvailable BOOLEAN, views INTEGER)''')

        for product in products:
            product_info = product.split('\n')
            if len(product_info) >= 7:
                name = product_info[0].split('::')[1]
                price = float(product_info[1].split('::')[1])
                quantity = int(product_info[2].split('::')[1])
                category = product_info[3].split('::')[1]
                fromCity = product_info[4].split('::')[1]
                isAvailable = bool(product_info[5].split('::')[1])
                if len(product_info[6].split('::')) > 1:
                    views = int(product_info[6].split('::')[1])
                else:
                    views = 0

                cursor.execute(
                    "INSERT INTO products (name, price, quantity, category, fromCity, isAvailable, views) VALUES (?, ?, ?,?, ?, ?, ?)",
                    (name, price, quantity, category, fromCity, isAvailable, views))

    # Считываем и применяем изменения
    update_filename = r'task_4_var_34_update_data.msgpack'
    with open(update_filename, 'rb') as update_file:
        update_data = msgpack.unpackb(update_file.read())

        apply_changes(cursor, update_data)

    conn.commit()

    # Запросы для проверки результатов
    cursor.execute("SELECT * FROM products")
    results = cursor.fetchall()
    for result in results:
        print(result)

    apply_changes(cursor, update_data)

    conn.commit()

    # Запрос 1: Топ-10 самых обновляемых товаров
    cursor.execute(
        "SELECT name, COUNT(*) as update_count FROM products GROUP BY name ORDER BY update_count DESC LIMIT 10")
    top_updated_products = cursor.fetchall()
    print("Топ-10 самых обновляемых товаров:")
    for product in top_updated_products:
        print(product)

    # Запрос 2: Анализ цен товаров
    cursor.execute(
        "SELECT category, SUM(price) as total_price, MIN(price) as min_price, MAX(price) as max_price, AVG(price) as avg_price, COUNT(*) as product_count FROM products GROUP BY category")
    price_analysis = cursor.fetchall()
    print("Анализ цен товаров:")
    for category in price_analysis:
        print(category)

    # Запрос 3: Анализ остатков товаров
    cursor.execute(
        "SELECT category, SUM(quantity) as total_quantity, MIN(quantity) as min_quantity, MAX(quantity) as max_quantity, AVG(quantity) as avg_quantity, COUNT(*) as product_count FROM products GROUP BY category")
    quantity_analysis = cursor.fetchall()
    print("Анализ остатков товаров:")
    for category in quantity_analysis:
        print(category)

    # Запрос 4: Произвольный запрос
    cursor.execute("SELECT category, COUNT(*) as total_count FROM products GROUP BY category")
    category_counts = cursor.fetchall()
    print("Список категорий товаров и общее количество товаров в каждой категории:")
    for category in category_counts:
        print(category)


if __name__ == "__main__":
    main()
