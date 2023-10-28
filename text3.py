file_path = r'text_3_var_20'
output_file_path = r'C:\Users\Dmitr\OneDrive\Рабочий стол\Инжиниринг Данных\вывод данных\output1.txt'

with open(file_path, 'r') as file:
    lines = file.readlines()

import math

def read_file(file_path):
    data = []
    with open(file_path, "r") as file:
        for line in file:
            line = line.strip()
            data.append(line.split(","))
    return data

def replace_na_values(data):
    for i in range(len(data)):
        for j in range(len(data[i])):
            if data[i][j] == "NA":
                left = right = j

                # Ищем левое ненулевое значение
                while left >= 0 and data[i][left] == "NA":
                    left -= 1

                # Ищем правое ненулевое значение
                while right < len(data[i]) and data[i][right] == "NA":
                    right += 1

                if left >= 0 and right < len(data[i]):
                    # Рассчитываем среднее значение
                    average = (int(data[i][left]) + int(data[i][right])) / 2
                    data[i][j] = str(average)

def filter_values(data):
    threshold = 50
    result = []
    for row in data:
        filtered_row = [value for value in row if value != "NA" and math.sqrt(float(value)) >= threshold]
        result.append(",".join(filtered_row))
    return result

def write_file(file_path, data):
    with open(file_path, "w") as file:
        for row in data:
            file.write(row + "\n")

file_path = r'text_3_var_20'
output_file_path = r'C:\Users\Dmitr\OneDrive\Рабочий стол\Инжиниринг Данных\вывод данных\output1.txt'

data = read_file(file_path)
replace_na_values(data)
filtered_data = filter_values(data)
write_file(output_file_path, filtered_data)