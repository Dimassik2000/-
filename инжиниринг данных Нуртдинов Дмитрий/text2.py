file_path = r'text_2_var_20'
output_file_path = r'C:\Users\Dmitr\OneDrive\Рабочий стол\Инжиниринг Данных\вывод данных\output.txt'

with open(file_path, 'r') as file:
    lines = file.readlines()

sums = []
for line in lines:
    line_sum = 0
    line_numbers = line.strip().split(".")  # Change the delimiter to "."
    for number in line_numbers:
        if number:
            line_sum += int(number)
    sums.append(line_sum)

with open(output_file_path, 'w') as file:
    for summ in sums:
        file.write(str(summ) + "\n")