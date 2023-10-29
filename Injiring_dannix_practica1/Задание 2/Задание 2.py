file_path = r'text_2_var_34'
output_file_path = r'output2.txt'

with open(file_path, 'r') as file:
    lines = file.readlines()

sums = []
for line in lines:
    line_sum = 0
    line_numbers = line.split("/")
    for number in line_numbers:
        line_sum += int(number)
    sums.append(line_sum)

with open(output_file_path, 'w') as file:
    for summ in sums:
        file.write(str(summ) + "\n")