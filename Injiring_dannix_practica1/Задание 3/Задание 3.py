file_path = r'text_3_var_34'
output_file_path = r'output3.txt'

with open(file_path, 'r') as file:
    lines = file.readlines()

with open(output_file_path, 'w') as file:
    for line in lines:
        line_values = line.strip().split(",")
        for i in range(len(line_values)):
            if line_values[i] == "NA":
                prev_num = float(line_values[i-1])
                next_num = float(line_values[i+1])
                line_values[i] = str((prev_num + next_num) / 2)
        result = []
        for num in line_values:
            if float(num) ** 0.5 >= 16 + 50:
                result.append(num)
        for num in result:
            file.write(str(num) + '\n')