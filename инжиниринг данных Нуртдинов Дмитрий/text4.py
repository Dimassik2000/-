import csv

aver_salary= 0
items= list()

with open ('text_4_var_20', newline='\n', encoding='utf-8') as file:
    reader = csv.reader(file, delimiter=',')
    for row in reader:
        print(row)
        item = {
            'number': int(row[0]),
            'name': row[2] + ' ' + row [1],
            'age': int(row[3]),
            'salary': int(row[4][0:-1])
        }

        aver_salary += item ['salary']
        items.append(item)
print (items)
aver_salary /= len (items)

filtered = list ()
for item in items:
    if (item ['salary']> aver_salary) and item ['age'] > 25:
        filtered.append(item)