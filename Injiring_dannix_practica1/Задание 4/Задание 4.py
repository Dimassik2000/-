import pandas as pd

file_path = r'text_4_var_34'
output_file_path = r'output4.csv'

df = pd.read_csv(file_path, encoding='utf-8',names=['порядковый номер', 'имя', 'фамилия', 'возраст', 'доход', 'номер телефона'])

df = df.drop('номер телефона', axis=1)

df['доход'] = df['доход'].str.replace('₽', '').astype(float)

average_salary = df['доход'].mean()

filtered_df = df[(df['доход'] > average_salary) & (df['возраст'] > 25)]

sorted_df = filtered_df.sort_values('порядковый номер')

sorted_df.to_csv(output_file_path, index=False, header=False, encoding='utf-8-sig')

print(sorted_df)