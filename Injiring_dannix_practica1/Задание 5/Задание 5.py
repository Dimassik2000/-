import csv
from bs4 import BeautifulSoup

file_path = r'text_5_var_34'
output_file_path = r'output5.csv'

with open(file_path, 'r') as file:
    html_data = file.read()

soup = BeautifulSoup(html_data, 'html.parser')

table_rows = soup.find_all('tr')

data = []

for row in table_rows:
    cells = row.find_all('td')
    data.append([cell.get_text(strip=True) for cell in cells])

with open(output_file_path, 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)

