from bs4 import BeautifulSoup
import numpy as np
import json


def get_num_stat(selector: str, items: list):
    nums = list(map(lambda x: int(x[selector]), items))

    stat = {}

    stat['sum'] = sum(nums)
    stat['min'] = min(nums)
    stat['max'] = max(nums)
    stat['avg'] = np.average(nums)
    stat['std'] = np.std(nums)

    return stat


def get_freq(selector: str, items: list):
    freq = {}

    for item in items:
        if selector in item:
            freq[item[selector]] = freq.get(item[selector], 0) + 1

    return freq


def handle_file(file_name):
    items = list()

    with open(file_name, encoding="utf-8") as file:
        text = ""
        for row in file.readlines():
            text += row

        root = BeautifulSoup(text, 'xml')

        for clothing in root.find_all("clothing"):
            item = dict()
            for el in clothing.contents:
                if el.name is None:
                    continue
                elif el.name == "price" or el.name == "reviews":
                    item[el.name] = int(el.get_text().strip())
                elif el.name == "rating":
                    item[el.name] = float(el.get_text().strip())
                elif el.name == "new":
                    item[el.name] = el.get_text().strip() == "+"
                elif el.name == "exclusive" or el.name == "sporty":
                    item[el.name] = el.get_text().strip() == "yes"
                else:
                    item[el.name] = el.get_text().strip()

            items.append(item)

        return items


items = []
for i in range(1, 101):
    file_name = f"4 задание/{i}.xml"
    items += handle_file(file_name)

items = sorted(items, key=lambda x: x['price'], reverse=True)

with open("4 задание/result_all4.json", 'w', encoding="utf-8") as f:
    f.write(json.dumps(items))

filtered_items = []
for item in items:
    if item['rating'] >= 4:
        filtered_items.append(item)

with open("4 задание/result_all4.json", 'w', encoding="utf-8") as f:
    f.write(json.dumps(filtered_items))

num_stat = get_num_stat("price", items)

print(num_stat)

title_freq = get_freq("size", items)

print(title_freq)