import json
import pickle

file1 = r'price_info_34.json' # укажите свой путь
file2 = r'products_34.pkl' # укажите свой путь
output = r'products_updated.pkl' # укажите свой путь

def update_price(product, price_info):
    method = price_info["method"]
    if method == "sum":
        product["price"] += price_info["param"]
    elif method == "sub":
        product["price"] -= price_info["param"]
    elif method == "percent+":
        product["price"] *= (1 + price_info["param"])
    elif method == "percent-":
        product["price"] *= (1 - price_info["param"])
    product["price"] = round(product["price"], 2)


with open(file2, "rb") as f:
    products = pickle.load(f)

with open(file1) as f:
    price_info = json.load(f)

price_info_dict = dict()

for item in price_info:
    price_info_dict[item["name"]] = item

for product in products:
    current_price_info = price_info_dict[product["name"]]
    update_price(product, current_price_info)

with open(output, "wb") as f:
    f.write(pickle.dumps(products))