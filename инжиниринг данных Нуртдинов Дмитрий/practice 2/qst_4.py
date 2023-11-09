import json
import pickle

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

with open('4_products_20.pkl', 'rb') as f:
    products = pickle.load(f)

print(products)

with open('products_result.json') as f:
    price_info = json.load(f)

print(price_info)

price_info_dict = dict()
name={
    "name": "Apple" ,
    "method": "add",
    "param": 4
}


for item in price_info:
    price_info_dict[item["name"]] = item

print (products)

for product in products:
    if product["name"] in price_info_dict:
        current_price_info = price_info_dict[product["name"]]
        if "method" in current_price_info:
            update_price(product, current_price_info)

print(products)

with open ('products_updated.pkl', 'wb') as f:
    f.write(pickle.dumps(products))
