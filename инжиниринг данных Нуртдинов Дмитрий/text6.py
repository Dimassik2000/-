import requests

url = "https://jsonplaceholder.typicode.com/posts"

response = requests.get(url)

if response.status_code == 200:

    data = response.json()

    html = "<ul>"
    for item in data:
        html += f"<li>{item['title']}</li>"
    html += "</ul>"

    print(html)
else:
    print("Ошибка при запросе данных")