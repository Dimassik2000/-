import requests
import json
from html import escape


def json_to_html(data):
    if 'title' in data and 'body' in data:
        title = escape(data['title'])
        body = escape(data['body'])
        return f"<h1>{title}</h1><p>{body}</p>"
    else:
        return "<p>No data available.</p>"


response = requests.get('https://jsonplaceholder.typicode.com/posts')
data = response.json()

html = ""
for post in data:
    html += json_to_html(post)

output = r'output6.html'

with open(output, 'w', encoding='utf-8') as file:
    file.write(html)
