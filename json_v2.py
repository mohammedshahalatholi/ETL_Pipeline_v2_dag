import json
import requests

data = [
    {
        "list_item": "ItemA",
        "total_sales": 1700,
        "total_orders": 42
    }, {
        "list_item": "ItemB",
        "total_sales": 3800,
        "total_orders": 77
    }
]

dict={"name": "example", "data": [1,2,3,4]}
print(dict)
json_data = json.dumps(dict)
print(json_data)

json_data=json.loads(json_data)
print("ists a dct data",json_data)

response = requests.get("https://jsonplaceholder.typicode.com/todos/")

todo_dict=json.loads(response.text)
print(todo_dict)

for todo in todo_dict:
    # data=json.dumps(todo)
    # print(data)
    print(f"Todo ID: {todo['id']}, Title: {todo['title']}, Completed: {todo['completed']}")