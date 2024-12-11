
import json
from elasticsearch import Elasticsearch, helpers
from employee import Employee  


es = Elasticsearch([{'host': '192.168.1.28', 'port': 9301, 'scheme': 'http'}])

index_name = 'employee-nested3'
file_path = 'employee_nested_data.json' 

def load_data(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    
    employees = []
    for item in data:
        employee = Employee(
            empid=item['empid'],
            empname=item['empname'],
            salary=item['salary'],
            family=item['family'],
            address=item['address']
        )
        employees.append(employee)
    
    return employees


if not es.indices.exists(index=index_name):
    mappings = {
        "mappings": {
            "properties": {
                "empid": {
                    "type": "keyword"
                },
                "family": {
                    "type": "nested"
                },
                "address": {
                    "type": "nested"
                }
            }
        }
    }
    es.indices.create(index=index_name, body=mappings)
    print(f"Index {index_name} created successfully.")


def generate_bulk_data(employees, index_name):
    for employee in employees:
        yield {
            "_index": index_name,
            "_source": employee.to_dict()
        }

try:
    employees = load_data(file_path)
    helpers.bulk(es, generate_bulk_data(employees, index_name))
    print("Data indexed successfully!")
except Exception as e:
    print(f"Error indexing data: {e}")