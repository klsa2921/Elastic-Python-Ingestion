import json
from elasticsearch import Elasticsearch, helpers
from employee import Employee  

es = Elasticsearch([{'host': '192.168.1.28', 'port': 9301, 'scheme': 'http'}])

index_name = 'employee-parent-child2'
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
                "empid": {"type": "keyword"},
                "empname": {"type": "text"},
                "emp-join-field": {
                    "type": "join",
                    "relations": {
                        "employee": ["address", "family"]
                    }
                }
            }
        }
    }
    es.indices.create(index=index_name, body=mappings)
    print(f"Index {index_name} created successfully.")


def generate_bulk_data(employees, index_name):
    for employee in employees:
        
        yield {
            "_op_type": "index",  
            "_index": index_name,
            "_id": employee.empid,  
            "_source": {
                "empid": employee.empid,
                "empname": employee.empname,
                "salary": employee.salary,
                "emp-join-field": {"name": "employee"}
            }
        }

        
        for family_member in employee.family:
            yield {
                "_op_type": "index",
                "_index": index_name,
                "_routing":employee.empid,
                "_source": {
                    "relation": family_member.relation,
                    "name": family_member.name,
                    "age": family_member.age,
                    "occupation": family_member.occupation,
                    "contact": family_member.contact,
                    "emp-join-field": {"name": "family", "parent": employee.empid}
                }
            }

        for address in employee.address:
            yield {
                "_op_type": "index",
                "_index": index_name,
                "_routing":employee.empid,
                "_source": {
                    "street": address.street,
                    "city": address.city,
                    "state": address.state,
                    "zipcode": address.zipcode,
                    "country": address.country,
                    "emp-join-field": {"name": "address", "parent": employee.empid}
                }
            }

try:
    employees = load_data(file_path)
    helpers.bulk(es, generate_bulk_data(employees, index_name))
    print("Data indexed successfully!")
except Exception as e:
    print(f"Error indexing data: {e}")
