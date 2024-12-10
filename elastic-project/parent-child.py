import json
from elasticsearch import Elasticsearch, helpers
from model.model import Employee,Address,FamilyMember
import csv

def load_data(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)

    employees = []
    family_members = []
    addresses = []

    for item in data:
        employee = Employee(
            empid=item['empid'],
            empname=item['empname'],
            salary=item['salary']
        )
        employees.append(employee)

        for f in item['family']:
            family_member = FamilyMember(**f)
            family_members.append(family_member)

        for a in item['address']:
            address = Address(**a)
            addresses.append(address)

    return employees, family_members, addresses

def load_csv_files():
    employees = []
    family_members = []
    addresses = []

    employee_file_path="./data/employee.csv"
    family_file_path="./data/family.csv"
    address_file_path="./data/address.csv"
    with open(employee_file_path,newline='',encoding='utf-8') as f:
        reader=csv.DictReader(f)
        for row in reader:
            employee=Employee(
                id=row['id'],
                empid=row['empid'],
                empname=row['empname'],
                salary=row['salary']
            )
            employees.append(employee)

    with open(family_file_path,newline='',encoding='utf-8') as f:
        reader=csv.DictReader(f)
        for row in reader:
            member=FamilyMember(
                id=row['id'],
                empid=row['empid'],
                relation=row['relation'],
                name=row['name'],
                age=row['age'],
                occupation=row['occupation'],
                contact=row['contact'],
            )
            family_members.append(member)

    with open(address_file_path,newline='',encoding='utf-8') as f:
        reader=csv.DictReader(f)
        for row in reader:

            address=Address(
                id = row["id"],
                city = row["city"],
                empid = row["empid"],
                street=row["street"],
                state = row["state"],
                zipcode = row["zipcode"],
                country = row["country"],
                contact = row["contact"]
            )
            print(row)
            addresses.append(address)
    return employees,family_members,addresses

es = Elasticsearch([{'host': '192.168.1.28', 'port': 9301, 'scheme': 'http'}])

index_name = 'employee-parent-child4'
file_path = 'employee_nested_data.json'

def generate_bulk_data_for_parent_child(employees, family_members, addresses, index_name):
    
    if not es.indices.exists(index=index_name):
        mappings = {
            "mappings": {
                "properties": {
                    "empid": {"type": "keyword"},
                    "empname": {"type": "text"},
                    "salary": {"type": "float"},
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


    for family_member in family_members:
        yield {
            "_op_type": "index",
            "_index": index_name,
            "_routing": family_member.empid,  
            "_source": {
                "empid": family_member.empid,
                "relation": family_member.relation,
                "name": family_member.name,
                "age": family_member.age,
                "occupation": family_member.occupation,
                "contact": family_member.contact,
                "emp-join-field": {"name": "family", "parent": family_member.empid}
            }
        }


    for address in addresses:
        yield {
            "_op_type": "index",
            "_index": index_name,
            "_routing": address.empid,  
            "_source": {
                "empid":address.empid,
                "street": address.street,
                "city": address.city,
                "state": address.state,
                "zipcode": address.zipcode,
                "country": address.country,
                "emp-join-field": {"name": "address", "parent": address.empid}
            }
        }

def generate_bulk_data_for_nested(employees, family_members, addresses, index_name):
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

    for employee in employees:
        yield {
            "_op_type": "index",
            "_index": index_name,
            "_id": employee.empid,
            "_source": {
                "empid": employee.empid,
                "empname": employee.empname,
                "salary": employee.salary,
                "family": [],
                "address": []
            }
        }

    for family_member in family_members:
        yield {
            "_op_type": "update",  
            "_index": index_name,
            "_id": family_member.empid,  
            "script": {
                "source": """
                    if (ctx._source.family == null) {
                        ctx._source.family = [];
                    }
                    ctx._source.family.add(params.family);
                """,
                "params": {
                    "family": {
                        "relation": family_member.relation,  
                        "name": family_member.name,
                        "age": family_member.age,
                        "occupation": family_member.occupation,
                        "contact": family_member.contact
                    }
                }
            }
        }

    for address in addresses:
        yield {
            "_op_type": "update",  
            "_index": index_name,
            "_id": address.empid,  
            "script": {
                "source": """
                    if (ctx._source.address == null) {
                        ctx._source.address = [];
                    }
                    ctx._source.address.add(params.address);
                """,
                "params": {
                    "address": {
                        "street": address.street,  
                        "city": address.city,
                        "state": address.state,
                        "zipcode": address.zipcode,
                        "country": address.country
                    }
                }
            }
        }

try:
#    employees, family_members, addresses = load_data(file_path)
    employees, family_members, addresses = load_csv_files()
    # helpers.bulk(es, generate_bulk_data_for_parent_child(employees, family_members, addresses, index_name))
    helpers.bulk(es, generate_bulk_data_for_nested(employees, family_members, addresses, "employee-nested4"))
    print("Data indexed successfully!")
except Exception as e:
    print(f"Error indexing data: {e}")
