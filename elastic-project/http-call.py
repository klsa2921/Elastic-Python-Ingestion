import json
import requests
import csv
from model.model import Employee, Address, FamilyMember
from run_querys import run_query_1,run_query_2,run_query_3

ES_HOST = 'http://192.168.1.28:9301'
INDEX_NAME_PARENT_CHILD = 'employee-parent-child10'
INDEX_NAME_NESTED = 'employee-nested10'


def load_csv_files():
    employees = []
    family_members = []
    addresses = []

    employee_file_path = "./data/employee.csv"
    family_file_path = "./data/family.csv"
    address_file_path = "./data/address.csv"
    salary_file_path="./data/salary.csv"
    joindate_file_path="./data/joiningDate.csv"

    
    with open(employee_file_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            employee = Employee(
                id=row['id'],
                empid=row['empid'],
                empname=row['empname'],
                salary=row['salary']
            )
            employees.append(employee)

    with open(family_file_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            member = FamilyMember(
                id=row['id'],
                empid=row['empid'],
                relation=row['relation'],
                name=row['name'],
                age=row['age'],
                occupation=row['occupation'],
                contact=row['contact'],
            )
            family_members.append(member)

    with open(address_file_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            address = Address(
                id=row["id"],
                city=row["city"],
                empid=row["empid"],
                street=row["street"],
                state=row["state"],
                zipcode=row["zipcode"],
                country=row["country"],
                contact=row["contact"]
            )
            addresses.append(address)

    return employees, family_members, addresses


def create_index(index_name, mappings):
    url = f"{ES_HOST}/{index_name}"
    response = requests.put(url, json=mappings)
    if response.status_code == 200:
        print(f"Index {index_name} created successfully.")
    else:
        print(f"Error creating index: {response.text}")


parent_child_mappings = {
    "mappings": {
        "properties": {
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


nested_mappings = {
    "mappings": {
        "properties": {
            "empid": {"type": "keyword"},
            "family": {
                "type": "nested"
            },
            "address": {
                "type": "nested"
            }
        }
    }
}


def generate_bulk_data_for_parent_child(employees, family_members, addresses):
    actions = []

    # Process employee data
    for employee in employees:
        actions.append({
            "index": {  
                "_index": INDEX_NAME_PARENT_CHILD,
                "_id": str(employee.empid)
            }
        })
        actions.append({
            "empid": str(employee.empid),
            "empname": employee.empname,
            "salary": int(employee.salary),  # Ensure salary is an integer
            "emp-join-field": {"name": "employee"}
        })

    # Process family member data (child documents)
    for family_member in family_members:
        actions.append({
            "index": {  
                "_index": INDEX_NAME_PARENT_CHILD,
                "_routing": str(family_member.empid),  # Routing by employee ID
                "_id": str(family_member.empid)  # Add the employee ID for child document
            }
        })
        actions.append({
            "empid": str(family_member.empid),
            "relation": family_member.relation,
            "name": family_member.name,
            "age": int(family_member.age),  # Ensure age is an integer
            "occupation": family_member.occupation,
            "contact": family_member.contact,
            "emp-join-field": {"name": "family", "parent": str(family_member.empid)}
        })

    # Process address data (child documents)
    for address in addresses:
        actions.append({
            "index": { 
                "_index": INDEX_NAME_PARENT_CHILD,
                "_routing": str(address.empid),  # Routing by employee ID
                "_id": str(address.empid)  # Add the employee ID for child document
            }
        })
        actions.append({
            "empid": str(address.empid),
            "street": address.street,
            "city": address.city,
            "state": address.state,
            "zipcode": address.zipcode,
            "country": address.country,
            "emp-join-field": {"name": "address", "parent": str(address.empid)}
        })

    # Create bulk request body by joining all actions with line breaks
    bulk_request_body = "\n".join([json.dumps(action) for action in actions]) + "\n"

    print(bulk_request_body)

    url = f"{ES_HOST}/{INDEX_NAME_PARENT_CHILD}/_bulk"
    response = requests.post(url, data=bulk_request_body, headers={"Content-Type": "application/x-ndjson"})
    
    if response.status_code == 200:
        print("Parent-child data indexed successfully!")
    else:
        print(f"Error indexing parent-child data: {response.text}")


def generate_bulk_data_for_nested(employees, family_members, addresses):
    actions = []


    for employee in employees:
        actions.append({
            "index": {  
                "_index": INDEX_NAME_NESTED,
                "_id": str(employee.empid),
                "_source": {
                    "empid": str(employee.empid),
                    "empname": employee.empname,
                    "salary": employee.salary,
                    "family": [],
                    "address": []
                }
            }
        })


    for family_member in family_members:
        actions.append({
            "update": { 
                "_index": INDEX_NAME_NESTED,
                "_id": str(family_member.empid),  
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
        })


    for address in addresses:
        actions.append({
            "update": {
                "_index": INDEX_NAME_NESTED,
                "_id": str(address.empid),  
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
        })


    bulk_request_body = "\n".join([json.dumps(action) for action in actions]) + "\n"

    print(bulk_request_body)

    url = f"{ES_HOST}/{INDEX_NAME_NESTED}/_bulk"
    response = requests.post(url, data=bulk_request_body, headers={"Content-Type": "application/x-ndjson"})
    
    if response.status_code == 200:
        print("Nested data indexed successfully!")
    else:
        print(f"Error indexing nested data: {response.text}")


try:

    employees, family_members, addresses = load_csv_files()

    create_index(INDEX_NAME_PARENT_CHILD, parent_child_mappings)
    # create_index(INDEX_NAME_NESTED, nested_mappings)

    generate_bulk_data_for_parent_child(employees, family_members, addresses)
    # generate_bulk_data_for_nested(employees, family_members, addresses)

except Exception as e:
    print(f"Error: {e}")



# run_query_1(INDEX_NAME_PARENT_CHILD)
# print("/n")
# run_query_2(INDEX_NAME_PARENT_CHILD)
# print("/n")
# run_query_3(INDEX_NAME_PARENT_CHILD)