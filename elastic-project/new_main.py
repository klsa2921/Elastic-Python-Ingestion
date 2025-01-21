import json
from elasticsearch import Elasticsearch, helpers
from model.model import Employee,Address,FamilyMember,Salary,JoiningDate,Leaves,Employee2,Salary2
import csv
from run_querys import run_query_1,run_query_2,run_query_3
from test import compare_results

def load_data(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)

    employees = []
    family_members = []
    addresses = []
    salary=[]
    joiningDate=[]

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
    salarys=[]
    joiningDate=[]
    leave_records=[]
    employee_file_path="./new_data/employee.csv"
    family_file_path="./new_data/family.csv"
    address_file_path="./new_data/address.csv"
    salary_file_path="./new_data/salary.csv"
    joining_file_path="./new_data/joiningDate.csv"
    # leave_file_path="./new_data/employee_leaves.csv"
    leave_file_path="./new_data/updated_employee_leaves.csv"

    employee_json_file_path="./new_data/empjson.json"

    # with open(employee_file_path,newline='',encoding='utf-8') as f:
    #     reader=csv.DictReader(f)
    #     for row in reader:
    #         employee=Employee(
    #             id=row['id'],
    #             empid=row['empid'],
    #             empname=row['empname'],
    #             tablename='employee'
    #         )
    #         employees.append(employee)

    with open(employee_file_path,newline='',encoding='utf-8') as f:
        with open(employee_json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            print("processing employee data")
            for item in data['employees']:
                employee = Employee2(
                    id=item['id'],
                    empid=item['empid'],
                    empname=item['empname'],
                    role='#'.join(item['role']),
                    tablename='employee'
                )
                employees.append(employee)
            print("processing salary data")
            for item in data["salarys"]:
                salary=Salary2(
                    id=item["salaryid"],
                    empid=item["empid"],
                    salary=int(item["amount"]),
                    role='#'.join(item["role"]),
                    tablename='salary'
                )
                salarys.append(salary)

            print("processing joining date data")
            for item in data["joiningDates"]:
                date=JoiningDate(
                    id=item["id"],
                    empid=item["empid"],
                    joinDate=item["joinDate"],
                    tablename='joinDate'
                )
                joiningDate.append(date)
            print("processing leave data")
            for item in data["leaves"]:
                leave=Leaves(
                    id=item["id"],
                    empid=item["empid"],
                    leave_type=item["leave_type"],
                    leave_start_date=item["leave_start_date"],
                    leave_end_date=item["leave_end_date"],
                    number_of_days=item["number_of_days"],
                    leave_status=item["leave_status"],
                    tablename='leaves'
                )
                leave_records.append(leave)
            print("processing address data")
            for item  in data["addresses"]:
                address=Address(
                    id=item["id"],
                    city=item["city"],
                    empid=item["empid"],
                    street=item["street"],
                    state=item["state"],
                    zipcode=item["zipcode"],
                    country=item["country"],
                    contact=item["contact"],
                    tablename='address'
                )
                addresses.append(address)

            print("processing family data")
            for item in data["families"]:
                member=FamilyMember(
                    id=item["id"],
                    empid=item["empid"],
                    relation=item["relation"],
                    name=item["name"],
                    age=item["age"],
                    occupation=item["occupation"],
                    contact=item["contact"],
                    tablename='family'
                )
                family_members.append(member)
            

    # with open(family_file_path,newline='',encoding='utf-8') as f:
    #     reader=csv.DictReader(f)
    #     for row in reader:
    #         member=FamilyMember(
    #             id=row['id'],
    #             empid=row['empid'],
    #             relation=row['relation'],
    #             name=row['name'],
    #             age=row['age'],
    #             occupation=row['occupation'],
    #             contact=row['contact'],
    #             tablename='family'
    #         )
    #         family_members.append(member)

    # with open(address_file_path,newline='',encoding='utf-8') as f:
    #     reader=csv.DictReader(f)
    #     for row in reader:

    #         address=Address(
    #             id = row["id"],
    #             city = row["city"],
    #             empid = row["empid"],
    #             street=row["street"],
    #             state = row["state"],
    #             zipcode = row["zipcode"],
    #             country = row["country"],
    #             contact = row["contact"],
    #             tablename='address'
    #         )
    #         addresses.append(address)
    
    # with open(salary_file_path,newline='',encoding='utf-8') as f:
    #     reader=csv.DictReader(f)
    #     print("opening file")
    #     for row in reader:
    #         salary=Salary(
    #             id = row["id"],
    #             empid = row["empid"],
    #             salary=int(row["salary"]),
    #             tablename='salary'
    #         )
    #         salarys.append(salary)
    
    # with open(salary_file_path,newline='',encoding='utf-8') as f:
    #     reader=csv.DictReader(f)
    #     print("opening file")
    #     for row in reader:
    #         salary=Salary(
    #             id = row["id"],
    #             empid = row["empid"],
    #             salary=int(row["salary"]),
    #             tablename='salary'
    #         )
    #         salarys.append(salary)

    # with open(joining_file_path,newline='',encoding='utf-8') as f:
    #     reader=csv.DictReader(f)
    #     for row in reader:

    #         date=JoiningDate(
    #             id = row["id"],
    #             empid = row["empid"],
    #             joinDate=row["joinDate"],
    #             tablename='joinDate'
    #         )
    #         joiningDate.append(date)

    # with open(leave_file_path, newline='', encoding='utf-8') as f:
    #     reader = csv.DictReader(f)
    #     for row in reader:
    #         leave = Leaves(
    #             id=row["id"],
    #             empid=row["empid"],
    #             leave_type=row["leave_type"],
    #             leave_start_date=row["leave_start_date"],
    #             leave_end_date=row["leave_end_date"],
    #             number_of_days=row["number_of_days"],
    #             leave_status=row["leave_status"],
    #             tablename='leaves'
    #         )
    #         leave_records.append(leave)
    
    
    return employees,family_members,addresses,salarys,joiningDate,leave_records

es = Elasticsearch([{'host': '192.168.1.27', 'port': 9200, 'scheme': 'http'}])
# es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])


parent_child_index = 'employee-parent-child25'

nested_index='employee-nested10'
file_path = 'employee_nested_data.json'

def generate_bulk_data_for_parent_child(employees, family_members, addresses,salarys,joiningDate,leave_records, index_name):
    
    if not es.indices.exists(index=index_name):
        mappings = {
            "mappings": {
                "properties": {
                    "empid": {"type": "keyword"},
                    "empname": {"type": "text"},
                    "emp-join-field": {
                        "type": "join",
                        "relations": {
                            "employee": ["address", "family","salary","joiningDate","leaves"]
                        }
                    }
                }
            }
        }
        es.indices.create(index=index_name, body=mappings)
        print(f"Index {index_name} created successfully.")

    # for employee in employees:
    
    #     yield {
    #         "_op_type": "index",  
    #         "_index": index_name,
    #         "_id": employee.empid,  
    #         "_source": {
    #             "empid": employee.empid,
    #             "empname": employee.empname,
    #             "tablename":employee.tablename,
    #             "emp-join-field": {"name": "employee"}
    #         }
    #     }
    
    for employee in employees:
    
        yield {
            "_op_type": "index",  
            "_index": index_name,
            "_id": employee.empid,  
            "_source": {
                "empid": employee.empid,
                "empname": employee.empname,
                "role":employee.role,
                "tablename":employee.tablename,
                "emp-join-field": {"name": "employee"}
            }
        }

    # for employee in employees:
    #     yield {
    #         "_op_type": "index",  
    #         "_index": index_name,
    #         "_id": employee.empid,  
    #         "_source": {
    #             "id": employee.id,
    #             "empid": employee.empid,
    #             "empname": employee.empname,
    #             "role": employee.role,
    #             "tablename": employee.tablename,
    #             "emp-join-field": {"name": "employee"}
    #         }
    #     }

    print("Processing family data")
    for family_member in family_members:
        yield {
            "_op_type": "index",
            "_index": index_name,
            "_routing": family_member.empid,
            "_id":family_member.id,  
            "_source": {
                "empid": family_member.empid,
                "relation": family_member.relation,
                "name": family_member.name,
                "age": family_member.age,
                "occupation": family_member.occupation,
                "contact": family_member.contact,
                "tablename":family_member.tablename,
                "emp-join-field": {"name": "family", "parent": family_member.empid}
            }
        }

    print("Processing address data")
    for address in addresses:
        yield {
            "_op_type": "index",
            "_index": index_name,
            "_routing": address.empid,
            "_id":address.id,  
            "_source": {
                "empid":address.empid,
                "street": address.street,
                "city": address.city,
                "state": address.state,
                "zipcode": address.zipcode,
                "country": address.country,
                "tablename":address.tablename,
                "emp-join-field": {"name": "address", "parent": address.empid}
            }
        }
    print("Processing salary data")
    try:
        for salary in salarys:
            yield {
                "_op_type": "index",
                "_index": index_name,
                "_routing": salary.empid,
                "_id": salary.id,
                "_source": {
                    "id": salary.id,
                    "empid": salary.empid,
                    "salary": salary.salary,
                    "role": salary.role,
                    "tablename": salary.tablename,
                    "emp-join-field": {"name": "salary", "parent": salary.empid}
                }
            }
    except Exception as e:
        print(f"Error processing salary data: {e}")

    print("Processing join data")
    for joinDate in joiningDate:
        yield{
            "_op_type": "index",
            "_index": index_name,
            "_routing": joinDate.empid,  
            "_id":joinDate.id,
            "_source": {
                "id":joinDate.id,
                "empid": joinDate.empid,
                "joinDate":joinDate.joinDate,
                "tablename":joinDate.tablename,
                "emp-join-field": {"name": "joiningDate", "parent": joinDate.empid}
            }
        }
    print("Processing leave data")
    for leave in leave_records:
        yield {
            "_op_type": "index",  # Operation type for Elasticsearch
            "_index": index_name,  # Index name
            "_routing": leave.empid,
            "_id":leave.id,  # Route by employee ID
            "_source": {
                "id": leave.id,  # Leave ID
                "empid": leave.empid,  # Employee ID
                "leave_type": leave.leave_type,  # Type of leave
                "leave_start_date": leave.leave_start_date,  # Start date of the leave
                "leave_end_date": leave.leave_end_date,  # End date of the leave
                "number_of_days":int(leave.number_of_days),
                "leave_status": leave.leave_status,  # Status of the leave
                "tablename":leave.tablename,
                "emp-join-field": {"name": "leaves", "parent": leave.empid}  # Linking leave to employee
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

    # for address in addresses:
    #     yield {
    #         "_op_type": "update",  
    #         "_index": index_name,
    #         "_id": address.empid,  
    #         "script": {
    #             "source": """
    #                 if (ctx._source.address == null) {
    #                     ctx._source.address = [];
    #                 }
    #                 ctx._source.address.add(params.address);
    #             """,
    #             "params": {
    #                 "address": {
    #                     "street": address.street,  
    #                     "city": address.city,
    #                     "state": address.state,
    #                     "zipcode": address.zipcode,
    #                     "country": address.country
    #                 }
    #             }
    #         }
    #     }

def run_test_queries():
    # run_query_1(parent_child_index)
    # print("/n")
    # run_query_2(parent_child_index)
    # print("/n")
    # run_query_3(parent_child_index)

    print("Comapring data ingested in both the indexes")
    compare_results(es,parent_child_index,nested_index)


def generate_bulk_data_for_documents(employees, family_members, addresses, salarys, joiningDate, leave_records, index_name):

    # Insert employees
    for employee in employees:
        yield {
            "_op_type": "index",  
            "_index": index_name,
            "_id": employee.empid,  
            "_source": {
                "empid": employee.empid,
                "empname": employee.empname,
                "tablename": employee.tablename,
            }
        }
    
    # Insert addresses (no parent-child relation)
    for address in addresses:
        yield {
            "_op_type": "index",
            "_index": index_name,
            "_id": address.id,  
            "_source": {
                "empid": address.empid,
                "street": address.street,
                "city": address.city,
                "state": address.state,
                "zipcode": address.zipcode,
                "country": address.country,
                "tablename": address.tablename,
            }
        }

    # Insert salaries (no parent-child relation)
    for salary in salarys:
        yield {
            "_op_type": "index",
            "_index": index_name,
            "_id": salary.id,  
            "_source": {
                "empid": salary.empid,
                "salary": salary.salary,
                "tablename": salary.tablename,
            }
        }

    # Insert joining dates (no parent-child relation)
    for joinDate in joiningDate:
        yield {
            "_op_type": "index",
            "_index": index_name,
            "_id": joinDate.id,  
            "_source": {
                "empid": joinDate.empid,
                "joinDate": joinDate.joinDate,
                "tablename": 'joinDate',
            }
        }

    # Insert leave records (no parent-child relation)
    for leave in leave_records:
        yield {
            "_op_type": "index",  # Operation type for Elasticsearch
            "_index": index_name,  # Index name
            "_id": leave.id,  # Leave ID as document ID
            "_source": {
                "empid": leave.empid,  # Employee ID
                "leave_type": leave.leave_type,  # Type of leave
                "leave_start_date": leave.leave_start_date,  # Start date of the leave
                "leave_end_date": leave.leave_end_date,  # End date of the leave
                "number_of_days": int(leave.number_of_days),  # Number of days of the leave
                "leave_status": leave.leave_status,  # Status of the leave
                "tablename": 'leaves',
            }
        }



# try:
#     # employees, family_members, addresses = load_data(file_path)
#     employees, family_members, addresses,salarys,joiningDate ,leave_records= load_csv_files()
#     # print(salarys)

#     #insert employees
#     # helpers.bulk(es, generate_bulk_data_for_documents(employees, family_members, addresses,salarys,joiningDate,leave_records,parent_child_index))

#     helpers.bulk(es, generate_bulk_data_for_parent_child(employees, family_members, addresses,salarys,joiningDate,leave_records,parent_child_index))
#     # helpers.bulk(es, generate_bulk_data_for_nested(employees, family_members, addresses, nested_index))
#     print("Data indexed successfully!")
# except Exception as e:
#     print(f"Error indexing data: {e}")




# run_test_queries()

try:
    employees, family_members, addresses, salarys, joiningDate, leave_records = load_csv_files()
    response = helpers.bulk(es, generate_bulk_data_for_parent_child(employees, family_members, addresses,salarys,joiningDate,leave_records,parent_child_index))
    print("Data indexed successfully!")
    print(f"Indexed {response[0]} documents successfully.")
    if response[1]:
        print(f"Failed to index {len(response[1])} documents.")
        for error in response[1]:
            print(error)
except Exception as e:
    print(f"Error indexing data: {e}")




