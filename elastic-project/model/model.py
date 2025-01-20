class FamilyMember:
    def __init__(self, id,relation, name, age, occupation, contact,empid,tablename):
        self.id=id
        self.relation = relation
        self.empid=empid
        self.name = name
        self.age = age
        self.occupation = occupation
        self.contact = contact
        self.tablename=tablename

class Address:
    def __init__(self, id,street, city, state, zipcode, country,contact,empid,tablename):
        self.street = street
        self.id = id
        self.city = city
        self.empid = empid
        self.state = state
        self.zipcode = zipcode
        self.country = country
        self.contact = contact
        self.tablename=tablename

class Employee:
    def __init__(self,id, empid, empname,tablename):
        self.id=id
        self.empid = empid
        self.empname = empname
        self.tablename=tablename

class Employee2:
    def __init__(self,id, empid, empname,tablename,accesslist,accessnamelist):
        self.id=id
        self.empid = empid
        self.empname = empname
        self.accesslist=accesslist
        self.accessnamelist=accessnamelist
        self.tablename=tablename

class Salary:
    def __init__(self,id,empid,salary,tablename):
        self.id=id
        self.empid=empid
        self.salary=salary
        self.tablename=tablename

class JoiningDate:
    def __init__(self,id,empid,joinDate,tablename):
        self.id=id
        self.empid=empid
        self.joinDate=joinDate
        self.tablename=tablename

class Leaves:
    def __init__(self,id,empid,leave_type,leave_start_date,leave_end_date,leave_status,number_of_days,tablename):
        self.id=id
        self.empid=empid
        self.leave_type=leave_type
        self.leave_start_date=leave_start_date
        self.leave_end_date=leave_end_date
        self.leave_status=leave_status
        self.number_of_days=number_of_days
        self.tablename=tablename