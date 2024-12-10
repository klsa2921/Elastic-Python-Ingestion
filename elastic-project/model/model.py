class FamilyMember:
    def __init__(self, id,relation, name, age, occupation, contact,empid):
        self.id=id
        self.relation = relation
        self.empid=empid
        self.name = name
        self.age = age
        self.occupation = occupation
        self.contact = contact

class Address:
    def __init__(self, id,street, city, state, zipcode, country,contact,empid):
        self.street = street
        self.id = id
        self.city = city
        self.empid = empid
        self.state = state
        self.zipcode = zipcode
        self.country = country
        self.contact = contact

class Employee:
    def __init__(self,id, empid, empname, salary):
        self.id=id
        self.empid = empid
        self.empname = empname
        self.salary = salary