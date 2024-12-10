class FamilyMember:
    def __init__(self, relation, name, age, occupation, contact,empid):
        self.relation = relation
        self.empid=empid
        self.name = name
        self.age = age
        self.occupation = occupation
        self.contact = contact

    def to_dict(self):
        return {
            "relation": self.relation,
            "name": self.name,
            "empid": self.empid,
            "age": self.age,
            "occupation": self.occupation,
            "contact": self.contact
        }

class Address:
    def __init__(self, street, city, state, zipcode, country,empid):
        self.street = street
        self.city = city
        self.empid = empid
        self.state = state
        self.zipcode = zipcode
        self.country = country

    def to_dict(self):
        return {
            "street": self.street,
            "empid": self.empid,
            "city": self.city,
            "state": self.state,
            "zipcode": self.zipcode,
            "country": self.country
        }

class Employee:
    def __init__(self, empid, empname, salary, family, address):
        self.empid = empid
        self.empname = empname
        self.salary = salary
        self.family = [FamilyMember(**f) for f in family]
        self.address = [Address(**a) for a in address]

    def to_dict(self):
        return {
            "empid": self.empid,
            "empname": self.empname,
            "salary": self.salary,
            "family": [f.to_dict() for f in self.family],
            "address": [a.to_dict() for a in self.address]
        }
    

class Employee2:
    def __init__(self, empid, empname, salary):
        self.empid = empid
        self.empname = empname
        self.salary = salary
    
    def to_dict(self):
        return {
            "empid": self.empid,
            "empname": self.empname,
            "salary": self.salary
        }
