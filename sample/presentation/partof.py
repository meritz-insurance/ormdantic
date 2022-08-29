from typing import ClassVar, List
import ormdantic as od
from datetime import date

class Person(od.PersistentModel, od.PartOfMixin['Company']):
    name:od.StringIndex
    birth:date

class Company(od.PersistentModel):
    name: od.StringIndex
    code: od.IntIndex
    
    members: List[Person]

od.update_forward_refs(Person, locals())

_config = {
    'user':'orm',
    'password':'iamroot',
    'database':'json_storage',
    'host':'localhost',
    'port':33069
}

pool = od.DatabaseConnectionPool(_config)
od.create_table(pool, Company)

apple_company = Company.parse_obj({
    'code': 32,
    'name': 'Apple', 
    'members':[
        {'name':'Steve Jobs', 'birth':'1955-02-24'},
        {'name':'Steve Wozniak', 'birth':'1950-09-11'},
    ]
})

od.upsert_objects(pool, apple_company)
