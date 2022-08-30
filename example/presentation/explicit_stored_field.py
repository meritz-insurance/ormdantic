from typing import ClassVar, List
import ormdantic as od
from datetime import date

class Member(od.SchemaBaseModel):
    name:str
    birth:date

class Company(od.PersistentModel):
    _stored_fields : ClassVar[od.StoredFieldDefinitions] = {
        "_members_name": (('$.members[*].name',), od.StringArrayIndex)
    }
    name: str
    code: od.IntIndex
    
    members: List[Member]

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
