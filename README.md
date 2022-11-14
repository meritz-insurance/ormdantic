# orm-(py)dantic

Managing the persistent Python object which was defined by Pydantic. 

 * save the Python object on database 
 * retrieve the Python object from database.


## Example

``` python
from typing import List, ClassVar
from datetime import date

import ormdantic as od
from ormdantic.database.storage import create_table

class Person(od.PersistentModel, od.PartOfMixin['Company']):
    _stored_fields: ClassVar[od.StoredFieldDefinitions] = {
        '_company_address': (('..', '$.address'), od.StringIndex)
    }

    name: od.StringIndex
    birth : date


class Company(od.IdentifiedModel):
    address: od.FullTextSearchedStringIndex
    members: List[Person]


od.update_forward_refs(Person, locals())

pool = od.DatabaseConnectionPool({
    'user':'orm',
    'password': 'iamroot',
    'database': 'json_storage',
    'host':'localhost',
    'port':3306
})

create_table(pool, Company)

storage = od.create_database_source(pool, 0, date.today())

apple_company = Company.parse_obj({
    'address':'California', 
    'members':[
        {'name':'Steve Jobs', 'birth':'1955-02-24'},
        {'name':'Steve Wozniak', 'birth':'1950-09-11'},
    ]
})

apple_company = storage.store(apple_company, od.VersionInfo())
apple_company.members[1].birth = date(1950, 8, 11)

storage.store(apple_company, od.VersionInfo())

storage = storage.clone_with()

companies_in_california = storage.find(
    Company, {'address': ('match', '+California')})

persons = storage.query(
    Person, {'name': ('like', '%Steve%')})

```
