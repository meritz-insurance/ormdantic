# orm-(py)dantic

Managing the persistent Python object which was defined by Pydantic. 

 * save the Python object on database 
 * retrieve the Python object from database.


## Example

``` python
from typing import List, ClassVar
from datetime import date

import ormdantic as od

class Person(od.PersistentModel, od.PartOfMixin['Company']):
    _stored_fields: ClassVar[od.StoredFieldDefinitions] = {
        '_company_address': (('..', '$.address'), od.StringIndex)
    }

    name: od.StringIndex
    birth_date : date


class Company(od.IdentifiedModel):
    address: od.FullTextSearchedStringIndex
    members: List[Person]

od.update_part_of_forward_refs(Person, locals())

pool = od.DatabaseConnectionPool({
    'user':'orm',
    'password': 'iamroot',
    'database': 'json_storage',
    'host':'localhost',
    'port':3306
})

od.create_table(pool, Company)


apple_company = Company.parse_obj({
    'address':'California', 
    'members':[
        {'name':'Steve Jobs', 'birth_date':'1955-02-24'},
        {'name':'Steve Wozniak', 'birth_date':'1950-09-11'},
    ]
})

apple_company = od.upsert_objects(pool, apple_company)

apple_company.members[1].birth_date = date(1950, 8, 11)

od.upsert_objects(pool, apple_company)

companies_in_california = od.find_objects(pool, Company, (('address', 'match', '+California'),))

persons = od.find_objects(pool, Person, (('name', 'like', '%Stev%'),))

```
