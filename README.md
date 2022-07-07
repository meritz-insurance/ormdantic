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

persons = od.find_objects(pool, Person, (('name', 'like', '%Stev'),))

```

## StoredMixin
StoredMixin indicate the json key the value of which will be saved on the database field.

StringIndex, FullTextSearchedStringIndex is derived from StoredMixin. 
The field which is declared as \*Index, ormdantic make the fields which hold the value and create the index also.

The table will be defined as following if you will use the StoredMxin.

``` python
class SampleModel(PersistentModel):
   name: StringIndex
```

The model will generate following the sql.

``` sql
CREATE TABLE IF NOT EXISTS `model_SampleModel` (
  `__row_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `__json` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin CHECK (JSON_VALID(`__json`)),
  `name` VARCHAR(200) AS (JSON_VALUE(`__json`, '$.name')) STORED,  
  KEY `name_index` (`name`)
)
```

## Explicit Stored Fields

## StringArrayIndex

## offset, limit and PartOfMixin

## Reference 
## Note
 * implemented for mariadb only.
 * support python 3.10+ only.

## TODO

 * reduce join if it is not necessary.
   if there is not limit, we don't need to join table with base table.

 * where support partof stored fields
   - wheres = ('persons.name', '=', 'steve')

 * cache
   - pydantic object validation is a little bit slow.
   - raw dictionary or validated python object.

 * derived class, cache, audit, bitemporal

 * separated 'part of' object
   - shared by content. 

 * shadow model which is duplicated or can be generated.

  * detect the fields is not existed in model.

