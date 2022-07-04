# orm-(py)dantic

Managing the persistent Python object which was defined by Pydantic. 

 * save the Python object on database 
 * retrieve the Python object from database.


## Example

``` python
# define class which will be save
class Company(IdentifiedModel):
    address: FullTextSearchedStringIndex
    member: List['Person']
    
class Person(PersistentModel, PartOfMixin[Company]):
    _stored_fields: StoredFieldDefinitions = {
        '_company_address': (('..', '$.address'), FullTextSearchedStringIndex)
    }

    name: StringIndex
    birth_date: date

# resolving ForwardRef 
update_part_of_forward_refs(Company, locals())

....

pool = DbConnectionPool(config)

# create_table should be called once.
create_table(pool, Company)

# create instance
one_company = Company(
    address='California'
    members = [
        Person(name='Steve Jobs', birth_date=date(1955, 2, 24))
        Person(name='Steve Wozniak', birth_date=date(1950, 9, 11))
    ]
)

# insert or update by id in IdentifiedModel. The instance of PartOfMixin could not be saved directly.
one_company = upsert_objects(pool, one_company)

one_company.members[1].birth_date=date(1950, 8, 11)

upsert_objects(pool, one_compay)

# find entry.
companies_in_california = find_objects(pool, Company, ('address', 'match', '+California'))

persons = find_objects(pool, Person, ('_company_address', '=', 'California'))
persons = find_objects(pool, Person, ('name', 'like', '%Stev%'))

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
 * test case for external index.
	external index of sub-part

 * external in various reference.
  * find_join_key
  * main_type 
  * count_row_query 
  * match 
  * is null will be applied the base table.

 * main_type이 있는 경우 join 처리

 * order by, offset, limit test.

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


