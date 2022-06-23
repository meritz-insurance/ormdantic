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
    _materialized_fields: MaterializedFieldDefinitions = {
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
companies_in_california = find_objects(pool, Company, ('address', 'match', 'California'))

persons = find_objects(pool, Person, ('_company_address', '=', 'California'))
persons = find_objects(pool, Person, ('name', 'like', '%Stev%'))

```
 
## Note
 * implemented for mariadb only.
 * support python 3.10+ only.

## TODO
 * unwind on array object. (like mongodb's $unwind)
   - query_objects(... unwind=('codes',))

 * link two different model objects

 * where support partof materialized fields
   - wheres = ('persons.name', '=', 'steve')

 * fast query using offset, limit
   - query_objects(..., offset=100, limit=100)

 * cache
   - pydantic object validation is a little bit slow.

 * derived class, cache, audit, bitemporal

 * separated 'part of' object
   - shared by content.
