import ormdantic as od
from datetime import date

class Member(od.PersistentModel):
    name:od.StringIndex
    birth:date

_config = {
    'user':'orm',
    'password':'iamroot',
    'database':'json_storage',
    'host':'localhost',
    'port':33069
}

pool = od.DatabaseConnectionPool(_config)

od.create_table(pool, Member)

member = Member(name=od.StringIndex('Steve Jobs'), 
                birth=date(1955, 2, 24))
od.upsert_objects(pool, member)
