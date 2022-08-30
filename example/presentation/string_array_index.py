from typing import List
import ormdantic as od
import ormdantic.util 

class Product(od.PersistentModel):
    name: str
    codes: od.StringArrayIndex

_config = {
    'user':'orm',
    'password':'iamroot',
    'database':'json_storage',
    'host':'localhost',
    'port':33069
}

pool = od.DatabaseConnectionPool(_config)
od.create_table(pool, Product)

macintosh = Product.parse_obj({
    'name': 'Macintosh', 
    'codes':[
        "code1", "code2"
    ]
})

od.upsert_objects(pool, macintosh)

assert List[str] == ormdantic.util.get_base_generic_alias_of(od.StringArrayIndex, list)