from typing import List, ClassVar
from datetime import date

import ormdantic as od

class ContentModel(od.PersistentModel):
    name: od.StringIndex
    description : od.FullTextSearchedStr


class ContentReference(od.StringReference[ContentModel]):
    _target_field: ClassVar[str] = 'name'


class ContentCodeModel(od.PersistentModel):
    content: ContentReference
    codes: od.StringArrayIndex


pool = od.DatabaseConnectionPool({
    'user':'orm',
    'password': 'iamroot',
    'database': 'json_storage',
    'host':'localhost',
    'port':33069
})

od.create_table(pool, ContentModel, ContentCodeModel)

content = ContentModel.parse_obj({
    'name':'first content', 
    'description': 'first content which will be referenced.'
})

content_code = ContentCodeModel.parse_obj({
    'content': 'first content',
    'codes': [
        'CODE-A', 'CODE-B'
    ]
})

content = od.upsert_objects(pool, [content, content_code])

models = od.query_records(
    pool, ContentCodeModel, tuple(),
    fields=('codes', 'content.name', 'content.description', 'content')
)

list(od.query_records(
    pool, ContentModel, tuple(),
    fields=('name', 'description', 'code.codes'),
    joined={'code':ContentCodeModel}
))

