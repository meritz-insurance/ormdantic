from pydoc import describe
from typing import  List, Tuple
from weakref import ref
from pyparsing import oneOf
import pytest
from pydantic import Field

from ormdantic.database.storage import (
    delete_objects, upsert_objects, load_object, 
    find_objects, build_where, find_object
)

from ormdantic.schema import (
    PersistentModel, FullTextSearchedStringIndex, PartOfMixin, StringArrayIndex, 
    update_forward_refs, IdentifiedModel, IdStr, StoredFieldDefinitions,
)
from ormdantic.schema.base import (
    StringIndex,
    get_identifer_of
)
from ormdantic.schema.shareds import ContentReferenceModel, PersistentSharedContentModel, SharedContentMixin

from .tools import (
    use_temp_database_pool_with_model, 
)

class SharedNameModel(PersistentSharedContentModel):
    name: FullTextSearchedStringIndex
    codes: StringArrayIndex


class NameSharedReferenceModel(ContentReferenceModel[SharedNameModel]):
    info: str = Field(default='')
    

class SimpleContentModel(IdentifiedModel):
    contents: List[NameSharedReferenceModel]


class SharedDescriptionModel(PersistentSharedContentModel):
    description: str


class DescriptionSharedReferenceModel(ContentReferenceModel[SharedDescriptionModel]):
    pass


class NestedSharedModel(PersistentSharedContentModel):
    description_model: DescriptionSharedReferenceModel


class NestedSharedReferenceModel(ContentReferenceModel[NestedSharedModel]):
    pass


class NestedContentModel(IdentifiedModel):
    nested: NestedSharedReferenceModel


class CodedDescriptionModel(PersistentSharedContentModel):
    description: str
    codes: StringArrayIndex


class CodedDescriptionReferenceModel(ContentReferenceModel[CodedDescriptionModel]):
    pass


class DescriptionWithExternalModel(IdentifiedModel):
    ref_model : CodedDescriptionReferenceModel

class PartModel(PersistentModel, PartOfMixin['ContainerModel']):
    name: StringIndex

class ContainerModel(PersistentSharedContentModel):
    parts: List[PartModel]

update_forward_refs(PartModel, locals())

class ReferenceWithPartsModel(IdentifiedModel):
    ref_model : ContentReferenceModel[ContainerModel]

models = [
    SimpleContentModel(
        id=IdStr('first'),
        contents=[
            NameSharedReferenceModel(content=
                SharedNameModel(name=FullTextSearchedStringIndex('name_1'), 
                                codes=StringArrayIndex(['code_1', 'code_2']))
            ),
            NameSharedReferenceModel(content=
                SharedNameModel(name=FullTextSearchedStringIndex('name_2'), 
                            codes=StringArrayIndex(['code_3', 'code_4']))
            )
        ]
    ),
    SimpleContentModel(
        id=IdStr('second'),
        contents=[
            NameSharedReferenceModel(content=
                SharedNameModel(name=FullTextSearchedStringIndex('name_1'), 
                                codes=StringArrayIndex(['code_1', 'code_2']))
            ),
            NameSharedReferenceModel(content=
                SharedNameModel(name=FullTextSearchedStringIndex('name_2'), 
                            codes=StringArrayIndex(['code_3', 'code_4']))
            )
        ]
    ),
    NestedContentModel(
        id=IdStr('nested'),
        nested = NestedSharedReferenceModel(
            content=NestedSharedModel(
                description_model= DescriptionSharedReferenceModel(
                    content=SharedDescriptionModel(
                        description='nested shared content'
                    )
                )
            )
        )
    ),
    DescriptionWithExternalModel(
        id=IdStr('external'),
        ref_model = CodedDescriptionReferenceModel(
            content = CodedDescriptionModel(
                description='coded reference description',
                codes=StringArrayIndex(['code_a', 'code_b'])
            )
        )
    ),
    ReferenceWithPartsModel(
        id=IdStr('part'),
        ref_model= ContentReferenceModel(
            content= ContainerModel(
                id=IdStr('container'),
                parts=[
                    PartModel(name=StringIndex('part_1')),
                    PartModel(name=StringIndex('part_2')),
                ]
            )
        )
    )
]

@pytest.fixture(scope='module')
def pool():
    with use_temp_database_pool_with_model(
        SimpleContentModel, NestedContentModel, 
        DescriptionWithExternalModel, ReferenceWithPartsModel) as pool:
        upsert_objects(pool, models)

        yield pool


def test_upsert_objects_with_shared(pool):
    simple_model = load_object(pool, SimpleContentModel, (('id', '=', 'first'),))

    assert isinstance(simple_model.contents[0].content, str)
    assert isinstance(simple_model.contents[1].content, str)

    simple_model = load_object(pool, SimpleContentModel, (('id', '=', 'first'),), 
                              concat_shared_models=True)

    assert not isinstance(simple_model.contents[0].content, str)
    assert not isinstance(simple_model.contents[1].content, str)


def test_upsert_objects_with_nested_shared(pool):
    nested_model = load_object(pool, NestedContentModel, (('id', '=', 'nested'),))

    assert isinstance(nested_model.nested.content, str)

    nested_model = load_object(pool, NestedContentModel, (('id', '=', 'nested'),), 
                               concat_shared_models=True)

    assert not isinstance(nested_model.nested.content, str)
    assert not isinstance(nested_model.nested.content.description_model.content, str)

    created = NestedContentModel(
        id=IdStr('created'),
        nested = NestedSharedReferenceModel(
            content=nested_model.nested.content.id
        )
    )
    
    upsert_objects(pool, created)

    created_model = load_object(pool, NestedContentModel, (('id', '=', 'created'),))

    assert created_model.nested.content == nested_model.nested.content.id


def test_upsert_objects_with_shared_and_externals(pool):    
    assert None is find_object(
        pool, CodedDescriptionModel, (('codes', '=', 'code_a'),),
    ) 

    description_model = load_object(
        pool, CodedDescriptionModel, (('codes', '=', 'code_a'),),
        unwind='codes'
    )

    assert isinstance(description_model, CodedDescriptionModel)

    description = load_object(pool, DescriptionWithExternalModel, (('id', '=', 'external'),),
        concat_shared_models=True)

    assert description.ref_model.get_content().description == description_model.description
    

def test_upsert_objects_with_shared_and_parts(pool):
    part_1_model = load_object(
        pool, PartModel, (('name', '=', 'part_1'),),
    ) 

    container = load_object(
        pool, ContainerModel, (('id', '=', 'f0c9920d60433b61c6aa3536e0c91697fb6d6af5'),)
    )

    assert container.parts[0] == part_1_model

    ref_with_parts = load_object(
        pool, ReferenceWithPartsModel, (('id', '=', 'part'),),
        concat_shared_models= True
    )

    assert ref_with_parts.ref_model.content == container


def test_delete_objects_with_shared(pool):
    with pytest.raises(RuntimeError, match='PersistentSharedContentModel could not be deleted. you tried to deleted ContainerModel'):
        delete_objects(pool, ContainerModel, (('id', '=', 'f0c9920d60433b61c6aa3536e0c91697fb6d6af5'),))


def test_find_objects_with_shared(pool):
    objects = find_objects(pool, SimpleContentModel, tuple(), concat_shared_models=True)

    first = next(objects)
    second = next(objects)

    assert first.contents[0].content is first.contents[0].content
