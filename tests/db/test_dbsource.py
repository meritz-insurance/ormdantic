from typing import  List
import pytest
from pydantic import Field
from datetime import date

from ormdantic.database.storage import (
    upsert_objects, 
)

from ormdantic.database.dbsource import ModelDatabaseStorage, create_database_source

from ormdantic.schema import (
    PersistentModel, FullTextSearchedStringIndex, PartOfMixin, StringArrayIndex, 
    update_forward_refs, IdentifiedModel, StrId
)
from ormdantic.schema.base import ( StringIndex,)
from ormdantic.schema.shareds import ContentReferenceModel, PersistentSharedContentModel
from ormdantic.schema.verinfo import VersionInfo

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

class ContainerContentReferenceModel(ContentReferenceModel[ContainerModel]):
    pass

class ReferenceWithPartsModel(IdentifiedModel):
    ref_model : ContainerContentReferenceModel

models = [
    SimpleContentModel(
        id=StrId('first'),
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
        id=StrId('second'),
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
        id=StrId('nested'),
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
        id=StrId('external'),
        ref_model = CodedDescriptionReferenceModel(
            content = CodedDescriptionModel(
                description='coded reference description',
                codes=StringArrayIndex(['code_a', 'code_b'])
            )
        )
    ),
    ReferenceWithPartsModel(
        id=StrId('part'),
        ref_model= ContainerContentReferenceModel(
            content= ContainerModel(
                id=StrId('container'),
                parts=[
                    PartModel(name=StringIndex('part_1')),
                    PartModel(name=StringIndex('part_2')),
                ]
            )
        )
    )
]

@pytest.fixture(scope='module')
def storage():
    with use_temp_database_pool_with_model(
        SimpleContentModel, NestedContentModel, 
        DescriptionWithExternalModel, ReferenceWithPartsModel) as pool:
        upsert_objects(pool, models, 0)

        yield create_database_source(pool, 0, date.today(), 0)


def test_load_with_shared(storage:ModelDatabaseStorage):
    simple_model = storage.load(SimpleContentModel, {'id':'first'})

    assert isinstance(simple_model.contents[0].content, str)
    assert isinstance(simple_model.contents[1].content, str)

    simple_model = storage.load(SimpleContentModel, {'id':'first'}, 
                                populated=True)

    assert not isinstance(simple_model.contents[0].content, str)
    assert not isinstance(simple_model.contents[1].content, str)


def test_load_with_nested_shared(storage:ModelDatabaseStorage):
    nested_model = storage.load(NestedContentModel, {'id':'nested'})

    assert isinstance(nested_model.nested.content, str)

    nested_model = storage.load(NestedContentModel, {'id':'nested'},
                               populated=True)

    assert not isinstance(nested_model.nested.content, str)
    assert not isinstance(nested_model.nested.content.description_model.content, str)

    created = NestedContentModel(
        id=StrId('created'),
        nested = NestedSharedReferenceModel(
            content=nested_model.nested.content.id
        )
    )
    
    storage.store(created, VersionInfo())

    assert None is storage.find( NestedContentModel, {'id':'created'})

    storage.update_version()

    created_model = storage.load( NestedContentModel, {'id':'created'})

    assert created_model.nested.content == nested_model.nested.content.id


def test_load_object_with_shared_and_externals(storage:ModelDatabaseStorage):    
    assert None is storage.find(
        CodedDescriptionModel, {'codes':'code_a'},
    ) 

    description_model = storage.load(
        CodedDescriptionModel, {'codes': 'code_a'},
        unwind='codes'
    )

    assert isinstance(description_model, CodedDescriptionModel)

    description = storage.load(DescriptionWithExternalModel, {'id':'external'},
                               populated=True)

    assert description.ref_model.get_content().description == description_model.description
    

# def test_upsert_objects_with_shared_and_parts(storage:ModelDatabaseStorage):
#     part_1_model = storage.load(PartModel, 'part_1')

#     container = storage.load(
#         ContainerModel, 'f0c9920d60433b61c6aa3536e0c91697fb6d6af5')

#     assert container.parts[0] == part_1_model

#     ref_with_parts = storage.load(
#         ReferenceWithPartsModel, 'part', populated=True)

#     assert ref_with_parts.ref_model.content == container


# def test_delete_objects_with_shared(storage:ModelDatabaseStorage):
#     with pytest.raises(RuntimeError, match='PersistentSharedContentModel could not be deleted. you tried to deleted ContainerModel'):
#         storage.purge(ContainerModel, ('id', '=', 'f0c9920d60433b61c6aa3536e0c91697fb6d6af5'), 
#                       version_info=VersionInfo())


# def test_find_objects_with_shared(storage:ModelDatabaseStorage):
#     objects = storage.query(SimpleContentModel, tuple(), populated=True)

#     first = next(objects)
#     second = next(objects)

#     assert first.contents[0].content is first.contents[0].content
