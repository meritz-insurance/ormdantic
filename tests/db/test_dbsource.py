from tabnanny import verbose
from typing import  List, Annotated
import pytest
from pydantic import Field
from datetime import date
from uuid import uuid4
import pickle

from ormdantic.database.storage import (
    upsert_objects, 
)

from ormdantic.database.dbsource import (
    ModelDatabaseStorage, create_database_source
)

from ormdantic.schema import (
    PersistentModel, FullTextSearchedStringIndex, PartOfMixin, StringArrayIndex, 
    update_forward_refs, IdentifiedModel, StrId
)
from ormdantic.schema.paths import get_paths_for
from ormdantic.schema.base import ( StringIndex, VersionMixin, MetaIdentifyingField)
from ormdantic.schema.shareds import (
    ContentReferenceModel, PersistentSharedContentModel,
    extract_shared_models
)
from ormdantic.schema.source import ChainedModelSource
from ormdantic.schema.verinfo import VersionInfo

from .tools import (
    use_temp_database_pool_with_model, 
)

class SharedNameModel(PersistentSharedContentModel):
    name: FullTextSearchedStringIndex
    codes: StringArrayIndex


class ProductModel(PersistentModel, VersionMixin):
    code: Annotated[StrId, MetaIdentifyingField()]
    description: StringIndex 


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

class PartModel(IdentifiedModel, PartOfMixin['ContainerModel']):
    name: StringIndex

class ContainerModel(PersistentSharedContentModel):
    name: StringIndex
    parts: List[PartModel]

update_forward_refs(PartModel, locals())

class ContainerContentReferenceModel(ContentReferenceModel[ContainerModel]):
    pass

class ReferenceWithPartsModel(IdentifiedModel):
    ref_model : ContainerContentReferenceModel

container_content= ContainerModel(
    name=StringIndex('container'),
    parts=[
        PartModel(id=StrId(uuid4().hex), name=StringIndex('part_1')),
        PartModel(id=StrId(uuid4().hex), name=StringIndex('part_2')),
    ]
)

models = [
    SimpleContentModel(
        id=StrId('first'),
        contents=[
            NameSharedReferenceModel(content=
                SharedNameModel(name=FullTextSearchedStringIndex('name_1'), 
                                codes=['code_1', 'code_2'])
            ),
            NameSharedReferenceModel(content=
                SharedNameModel(name=FullTextSearchedStringIndex('name_2'), 
                            codes=['code_3', 'code_4'])
            )
        ]
    ),
    SimpleContentModel(
        id=StrId('second'),
        contents=[
            NameSharedReferenceModel(content=
                SharedNameModel(name=FullTextSearchedStringIndex('name_1'), 
                                codes=['code_1', 'code_2'])
            ),
            NameSharedReferenceModel(content=
                SharedNameModel(name=FullTextSearchedStringIndex('name_2'), 
                            codes=['code_3', 'code_4'])
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
                codes=['code_a', 'code_b']
            )
        )
    ),
    ReferenceWithPartsModel(
        id=StrId('part'),
        ref_model= ContainerContentReferenceModel(
            content=container_content
        )
    )
]

@pytest.fixture(scope='module')
def storage():
    with use_temp_database_pool_with_model(
        SimpleContentModel, NestedContentModel, 
        DescriptionWithExternalModel, ReferenceWithPartsModel, ProductModel) as pool:
        upsert_objects(pool, models, 0, False, VersionInfo())

        yield create_database_source(pool, 0, date.today(), 0)


@pytest.fixture(scope='module')
def chained_source():
    with use_temp_database_pool_with_model(
        SimpleContentModel, NestedContentModel, 
        DescriptionWithExternalModel, ReferenceWithPartsModel, ProductModel) as pool:
        upsert_objects(pool, models, 0, False, VersionInfo())

        storage_0 = create_database_source(pool, 0, date.today(), None)
        storage_1 = create_database_source(pool, 1, date.today(), None)

        storage_0.store(ProductModel(code=StrId('0'), description=StringIndex('first')), VersionInfo())
        storage_1.store(ProductModel(code=StrId('1'), description=StringIndex('second')), VersionInfo())

        source = ChainedModelSource(storage_1, storage_0)
        source.update_version()

        yield source


def test_load_unwind(storage:ModelDatabaseStorage):
    model = storage.load(CodedDescriptionModel, {'codes':'code_a'}, unwind='codes')

    assert model.description == 'coded reference description'


def test_find_record(storage:ModelDatabaseStorage):
    with pytest.raises(RuntimeError, match='More than.*'):
        storage.find_record(SimpleContentModel, {'id':('in', ['first', 'second'])})


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

def test_find_raises(storage:ModelDatabaseStorage):    
    with pytest.raises(RuntimeError):
        storage.find(
            CodedDescriptionModel, {'codes': ('in', ['code_a', 'code_b'])},
            unwind='codes'
        ) 


def test_load_object_with_shared_and_externals(storage:ModelDatabaseStorage):    
    assert None is storage.find(
        CodedDescriptionModel, {'codes': 'code_a'},
    ) 

    description_model = storage.load(
        CodedDescriptionModel, {'codes': 'code_a'},
        unwind='codes'
    )

    assert isinstance(description_model, CodedDescriptionModel)

    description = storage.load(DescriptionWithExternalModel, {'id':'external'},
                               populated=True)

    assert description.ref_model.get_content().description == description_model.description
    

def test_clone_with(storage:ModelDatabaseStorage):
    old_cache = storage._cache

    assert old_cache == storage.clone_with()._cache
    assert old_cache != storage.clone_with(version=2)
    assert old_cache != storage.clone_with(ref_date=date(2000,1,1))


def test_store_delete_purge(storage:ModelDatabaseStorage):
    to_be_stored = NestedContentModel(
        id=StrId('to_be_stored'),
        nested = NestedSharedReferenceModel(
            content=NestedSharedModel(
                description_model= DescriptionSharedReferenceModel(
                    content=SharedDescriptionModel(
                        description='nested shared content'
                    )
                )
            )
        )
    )

    stripped = storage.store(to_be_stored, VersionInfo())

    assert None is storage.find(NestedContentModel, {'id':'to_be_stored'})

    # after update version we can see stored item.
    stored_version = storage.update_version()
    assert to_be_stored == storage.find(NestedContentModel, {'id':'to_be_stored'}, 
                                        populated=True)
    assert stripped == storage.find(NestedContentModel, {'id':'to_be_stored'})

    # delete will mark record as deleted after current version.
    assert ['to_be_stored'] == storage.delete(NestedContentModel, {'id':'to_be_stored'}, VersionInfo())
    assert stripped == storage.find(NestedContentModel, {'id':'to_be_stored'})

    storage.update_version()

    # after update versionn, we cannot see deleted the item.
    assert None is storage.find(NestedContentModel, {'id':'to_be_stored'})

    # but if we set version as old, we can see the item.
    storage.update_version(stored_version)
    assert stripped == storage.find(NestedContentModel, {'id':'to_be_stored'})

    storage.update_version()
    assert ['to_be_stored'] == storage.purge(NestedContentModel, {'id':'to_be_stored'}, VersionInfo())

    # reset cache by setting different version.
    storage.update_version(0)
    assert None is storage.find(NestedContentModel, {'id':'to_be_stored'})


def test_squash(storage:ModelDatabaseStorage):
    product = ProductModel(code=StrId('squashed'), description=StringIndex('first'))
    storage.store(product, VersionInfo())
    first_version = storage.get_latest_version()

    product.description = StringIndex('second')
    storage.store(product, VersionInfo())
    second_version = storage.get_latest_version()

    storage.update_version(first_version)
    assert 'first' == storage.load(ProductModel, {'code':'squashed'}).description

    storage.update_version()
    assert [{'code':'squashed'}] == storage.squash(ProductModel, {'code':'squashed'}, VersionInfo())

    storage.update_version(first_version)
    assert 'second' == storage.load(ProductModel, {'code':'squashed'}).description

    product.description = StringIndex('third')
    storage.store(product, VersionInfo())
    third_version = storage.get_latest_version()

    product.description = StringIndex('forth')
    storage.store(product, VersionInfo())

    assert [{'code':'squashed'}] == storage.squash(ProductModel, {'code':'squashed'}, VersionInfo())

    storage.update_version(third_version)
    assert 'forth' == storage.load(ProductModel, {'code':'squashed'}).description

    storage.update_version(second_version)
    assert 'second' == storage.load(ProductModel, {'code':'squashed'}).description


def test_chained_find_record(chained_source:ChainedModelSource):
    assert chained_source.find_record(ProductModel, {'code':'0'})
    assert chained_source.find_record(ProductModel, {'code':'1'})


def test_chained_update_version(chained_source:ChainedModelSource):
    chained_source.update_version(0)
    assert not chained_source.find_record(ProductModel, {'code':'0'})

    chained_source.update_version()
    assert chained_source.find_record(ProductModel, {'code':'0'})


def test_chained_clone_with(chained_source:ChainedModelSource):
    source = chained_source.clone_with(version=0)
    assert not source.find_record(ProductModel, {'code':'0'})

    source = chained_source.clone_with(version=None)
    assert source.find_record(ProductModel, {'code':'0'})


def test_upsert_objects_with_shared_and_parts(storage:ModelDatabaseStorage):
    part_1_model = storage.load(PartModel, {'name':'part_1'})

    assert 'part_1' == part_1_model.name

    ref_with_parts = storage.load(
        ReferenceWithPartsModel, {'id':'part'}, populated=True)

    container_content.parts[0]

    assert ref_with_parts.ref_model.content == container_content


def test_delete_objects_with_shared(storage:ModelDatabaseStorage):
    with pytest.raises(RuntimeError, match='PersistentSharedContentModel could not be deleted. you tried to deleted ContainerModel'):
        storage.purge(ContainerModel, {'id':'f0c9920d60433b61c6aa3536e0c91697fb6d6af5'}, 
                      version_info=VersionInfo())


def test_reduce(storage:ModelDatabaseStorage):
    loaded = pickle.loads(pickle.dumps(storage))

    assert loaded._pool._connection_config == storage._pool._connection_config