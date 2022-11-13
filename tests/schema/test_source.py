from codecs import unicode_escape_decode
import pytest
import pickle

from typing import List, Type

from ormdantic.schema.base import (
    FullTextSearchedStr, PersistentModel, 
    StringArrayIndex, StringIndex, StrId
)
from ormdantic.schema.modelcache import ModelCache
from ormdantic.schema.shareds import ContentReferenceModel, PersistentSharedContentModel

from ormdantic.schema.source import (
    ChainedModelSource, ChainedSharedModelSource, SharedModelSource, ModelSource,
    MemoryModelSource, MemorySharedModelSource,
    extract_id_values
)
from ormdantic.schema.verinfo import VersionInfo

class MySharedContent(PersistentSharedContentModel): 
    name: StringIndex

class SharedContentReferenceModel(ContentReferenceModel[MySharedContent]):
    pass

class MyNestedContent(PersistentSharedContentModel):
    description: StringIndex
    item: SharedContentReferenceModel

class NestedContentReferenceModel(ContentReferenceModel[MyNestedContent]):
    pass

class MyComplexContent(PersistentModel):
    code: StringIndex
    nested: NestedContentReferenceModel
    items: List[SharedContentReferenceModel]

class MyProduct(PersistentModel):
    code: StrId
    name: FullTextSearchedStr
    tags: StringArrayIndex
    nested: NestedContentReferenceModel

first_shared = MySharedContent(name=StringIndex('first'))
first_shared.refresh_id()
second_shared = MySharedContent(name=StringIndex('second'))
second_shared.refresh_id()

found_1_shared = MySharedContent(name=StringIndex('found-1'))
found_1_shared.refresh_id()
found_2_shared = MySharedContent(name=StringIndex('found-2'))
found_2_shared.refresh_id()

nested_shared = MyNestedContent(description=StringIndex('nested'), 
                         item=SharedContentReferenceModel(content=first_shared.id))
nested_shared.refresh_id()

found_1 = MyProduct(code=StrId('found-1'), name=FullTextSearchedStr('found product'), 
                    tags=StringArrayIndex(['a', 'b']),
                    nested=NestedContentReferenceModel(content=nested_shared.id))
found_2 = MyProduct(code=StrId('found-2'), name=FullTextSearchedStr('found product'), 
                    tags=StringArrayIndex(['b', 'c']),
                    nested=NestedContentReferenceModel(content=''))

def _find_records(type:Type, *id_values:str):
    for index, item in enumerate([found_1, found_2]):
        if isinstance(item, type) and (item.code,) == id_values:
            return (item.json(), index)

    return None

@pytest.fixture(scope='function')
def shared_source(monkeypatch:pytest.MonkeyPatch):
    def _find_shared_records(type:Type, *ids:str):
        for id in ids:
            for index, item in enumerate([found_1_shared, nested_shared]):
                if isinstance(item, type) and item.id == id:
                    yield (item.json(), index)

    cache = ModelCache()

    for shared in [first_shared, second_shared]:
        cache.register(MySharedContent, shared)

    source = SharedModelSource(cache)

    monkeypatch.setattr(source, 'find_records_by_ids', _find_shared_records)

    return source

@pytest.fixture(scope='function')
def other_shared_source(monkeypatch:pytest.MonkeyPatch):
    def _find_shared_records(type:Type, id:str):
        for index, item in enumerate([found_2_shared]):
            item.refresh_id()
            if isinstance(item, type) and item.id == id:
                yield (item.json(), index)

    cache = ModelCache()

    for shared in [first_shared, second_shared]:
        cache.register(MySharedContent, shared)

    source = SharedModelSource(cache)

    monkeypatch.setattr(source, 'find_records_by_ids', _find_shared_records)
    return source


@pytest.fixture(scope='function')
def chained_shared_source(shared_source, other_shared_source):
    return ChainedSharedModelSource(shared_source, other_shared_source)

@pytest.fixture(scope='function')
def source(shared_source, monkeypatch:pytest.MonkeyPatch):
    cache = ModelCache()

    source = MemoryModelSource([found_1, found_2], shared_source=shared_source)

    monkeypatch.setattr(source, 'find_record', _find_records)
    monkeypatch.setattr(source, 'get_latest_version', lambda: 0)

    return source

@pytest.fixture(scope='function')
def chained_source():
    return ChainedModelSource(
        MemoryModelSource([found_1]),
        MemoryModelSource([found_2])
    )

def test_shared_source_find(shared_source):
    assert not shared_source._cache.has_entry(MySharedContent, found_1_shared.id)

    assert first_shared == shared_source.find(MySharedContent, first_shared.id)
    assert found_1_shared == shared_source.find(MySharedContent, found_1_shared.id)

    assert shared_source._cache.has_entry(MySharedContent, found_1_shared.id)
    
    assert None is shared_source.find(MySharedContent, 'not existed')

def test_shared_source_find_multiple(shared_source):
    assert [
        first_shared, second_shared, found_1_shared
    ] == list(
        shared_source.find_multiple(MySharedContent, 
                                    first_shared.id, second_shared.id, found_1_shared.id, 
                                    'not_existed')
    )

def test_shared_source_load(shared_source : SharedModelSource):
    assert first_shared == shared_source.load(MySharedContent, first_shared.id)

    with pytest.raises(RuntimeError, match='no such.*'):
        shared_source.load(MySharedContent, 'not existed')


def test_shared_source_find_records_by_ids(shared_source : SharedModelSource):
    assert [] == list(shared_source.find_records_by_ids(MySharedContent, 'id'))


def test_shared_source_populate(shared_source: SharedModelSource):
    complex_model = MyComplexContent(code=StringIndex('code1'), 
        nested=NestedContentReferenceModel(content=nested_shared.id), 
        items=[
            SharedContentReferenceModel(content=found_1_shared.id),
            SharedContentReferenceModel(content=second_shared.id),
        ])

    populated = shared_source.populate_shared_models(complex_model)

    assert nested_shared != populated.nested.content
    assert shared_source.populate_shared_models(nested_shared) == populated.nested.content
    assert isinstance(populated.nested.content, MyNestedContent)

    assert first_shared == populated.nested.content.item.content

    assert found_1_shared == populated.items[0].content
    assert second_shared == populated.items[1].content


def test_source_query_records(source:ModelSource):
    records = list(source.query_records(
        MyProduct, {'code': ('=', 'found-1')},
        fields=('code', '')
    ))
    assert [{'code': 'found-1'}] == records

def test_source_query_records_raises(source:ModelSource):
    with pytest.raises(RuntimeError, match='.*specific op.*'):
        list(source.query_records(
            MyProduct, {'code': ('!=', 'found-1')},
            fields=('code',)
        ))
 

def test_source_find(source:ModelSource):
    assert found_1 == source.find(MyProduct, {'code':'found-1'})

    assert found_1 != source.find(MyProduct, {'code':'found-1'}, populated=True)

    assert (
        source._shared_source.populate_shared_models(found_1) 
        == source.find(MyProduct, {'code':'found-1'}, populated=True)
    )
    
    assert None is source.find(MyProduct, {'code':'not existed'})


def test_source_load(source:ModelSource):
    assert found_1 == source.load(MyProduct, {'code':'found-1'})
    assert (
        source._shared_source.populate_shared_models(found_1) 
        == source.load(MyProduct, {'code':'found-1'}, populated=True)
    )

    with pytest.raises(RuntimeError, match='no such MyProduct'):
        source.load(MyProduct, {'code':'not existed'})


def test_source_query(source:ModelSource):
    assert [found_1, found_2] == list(source.query(MyProduct, {'name': 'found product'}))

    assert (
        [source._shared_source.populate_shared_models(found_1)]
        == list(source.query(MyProduct, {'code': 'found-1'}, populated=True))
    )

    assert [] == list(source.query(MyProduct, {'id': 'not-existed'}))


def test_chained_shared_find(chained_shared_source:ChainedSharedModelSource):
    assert found_1_shared == chained_shared_source.find(MySharedContent, found_1_shared.id) 
    assert found_2_shared == chained_shared_source.find(MySharedContent, found_2_shared.id) 
    assert None is chained_shared_source.find(MySharedContent, 'not-existed')


def test_chained_shared_find_records_by_ids(chained_shared_source:ChainedSharedModelSource):
    assert (
        [(found_1_shared.json(), 0), (found_2_shared.json(), 0)] 
        == list(chained_shared_source.find_records_by_ids(
            MySharedContent, found_1_shared.id, found_2_shared.id)
        )
    )


def test_chained_shared_find_multiple(chained_shared_source:ChainedSharedModelSource):
    assert [found_1_shared, found_2_shared] == list(chained_shared_source.find_multiple(
        MySharedContent, found_1_shared.id, found_2_shared.id))
    assert [] == list(chained_shared_source.find_multiple(
        MySharedContent, 'not-existed'))


def test_chained_shared_populate_share_model(chained_shared_source:ChainedSharedModelSource):
    complex_model = MyComplexContent(code=StringIndex('code1'), 
        nested=NestedContentReferenceModel(content=nested_shared.id), 
        items=[
            SharedContentReferenceModel(content=found_2_shared.id),
            SharedContentReferenceModel(content=second_shared.id),
        ])

    populated = chained_shared_source.populate_shared_models(complex_model)

    assert nested_shared != populated.nested.content
    assert chained_shared_source.populate_shared_models(nested_shared) == populated.nested.content
    assert isinstance(populated.nested.content, MyNestedContent)

    assert first_shared == populated.nested.content.item.content


def test_chained_find(chained_source:ModelSource):
    assert found_1 == chained_source.find(MyProduct, {'code':'found-1'})
    assert found_2 == chained_source.find(MyProduct, {'code':'found-2'})
    assert None is chained_source.find(MyProduct, {'code':'not-existed'})


def test_chained_query(chained_source:ModelSource):
    assert {found_1.code, found_2.code} == set(
        model.code for model in chained_source.query(MyProduct, {'name': 'found product'})
    )

def test_memory_shared_model_source_find_records_by_ids():
    shared_source = MemorySharedModelSource([])

    assert [] == list(shared_source.find_records_by_ids(MyProduct, '1', '2'))

def test_memory_model_source_store():
    source = MemoryModelSource([])

    assert None is source.find(MyProduct, {'code':'found-1'})

    source.store([first_shared, found_1], VersionInfo())

    assert first_shared == source.find(MySharedContent, {'id':first_shared.id})
    assert found_1 == source.find(MyProduct, {'code':'found-1'})


def test_memory_model_source_delete():
    source = MemoryModelSource([first_shared, found_1])

    assert None is not source.find(MyProduct, {'code':'found-1'})
    source.delete(MyProduct, {'code':found_1.code}, version_info=VersionInfo())
    
    assert None is source.find(MyProduct, {'code':'found-1'})

    source.delete(MySharedContent, {'id':first_shared.id}, VersionInfo())


def test_memory_model_source_purge():
    source = MemoryModelSource([first_shared, found_1])

    assert None is not source.find(MyProduct, {'code':'found-1'})
    source.purge(MyProduct, {'code':found_1.code}, version_info=VersionInfo())

    assert None is source.find(MyProduct, {'code':'found-1'})
 

def test_memory_model_source_squash():
    source = MemoryModelSource([first_shared, found_1])

    assert [] == source.squash(MyProduct, {'code':'found-1'}, VersionInfo())
    

def test_test_id_values():
    class FieldEmpty(PersistentModel):
        pass

    with pytest.raises(RuntimeError, match='no identified fields'):
        extract_id_values(FieldEmpty, {'id':'hello'})

    class ClassWithId(PersistentModel):
        id:StrId

    assert ('hello',) == extract_id_values(ClassWithId, {'id':'hello'})
    assert ('hello',) == extract_id_values(ClassWithId, {'id':('=', 'hello')})

    assert None is extract_id_values(ClassWithId, {'id':('!=', 'hello')})


def test_reduce(chained_source: ModelSource):
    source = MemoryModelSource([first_shared, found_1])

    data = pickle.loads(pickle.dumps(source))
    assert type(data) == type(source)

    assert found_1 == data.find(MyProduct, {'code':'found-1'})

    data = pickle.loads(pickle.dumps(chained_source))
    assert type(data) == type(chained_source)

    assert found_1 == data.find(MyProduct, {'code':'found-1'})

    chained_shared_source = ChainedSharedModelSource(
        MemorySharedModelSource([]), MemorySharedModelSource([first_shared]))

    data = pickle.loads(pickle.dumps(chained_shared_source))
    assert type(data) == type(chained_shared_source)

    assert first_shared == data.find(MySharedContent, first_shared.id)