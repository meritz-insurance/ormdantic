from typing import List, Tuple, Union

import pytest
from ormdantic import TypeNamedModel
from ormdantic.schema.typed import BaseClassTableModel
from ormdantic.schema.modelcache import ModelCache
from ormdantic.schema.shareds import PersistentSharedContentModel

class MyCachedBaseModel(TypeNamedModel, PersistentSharedContentModel):
    name: str


class MyCachedDerivedModel(MyCachedBaseModel, BaseClassTableModel):
    pass

class MyTableDerivedModel(MyCachedDerivedModel):
    pass


class MyCachedContainer(TypeNamedModel):
    items : List[MyCachedBaseModel]
    objs : Tuple[MyCachedBaseModel, ...]


derived_model = MyCachedDerivedModel(name='test')
table_model = MyTableDerivedModel(name='test')

@pytest.fixture(scope='function')
def model_cache():
    cache = ModelCache(threshold=1)
    derived_model.refresh_id()

    cache.register(MyCachedBaseModel, derived_model)
    cache.register(MyTableDerivedModel, table_model)

    return cache


def test_find(model_cache:ModelCache):
    assert derived_model == model_cache.find(MyCachedBaseModel, derived_model.id)
    assert table_model == model_cache.find(MyTableDerivedModel, table_model.id)

    assert None is model_cache.find(MyCachedBaseModel, 'not exist')


def test_get(model_cache:ModelCache):
    assert derived_model == model_cache.get(MyCachedBaseModel, derived_model.id, lambda x, y: derived_model)
    assert derived_model == model_cache.find(MyCachedBaseModel, derived_model.id)

    assert derived_model == model_cache.get(MyCachedBaseModel, derived_model.id, lambda x, y: None)

    model_cache.clear()

    assert None is model_cache.get(MyCachedBaseModel, derived_model.id, lambda x, y: None)


def test_delete(model_cache:ModelCache):
    model_cache.delete(MyCachedDerivedModel, derived_model.id)
    assert None is model_cache.find(MyCachedBaseModel, derived_model.id)


def test_has_entry(model_cache:ModelCache):
    assert model_cache.has_entry(MyCachedDerivedModel, derived_model.id)
    assert model_cache.has_entry(MyCachedBaseModel, derived_model.id)

