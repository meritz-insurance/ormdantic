from typing import List, Tuple, Union

import pytest
from ormdantic import SharedContentModel, TypeNamedModel
from ormdantic.schema.base import UseBaseClassTableMixin
from ormdantic.schema.modelcache import ModelCache

class MyCachedBaseModel(TypeNamedModel, SharedContentModel):
    name: str


class MyCachedDerivedModel(MyCachedBaseModel):
    pass

class MyTableDerivedModel(MyCachedBaseModel, UseBaseClassTableMixin):
    pass


class MyCachedContainer(TypeNamedModel):
    items : List[MyCachedBaseModel]
    objs : Tuple[MyCachedBaseModel, ...]


def test_fetch():
    cache = ModelCache()
    derived_model = MyCachedDerivedModel(name='test')
    table_model = MyTableDerivedModel(name='test')
    derived_model.refresh_id()

    cache.register(MyCachedBaseModel, '', derived_model)
    cache.register(MyTableDerivedModel, '', table_model)

    assert derived_model == cache.fetch(MyCachedBaseModel, '')
    assert table_model == cache.fetch(MyTableDerivedModel, '')

    assert None is cache.fetch(MyCachedBaseModel, 'not exist')


def test_cached_get():
    cache = ModelCache(threshold=1)
    derived_model = MyCachedDerivedModel(name='test')

    assert derived_model == cache.cached_get(MyCachedBaseModel, '', lambda x, y: derived_model)
    assert derived_model == cache.fetch(MyCachedBaseModel, '')

    assert derived_model == cache.cached_get(MyCachedBaseModel, '', lambda x, y: None)

    cache.clear()

    assert None is cache.cached_get(MyCachedBaseModel, '', lambda x, y: None)
