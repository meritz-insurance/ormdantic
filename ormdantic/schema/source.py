from typing import (Type, Dict, Any, Tuple, Iterator, cast)

from datetime import date

from ormdantic.schema.typed import parse_object_for_model

from ..util import get_logger, is_derived_from

from .base import PersistentModelT, PersistentModel
from .shareds import PersistentSharedContentModel
from .modelcache import ModelCache

_logger = get_logger(__name__)

class PersistentModelCache(ModelCache[PersistentModel]):
    pass

class PersistentSharedModelSource:
    ''' SharedModel은 version이나 ref_date에 따라서 달라지지 않는다.  '''
    def __init__(self):
        self._cache = PersistentModelCache()

    def fetch(self, type_:Type[PersistentModelT], id:str) -> PersistentModelT | None: 
        return cast(Any, self._cache.cached_get(type_, id, self._take))

    def take(self, type_:Type[PersistentModelT], id:str) -> Dict[str, Any] | None:
        raise NotImplementedError('take should be implemented')

    def _take(self, type_:Type[PersistentModelT], id:str) -> PersistentModelT | None:
        taken = self.take(type_, id)

        if taken:
            return parse_object_for_model(taken, type_)

        return None

    def load(self, type_:Type, id:str):
        fetch = self.fetch(type_, id)

        if fetch is None:
            _logger.fatal(f'cannot load {type_=}:{id=} from {self=}')
            raise RuntimeError('no such SharedModel')

        return fetch


class PersistentModelSource:
    def __init__(self, shared_source:PersistentSharedModelSource, ref_date:date, version:int):
        self._shared_source = shared_source
        self._ref_date = ref_date
        self._version = version

        self._cache = PersistentModelCache()

    def take(self, type_:Type[PersistentModelT], id:str) -> Dict[str, Any] | None:
        raise NotImplementedError('take should be implemented')

    def _take(self, type_:Type[PersistentModelT], id:str) -> PersistentModelT | None:
        taken = self.take(type_, id)

        if taken:
            return parse_object_for_model(taken, type_)

        return None
       
    def fetch(self, type_:Type[PersistentModelT], id:str) -> PersistentModelT | None:
        if is_derived_from(type_, PersistentSharedContentModel):
            return self._shared_source.fetch(cast(Type[PersistentModelT], type_), id)

        return cast(Any, self._cache.cached_get(type_, id, self._take))
    
    def load(self, type_:Type, id:str):
        fetch = self.fetch(type_, id)

        if fetch is None:
            _logger.fatal(f'cannot load {type_=}:{id=} from {self=}')
            raise RuntimeError('no such SharedModel')

        return fetch

    def query(self, type_:Type, where:Where, 
             *,
             fetch_size:int | None,
             fields:Tuple[str, ...],
             order_by:Tuple[str, ...] | str,
             limit: int | None = None,
             offset: int | None = None,
             unwind: Tuple[str, ...] | str = tuple(),
             joined: Dict[str, Type[PersistentModelT]] | None = None) -> Iterator[Dict[str, Any]]:
        return
        yield


