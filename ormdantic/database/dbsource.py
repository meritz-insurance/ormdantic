from typing import Type, Tuple, Iterator, Any, Dict, Iterable, List

from datetime import date
from ormdantic.schema.modelcache import ModelCache

from ormdantic.schema.verinfo import VersionInfo

from ..util import get_logger
from ..schema.base import PersistentModelT, PersistentModel
from ..schema.source import (
    ModelStorage, QueryConditionType, SharedModelSource, 
)

from .connections import DatabaseConnectionPool
from .storage import (
    delete_objects, purge_objects, get_current_version, query_records, 
    squash_objects, upsert_objects, delete_objects
)
from .queries import _ROW_ID_FIELD, _JSON_FIELD


_logger = get_logger(__name__)

class SharedModelDatabaseSource(SharedModelSource):
    def __init__(self, 
                 pool: DatabaseConnectionPool,
                 set_id:int,
                 cache: ModelCache | None = None):
        self._pool = pool
        self._set_id = set_id

        super().__init__(cache)

    def find_records_by_ids(self, type_:Type[PersistentModelT], *ids:str | int
                     ) -> Iterator[Tuple[str, int]]:
        return _find_objects(self._pool, type_, {'id': ('in', ids)}, self._set_id)

    def __reduce__(self) -> str | tuple[Any, ...]:
        return (SharedModelDatabaseSource, (self._pool, self._set_id))


class ModelDatabaseStorage(ModelStorage):
    def __init__(self, 
                 pool: DatabaseConnectionPool, shared_source: SharedModelSource,
                 set_id:int, 
                 ref_date: date, version: int | None,
                 cache: ModelCache | None = None):
        
        self._pool = pool
        self._set_id = set_id

        super().__init__(shared_source, ref_date, version, 'main', cache=cache)

    def __reduce__(self) -> str | tuple[Any, ...]:
        return (ModelDatabaseStorage, (self._pool, self._shared_source, 
                                       self._set_id, self._ref_date,
                                       self._version))

    def find_record(self, type_:Type[PersistentModelT], 
                    query_condition: QueryConditionType,
                    unwind:Tuple[str,...]|str = tuple()
                    ) -> Tuple[str, int] | None:

        return _find_object(self._pool, type_, query_condition, self._set_id,
                            unwind=unwind,
                            version=self._version, ref_date=self._ref_date)

    def query_records(self, type_:Type, where:QueryConditionType, 
             *,
             fetch_size:int | None = None,
             fields:Tuple[str, ...] = tuple(),
             order_by:Tuple[str, ...] | str = tuple(),
             limit: int | None = None,
             offset: int | None = None,
             unwind: Tuple[str, ...] | str = tuple(),
             joined: Dict[str, Type] | None = None) -> Iterator[Dict[str, Any]]:

        return query_records(self._pool, type_, where, self._set_id,
                             fetch_size=fetch_size, fields=fields, 
                             order_by=order_by, limit=limit, offset=offset,
                             unwind=unwind, joined=joined,
                             version=self._version, ref_date=self._ref_date)

    def get_latest_version(self) -> int:
        return get_current_version(self._pool)

    def store(self, models:Iterable[PersistentModel] | PersistentModel, 
              version_info: VersionInfo) -> Tuple[PersistentModel] | PersistentModel:

        upserted = upsert_objects(self._pool, models, self._set_id, False, version_info)

        return upserted

    def _squash_models(self, type_: Type, identifieds: Iterator[Dict[str, Any]], 
                       version_info: VersionInfo) -> List[Dict[str, Any]]:
        return squash_objects(self._pool, type_, list(identifieds), 
                              self._set_id, version_info=version_info)

    def _delete_models(self, type_:Type, identifieds: Iterator[Dict[str, Any]], 
                       version_info: VersionInfo) -> List[Dict[str, Any]]:
        return delete_objects(self._pool, type_, list(identifieds), self._set_id, version_info=version_info) 

    def _purge_models(self, type_:Type, identifieds: Iterator[Dict[str, Any]], 
                       version_info: VersionInfo) -> List[Dict[str, Any]]:
        return purge_objects(self._pool, type_, list(identifieds), self._set_id, version_info=version_info) 


def create_database_source(pool:DatabaseConnectionPool, set_id:int, ref_date:date, version:int | None = None):
    return ModelDatabaseStorage(pool, SharedModelDatabaseSource(pool, set_id), 
                                set_id, ref_date, version)


def _find_object(pool: DatabaseConnectionPool, 
                  type_: Type[PersistentModelT], where: QueryConditionType,
                  set_id: int,
                  *,
                  unwind: Tuple[str, ...]| str = tuple(),
                  ref_date: date | None = None,
                  version: int = 0) -> Tuple[str, int] | None:
    iterators = _find_objects(pool, type_, where, set_id,
                              ref_date=ref_date, version=version, 
                              unwind=unwind)
    
    first = next(iterators, None)

    if first is None:
        return None

    second = next(iterators, None)

    if second is not None:
        _logger.fatal(f'More than one object found. {type_=} {where=} in {pool=}. {(first[1], second[1])=}')
        raise RuntimeError(f'More than one object is found of {type_} condition {where}')

    return first
 

def _find_objects(pool: DatabaseConnectionPool, 
                  type_: Type[PersistentModelT], where: QueryConditionType,
                  set_id:int,
                  *,
                  fetch_size: int | None = None,
                  ref_date: date | None = None,
                  version: int = 0,
                  unwind: Tuple[str,...]|str = tuple()
                  ) -> Iterator[Tuple[str, int]]:
    for record in query_records(pool, type_, where, set_id, fetch_size,
                                fields=(_JSON_FIELD, _ROW_ID_FIELD),
                                version=version, ref_date=ref_date,
                                unwind=unwind):

        yield record[_JSON_FIELD], record[_ROW_ID_FIELD]

