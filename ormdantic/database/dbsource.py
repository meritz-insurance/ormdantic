from typing import Type, Tuple, Iterator, Any, Dict, Iterable, List

from datetime import date

from ormdantic.schema.verinfo import VersionInfo
from ormdantic.util.tools import convert_tuple

from ..util import get_logger
from ..schema.base import PersistentModelT, get_identifying_fields, PersistentModel
from ..schema.source import (
    ModelStorage, SharedModelSource, Where, 
)

from .connections import DatabaseConnectionPool
from .storage import (
    purge_objects, get_current_version, query_records, squash_objects, upsert_objects
)
from .queries import _ROW_ID_FIELD, _JSON_FIELD


_logger = get_logger(__name__)

class DatabasePersistentSharedModelSource(SharedModelSource):
    def __init__(self, 
                 pool: DatabaseConnectionPool,
                 set_id:int):
        self._set_id = set_id
        self._pool = pool

    def find_records(self, type_:Type[PersistentModelT], *ids:str | int
                     ) -> Iterator[Tuple[str, int]]:
        return _find_objects(self._pool, type_, (('id', 'in', ids),), self._set_id)


class DatabasePersistentModelStorage(ModelStorage):
    def __init__(self, 
                 pool: DatabaseConnectionPool, shared_source: SharedModelSource,
                 set_id:int, 
                 ref_date: date, version: int | None):
        
        super().__init__(shared_source, ref_date, version)
        self._set_id = set_id
        self._pool = pool

    def find_record(self, type_:Type[PersistentModelT], *id_values:Any
                    ) -> Tuple[str, int] | None:
        where = _build_where_from_id_values(type_, id_values)

        return _find_object(self._pool, type_, where, self._set_id,
                              version=self._version, ref_date=self._ref_date)

    def query_records(self, type_:Type, where:Where, 
             *,
             fetch_size:int | None,
             fields:Tuple[str, ...],
             order_by:Tuple[str, ...] | str,
             limit: int | None = None,
             offset: int | None = None,
             unwind: Tuple[str, ...] | str = tuple(),
             joined: Dict[str, Type] | None = None) -> Iterator[Dict[str, Any]]:

        return query_records(self._pool, type_, where, self._set_id,
                             fetch_size=fetch_size, fields=fields, 
                             order_by=order_by, limit=limit, offset=offset,
                             unwind=unwind, joined=joined,
                             version=self._version, ref_date=self._ref_date)

    def get_current_version(self) -> int:
        return get_current_version(self._pool)

    def store(self, models:Iterable[PersistentModel] | PersistentModel, 
              version_info: VersionInfo) -> Tuple[PersistentModel] | PersistentModel:
        return upsert_objects(self._pool, models, self._set_id, version_info)

    def squash(self, type_: Type, *id_values_set: Any, version_info: VersionInfo) -> List[Dict[str, Any]]:
        wheres = [_build_where_from_id_values(type_, id_values) for id_values in id_values_set]
        
        return squash_objects(self._pool, type_, wheres, self._set_id, version_info=version_info)

    def delete(self, type_:Type, *id_values_set: Any, version_info:VersionInfo) -> List[Dict[str, Any]]:
        wheres = [_build_where_from_id_values(type_, id_values) for id_values in id_values_set]

        return purge_objects(self._pool, type_, wheres, self._set_id, version_info=version_info) 


def _build_where_from_id_values(type_:Type, id_values:Any):
    id_values = convert_tuple(id_values)

    fields = get_identifying_fields(type_)

    if len(fields) != len(id_values):
        _logger.fatal(f'cannot build where clause from id_values. mismatched size. '
            f'{fields=}, {id_values=}')
        raise RuntimeError('mismatched size of id values')

    return tuple((field, '=', value) for field, value in zip(fields, id_values))


def _find_object(pool: DatabaseConnectionPool, 
                  type_: Type[PersistentModelT], where: Where,
                  set_id: int,
                  *,
                  ref_date: date | None = None,
                  version: int = 0) -> Tuple[str, int] | None:
    iterators = _find_objects(pool, type_, where, set_id,
                              ref_date=ref_date, version=version)
    
    first = next(iterators, None)

    if first is None:
        return None

    second = next(iterators, None)

    if second is not None:
        _logger.fatal(f'More than one object found. {type_=} {where=} in {pool=}. {(first[1], second[1])=}')
        raise RuntimeError(f'More than one object is found of {type_} condition {where}')

    return first
 

def _find_objects(pool: DatabaseConnectionPool, 
                  type_: Type[PersistentModelT], where: Where,
                  set_id:int,
                  *,
                  fetch_size: int | None = None,
                  ref_date: date | None = None,
                  version: int = 0
                  ) -> Iterator[Tuple[str, int]]:
    for record in query_records(pool, type_, where, set_id, fetch_size,
                                fields=(_JSON_FIELD, _ROW_ID_FIELD),
                                version=version, ref_date=ref_date):

        yield record[_JSON_FIELD], record[_ROW_ID_FIELD]

