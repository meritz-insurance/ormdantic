from typing import Optional, Iterable, Type, Iterator, Dict, Any, Tuple, overload
from collections import deque

from .connectionpool import DbConnectionPool

from ..schema import PersistentModel, ModelT, get_container_type
from ..schema.base import PartOfMixin, assign_identifying_fields_if_empty, get_part_types, PersistentModelT
from ..util import get_logger
from .queries import (
    Where, execute_and_get_last_id, 
    get_sql_for_creating_table,
    get_query_and_args_for_upserting, 
    get_query_and_args_for_reading, 
    get_sql_for_inserting_parts_table,
    get_query_and_args_for_deleting,
    _ROW_ID_FIELD, _JSON_FIELD
)


# storage will generate pydantic object from json data.

_logger = get_logger(__name__)


def build_where(items:Iterator[Tuple[str, str]] | Iterable[Tuple[str, str]]) -> Where:
    return tuple((item[0], '=', item[1]) for item in items)

       
def create_table(pool:DbConnectionPool, *types:Type[ModelT]):
    with pool.open_cursor(True) as cursor:
        for type_ in _iterate_types_for_creating_order(types):
            for sql in get_sql_for_creating_table(type_):
                cursor.execute(sql)

@overload
def upsert_objects(pool:DbConnectionPool, models:PersistentModelT) -> PersistentModelT:
    ...

@overload
def upsert_objects(pool:DbConnectionPool, models:Iterable[PersistentModelT]) -> Tuple[PersistentModelT,...]:
    ...

def upsert_objects(pool:DbConnectionPool, models:PersistentModelT | Iterable[PersistentModelT]):
    is_single = isinstance(models, PersistentModel)

    model_list = [models] if is_single else models

    mixins = tuple(filter(lambda x: isinstance(x, PartOfMixin), model_list))

    if mixins:
        _logger.debug(mixins)
        _logger.fatal(f'{[type(m) for m in mixins]} can not be stored directly. it is part of mixin')
        raise RuntimeError(f'PartOfMixin could not be saved directly.')

    targets = tuple(assign_identifying_fields_if_empty(m) for m in model_list)

    with pool.open_cursor(True) as cursor:
        for model in targets:
            inserted_id = execute_and_get_last_id(
                cursor, *get_query_and_args_for_upserting(model))

            _upsert_parts(cursor, inserted_id, type(model))

    return targets[0] if is_single else targets


def delete_objects(pool:DbConnectionPool, type_:Type[ModelT], where:Where):
    with pool.open_cursor(True) as cursor:
        cursor.execute(*get_query_and_args_for_deleting(type_, where))


def find_object(pool:DbConnectionPool, type_:Type[ModelT], where:Where) -> Optional[ModelT]:
    objs = list(query_records(pool, type_, where, 2))

    if len(objs) == 2:
        _logger.fatal(f'More than one object found. {type_=} {where=} in {pool=}. {[obj[_ROW_ID_FIELD] for obj in objs]}')
        raise RuntimeError(f'More than one object is found of {type_} condition {where}')

    if len(objs) == 1: 
        return convert_dict_to_model(type_, objs[0])

    return None


def find_objects(pool:DbConnectionPool, type_:Type[ModelT], where:Where, 
                         fetch_size: Optional[int] = None) -> Iterator[ModelT]:

    for record in query_records(pool, type_, where, fetch_size):
        yield convert_dict_to_model(type_, record)


def convert_dict_to_model(type_:Type[ModelT], record:Dict[str, Any]) -> ModelT:
    return type_.parse_raw(record[_JSON_FIELD].encode())


def query_records(pool:DbConnectionPool, 
                  type_:Type[ModelT], 
                  where: Where,
                  fetch_size: Optional[int] = None,
                  fields: Tuple[str, ...] = (_JSON_FIELD, _ROW_ID_FIELD),
                  order_by: Tuple[str, ...] = tuple(),
                  limit: int | None = None,
                  offset: int | None = None
                  ) -> Iterator[Dict[str, Any]]:

    query_and_param = get_query_and_args_for_reading(type_, fields, where, order_by=order_by, limit=limit, offset=offset)

    with pool.open_cursor() as cursor:
        cursor.execute(*query_and_param)

        while results := cursor.fetchmany(fetch_size):
            yield from results
 

def _iterate_types_for_creating_order(types:Iterable[Type[ModelT]]) -> Iterator[Type[ModelT]]:
    to_be_created = deque(types)

    while to_be_created and (type_ := to_be_created.popleft()):
        container = get_container_type(type_)

        if container in to_be_created:
            to_be_created.append(type_)
        else:
            yield type_

            for part_type in _traverse_all_part_types(type_):
                yield part_type

                if part_type in to_be_created:
                    to_be_created.remove(part_type)


def _traverse_all_part_types(type_:Type[ModelT]):
    for part_type in get_part_types(type_):
        yield part_type
        yield from _traverse_all_part_types(part_type)


def _upsert_parts(cursor, root_inserted_id:int, type_:Type) -> None:
    for (part_type, sqls) in get_sql_for_inserting_parts_table(type_).items():
        args = {
            '__root_row_id': root_inserted_id,
        }

        for sql in sqls:
            cursor.execute(sql, args)

        _upsert_parts(cursor, root_inserted_id, part_type)

