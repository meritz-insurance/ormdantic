from typing import Optional, Iterable, Type, Iterator, Dict, Any, Tuple, overload
from collections import deque
from contextlib import contextmanager

from pydantic import BaseModel

from ormdantic.schema.shareds import ( 
    collect_shared_model_type_and_ids, concat_shared_models, extract_shared_models_for, 
    has_shared_models
)
from ormdantic.util.hints import is_derived_from

from .connections import DatabaseConnectionPool

from ..schema import ModelT, PersistentModel, get_container_type
from ..schema.base import (
    PartOfMixin, SchemaBaseModel, assign_identifying_fields_if_empty, get_part_types, 
    PersistentModelT
)
from ..util import get_logger
from .queries import (
    Where, execute_and_get_last_id, 
    get_sql_for_creating_table,
    get_query_and_args_for_upserting, 
    get_query_and_args_for_reading, 
    get_sql_for_upserting_external_index_table, 
    get_sql_for_upserting_parts_table,
    get_query_and_args_for_deleting,
    _ROW_ID_FIELD, _JSON_FIELD
)


# storage will generate pydantic object from json data.

_logger = get_logger(__name__)


def build_where(items:Iterator[Tuple[str, str]] | Iterable[Tuple[str, str]]) -> Where:
    return tuple((item[0], '=', item[1]) for item in items)

       
def create_table(pool:DatabaseConnectionPool, *types:Type[PersistentModelT]):
    with pool.open_cursor(True) as cursor:
        for type_ in _iterate_types_for_creating_order(types):
            for sql in get_sql_for_creating_table(type_):
                cursor.execute(sql)

@overload
def upsert_objects(pool:DatabaseConnectionPool, models:PersistentModelT) -> PersistentModelT:
    ...

@overload
def upsert_objects(pool:DatabaseConnectionPool, models:Iterable[PersistentModelT]) -> Tuple[PersistentModelT,...]:
    ...

def upsert_objects(pool:DatabaseConnectionPool, models:PersistentModelT | Iterable[PersistentModelT]):
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
            model._before_save()

            # we will remove content of the given model. so, we copy it and remove them.
            # for not updating original model.

            for sub_model in _iterate_extracted_persistent_shared_models(model):
                inserted_id = execute_and_get_last_id(
                    cursor, *get_query_and_args_for_upserting(sub_model))

                _upsert_parts_and_externals(cursor, inserted_id, type(sub_model))

    return targets[0] if is_single else targets


def _iterate_extracted_persistent_shared_models(model:PersistentModel) -> Iterator[PersistentModel]:
    has_shared = has_shared_models(model)

    if has_shared:
        model = model.copy()

        for content_model in extract_shared_models_for(model, PersistentModel, True).values():
            yield from _iterate_extracted_persistent_shared_models(content_model)

        yield model
    else:
        yield model


def delete_objects(pool:DatabaseConnectionPool, type_:Type[PersistentModelT], where:Where):
    with pool.open_cursor(True) as cursor:
        cursor.execute(*get_query_and_args_for_deleting(type_, where))


def find_object(pool:DatabaseConnectionPool, type_:Type[PersistentModelT], where:Where, 
                *, concat_shared_models: bool = False) -> Optional[PersistentModelT]:
    objs = list(query_records(pool, type_, where, 2))

    if len(objs) == 2:
        _logger.fatal(f'More than one object found. {type_=} {where=} in {pool=}. {[obj[_ROW_ID_FIELD] for obj in objs]}')
        raise RuntimeError(f'More than one object is found of {type_} condition {where}')

    if len(objs) == 1: 
        with _context_for_shared_model(pool, concat_shared_models) as convert_model:
            return convert_model(type_, objs[0])

    return None


def find_objects(pool:DatabaseConnectionPool, type_:Type[PersistentModelT], where:Where, 
                *, fetch_size: Optional[int] = None,
                concat_shared_models: bool = False) -> Iterator[PersistentModelT]:

    with _context_for_shared_model(pool, concat_shared_models) as convert_model:
        for record in query_records(pool, type_, where, fetch_size, fields=(_JSON_FIELD, _ROW_ID_FIELD)):
            model = convert_model(type_, record)

            yield model


def _build_shared_model_set(pool:DatabaseConnectionPool, model:PersistentModel, shared_set:Dict[str, Dict[Type, SchemaBaseModel]]):
    type_and_ids = collect_shared_model_type_and_ids(model)

    for shared_type, shared_ids in type_and_ids.items():
        to_be_retreived = [id for id in shared_ids if id not in shared_set]

        if not to_be_retreived:
            continue

        if is_derived_from(shared_type, PersistentModel):
            for shared_model_record in query_records(pool,
                    shared_type, ((_ROW_ID_FIELD, 'in', to_be_retreived),),
                    fields=(_JSON_FIELD, _ROW_ID_FIELD)):

                shared_model = _convert_record_to_model(shared_type, shared_model_record)
                
                _build_shared_model_set(pool, shared_model, shared_set)


def _concat_shared_models_recursively(model:PersistentModel, shared_set:Dict[str, Dict[Type, SchemaBaseModel]]):
    concat_shared_models(model, shared_set)

    for type_and_shareds in shared_set.values():
        for another_model in type_and_shareds.values():
            if another_model and has_shared_models(another_model):
                _concat_shared_models_recursively(another_model, shared_set)


@contextmanager
def _context_for_shared_model(pool:DatabaseConnectionPool, concat_shared_models:bool):
    shared_set = {}

    def convert_model(type_:Type, record:Dict[str, Any]):
        model = _convert_record_to_model(type_, record)

        if concat_shared_models:
            _build_shared_model_set(pool, model, shared_set)
            _concat_shared_models_recursively(model, shared_set)

        return model

    yield convert_model

def _convert_record_to_model(type_:Type[PersistentModelT], record:Dict[str, Any]) -> PersistentModelT:
    model = type_.parse_raw(record[_JSON_FIELD].encode())
    model._row_id = record[_ROW_ID_FIELD]

    model._after_load()

    return model


# In where or fields, the nested expression for json path can be used.
# like 'persons.name'
# if persons indicate the simple object, we will use JSON_EXTRACT or JSON_VALUE
# if persons indicate ForeginReferenceMixin, the table which is referenced 
# from Mixin will be joined.
#
# for paging the result, we will use offset, limit and order by.
# if such feature is used, the whole used fields of table will be scaned.
# It takes a time for scanning because JSON_EXTRACT is existed in view defintion.
# So, we will build core tables for each table, which has _row_id and fields which is referenced
# in where field. the core tables will be joined. then joined table will be call as base
# table. the base table will be limited. 
#
# The base table will be join other table which can produce the fields which are targeted.
# So, JSON_EXTRACT will be call on restrcited row.
#
def query_records(pool: DatabaseConnectionPool, 
                  type_: Type[PersistentModelT], 
                  where: Where,
                  fetch_size: Optional[int] = None,
                  fields: Tuple[str, ...] = (_JSON_FIELD, _ROW_ID_FIELD),
                  order_by: Tuple[str, ...] = tuple(),
                  limit: int | None = None,
                  offset: int | None = None,
                  joined: Dict[str, Type[PersistentModelT]] | None = None
                  ) -> Iterator[Dict[str, Any]]:

    query_and_param = get_query_and_args_for_reading(
        type_, fields, where, order_by=order_by, limit=limit, offset=offset, 
        ns_types=joined)

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


def _upsert_parts_and_externals(cursor, root_inserted_id:int, type_:Type) -> None:
    args = {
        '__root_row_id': root_inserted_id,
    }

    for sql in get_sql_for_upserting_external_index_table(type_):
        cursor.execute(sql, args)

    for (part_type, sqls) in get_sql_for_upserting_parts_table(type_).items():
        for sql in sqls:
            cursor.execute(sql, args)

        _upsert_parts_and_externals(cursor, root_inserted_id, part_type)


