from typing import (
    Optional, Iterable, Type, Iterator, Dict, Any, Tuple, 
    overload, cast, TypeVar, List, Callable, Literal
)
from collections import deque
import itertools
from datetime import date, datetime

from pymysql.cursors import DictCursor

from ormdantic.util.tools import convert_list

from .connections import DatabaseConnectionPool

from ..util import get_logger, is_derived_from, L
from ..schema import ModelT, PersistentModel, get_container_type
from ..schema.base import (
    PartOfMixin, PersistentModel, allocate_fields_if_empty, 
    get_part_types, PersistentModelT,
    get_identifying_fields, get_identifying_field_values
)
from ..schema.verinfo import VersionInfo
from ..schema.source import QueryConditionType, to_normalize_query_condition
from ..schema.shareds import ( 
    PersistentSharedContentModel,
    extract_shared_models, 
    get_shared_content_types,
    has_shared_models, iterate_isolated_models
)

from .queries import (
    get_query_and_args_for_auditing,
    get_query_and_args_for_deleting,
    get_query_and_args_for_getting_version,
    get_query_and_args_for_getting_version_info,
    get_query_and_args_for_getting_model_changes_of_version,
    get_query_and_args_for_inserting_audit,
    get_query_and_args_for_squashing,
    get_query_for_next_seq,
    get_sql_for_creating_version_info_table, 
    get_sql_for_creating_table,
    get_query_and_args_for_upserting, 
    get_query_and_args_for_reading, 
    get_sql_for_purging_external_index, get_sql_for_purging_parts, 
    get_sql_for_upserting_external_index, 
    get_sql_for_upserting_parts,
    get_query_and_args_for_purging,
    get_query_for_adjust_seq,
    get_query_for_copying_single_object,
    _ROW_ID_FIELD, _JSON_FIELD, _VALID_START_FIELD, _VALID_END_FIELD, _BIG_INT_MAX,
    _AUDIT_VERSION_FIELD, _AUDIT_MODEL_ID_FIELD, _AUDIT_DATA_VERSION_FIELD,
    _AUDIT_TABLE_NAME_FIELD,
)


_logger = get_logger(__name__)

_T = TypeVar('_T')


def build_where(items:Iterator[Tuple[str, str]] | Iterable[Tuple[str, str]]
                ) -> QueryConditionType:
    return {item[0]:('=', item[1]) for item in items}

       
def create_table(pool:DatabaseConnectionPool, *types:Type[PersistentModelT]):
    with pool.open_cursor(True) as cursor:
        for sql in get_sql_for_creating_version_info_table():
            cursor.execute(sql)

        for type_ in _iterate_types_for_creating_order(types):
            for sql in get_sql_for_creating_table(type_):
                cursor.execute(sql)

SavedCallback = Callable[[Tuple[Any,...], PersistentModel | BaseException], None]

@overload
def upsert_objects(pool: DatabaseConnectionPool, 
                   models: PersistentModelT,
                   set_id: int,
                   ignore_error:Literal[False],
                   version_info:VersionInfo,
                   saved_callback:SavedCallback | None = None
                   ) -> PersistentModelT:
    ...

@overload
def upsert_objects(pool:DatabaseConnectionPool, 
                   models: Iterable[PersistentModelT],
                   set_id: int,
                   ignore_error:Literal[False],
                   version_info:VersionInfo,
                   saved_callback:SavedCallback | None = None
                   ) -> Tuple[PersistentModelT, ...]:
    ...

@overload
def upsert_objects(pool:DatabaseConnectionPool, 
                   models: PersistentModelT | Iterable[PersistentModelT],
                   set_id: int,
                   ignore_error:Literal[False],
                   version_info:VersionInfo,
                   saved_callback:SavedCallback | None = None
                   ) -> PersistentModelT | Tuple[PersistentModelT, ...]:
    ...


@overload
def upsert_objects(pool: DatabaseConnectionPool, 
                   models: PersistentModelT | Iterable[PersistentModelT],
                   set_id: int,
                   ignore_error:Literal[True],
                   version_info:VersionInfo,
                   saved_callback:SavedCallback | None = None
                   ) -> Dict[Tuple[Any,...], PersistentModelT | BaseException]:
    ...


@overload
def upsert_objects(pool: DatabaseConnectionPool, 
                   models: PersistentModelT | Iterable[PersistentModelT],
                   set_id: int,
                   ignore_error:bool,
                   version_info:VersionInfo,
                   saved_callback:SavedCallback | None = None
                   ) -> Dict[Tuple[Any], PersistentModelT | BaseException] | Tuple[PersistentModelT, ...] | PersistentModelT:
    ...


def upsert_objects(pool:DatabaseConnectionPool, 
                   models: PersistentModelT | Iterable[PersistentModelT],
                   set_id: int,
                   ignore_error:bool,
                   version_info:VersionInfo,
                   saved_callback:SavedCallback | None = None
                   ) -> Dict[Tuple[Any], PersistentModelT | BaseException] | Tuple[PersistentModelT, ...] | PersistentModelT:
    is_single = isinstance(models, PersistentModel)

    model_list = [models] if is_single else models

    results : Dict[Tuple[Any,...], PersistentModelT | BaseException] = {}

    with pool.open_cursor(True) as cursor:
        # we get next seq in current db transaction.
        targets = tuple(
            allocate_fields_if_empty(
                m, next_seq=lambda f: _next_seq_for(cursor, type(m), f))
            for m in model_list
        )

        targets = tuple(
            m.copy(deep=True) if has_shared_models(m) else m
            for m in targets
        )

        allocate_audit_version(cursor, version_info)

        for model in targets:
            if isinstance(model, PartOfMixin):
                _logger.fatal(f'{type(model)} can not be stored directly. it is part of mixin')
                raise RuntimeError(L('PartOfMixin could not be saved directly. check {0}', type(model).__name__))

            # we will remove content of the given model. so, we copy it and remove them.
            # for not updating original model.
            id_values = tuple(get_identifying_field_values(model).values())
            results[id_values] = model

            try:
                for sub_model in iterate_isolated_models(model):
                    allocate_fields_if_empty(
                        sub_model, 
                        next_seq=lambda f: _next_seq_for(cursor, type(sub_model), f))

                    sub_model._before_save()

                    cursor.execute(*get_query_and_args_for_upserting(sub_model, set_id))

                    for row in fetch_multiple_set(cursor):
                        _update_audit_model(cursor, row)
                        _upsert_parts_and_externals(cursor, row[_ROW_ID_FIELD], type(sub_model))

                extract_shared_models(model, True)

                if saved_callback:
                    saved_callback(id_values, model)
            except BaseException as e:
                if not ignore_error:
                    raise
                else:
                    _logger.warning('{e} is raised. but ignored by user.')

                    results[id_values] = e

                    if saved_callback:
                        saved_callback(id_values, e)

    if ignore_error:
        return results
    else:
        if isinstance(models, PersistentModel):
            return targets[0]

        return targets


def fetch_multiple_set(cursor:DictCursor) -> Iterator[Dict[str, Any]]:
    loop = True
    while loop:
        fetched = cursor.fetchall()

        yield from fetched

        loop = cursor.nextset()


def allocate_audit_version(cursor:DictCursor, audit:VersionInfo, revert=False) -> int:
    cursor.execute(*get_query_and_args_for_inserting_audit(revert, audit))
    fetched = cursor.fetchall()

    audit_version = fetched[0][_AUDIT_VERSION_FIELD]
    return audit_version


def _update_audit_model(cursor:DictCursor, item:Dict[str, Any]):
    cursor.execute(*get_query_and_args_for_auditing(item))


def _next_seq_for(cursor, t:Type, field:str) -> str | int:
    sql = get_query_for_next_seq(t, field)

    cursor.execute(sql)
    record = cursor.fetchone()

    assert record

    return record['NEXT_SEQ']


def squash_objects(pool: DatabaseConnectionPool, 
                   type_: Type[PersistentModelT], wheres:List[QueryConditionType] | QueryConditionType,
                   set_id: int,
                   version_info:VersionInfo = VersionInfo()) -> List[Dict[str, Any]]:
    version = get_current_version(pool)
    fields = get_identifying_fields(type_)

    wheres = convert_list(wheres)
    squasheds = []

    with pool.open_cursor() as cursor:
        allocate_audit_version(cursor, version_info)

        for where in wheres:
            query_and_param = get_query_and_args_for_reading(
                type_, fields, to_normalize_query_condition(where),
                version=version, set_id=set_id)

            cursor.execute(*query_and_param)

            while results := cursor.fetchmany():
                for result in results:
                    squasheds.append(result)
                    cursor.execute(*get_query_and_args_for_squashing(type_, result, set_id=set_id))
                                                                    
                    row_ids = []

                    for row in fetch_multiple_set(cursor):
                        _update_audit_model(cursor, row)
                        row_ids.append(row[_ROW_ID_FIELD])

                    if row_ids:
                        _delete_parts_and_externals(cursor, tuple(row_ids), type_)

    return squasheds


def delete_objects(pool: DatabaseConnectionPool,
                   type_: Type[PersistentModelT], wheres: List[QueryConditionType] | QueryConditionType,
                   set_id:int,
                   version_info: VersionInfo = VersionInfo()) -> List[Dict[str, Any]]:
    if not is_derived_from(type_, PersistentModel):
        _logger.fatal(
            f"try to delete {type_=}. it is impossible. type should be dervied "
            f"from PersistentModel. {type_.mro()=}"
        )
        raise RuntimeError(L('could not be deleted. it should be derived from PersistentModel. check {0}', type_.__name__))

    if issubclass(type_, PersistentSharedContentModel):
        _logger.fatal(
            f"try to delete PersistentSharedContentModel {type_=}. it is impossible. "
            "we don't know whether share context mixin can be referenced by "
            "other entity or not"
        )
        raise RuntimeError(L('PersistentSharedContentModel could not be deleted. you tried to deleted {0}', type_.__name__))

    deleted = []

    wheres = convert_list(wheres)

    with pool.open_cursor(True) as cursor:
        allocate_audit_version(cursor, version_info)

        for where in wheres:
            deleted.extend(_delete_objects(cursor, type_, where, set_id))

    return deleted

def _delete_objects(cursor: DictCursor, type_:Type, where:QueryConditionType, set_id:int):
    deleted = []

    cursor.execute(
        *get_query_and_args_for_deleting(
            type_, to_normalize_query_condition(where), set_id=set_id))

    for row in fetch_multiple_set(cursor):
        _update_audit_model(cursor, row)
        deleted.append(row[_AUDIT_MODEL_ID_FIELD])

    return deleted


def purge_objects(pool: DatabaseConnectionPool,
                   type_: Type[PersistentModelT], wheres: List[QueryConditionType] | QueryConditionType,
                   set_id:int,
                   version_info: VersionInfo = VersionInfo(), forced:bool = False) -> List[Dict[str, Any]]:
    if not is_derived_from(type_, PersistentModel):
        _logger.fatal(
            f"try to delete {type_=}. it is impossible. type should be dervied "
            f"from PersistentModel. {type_.mro()=}"
        )
        raise RuntimeError(L('{0} could not be deleted. it should be derived from PersistentModel', type_.__name__))

    if issubclass(type_, PersistentSharedContentModel):
        _logger.fatal(
            f"try to delete PersistentSharedContentModel {type_=}. it is impossible. "
            "we don't know whether share context mixin can be referenced by "
            "other entity or not"
        )
        raise RuntimeError(L('PersistentSharedContentModel could not be deleted. you tried to deleted {0}', type_.__name__))

    purged = []

    wheres = convert_list(wheres)

    with pool.open_cursor(True) as cursor:
        allocate_audit_version(cursor, version_info)

        for where in wheres:
            purged.extend(_check_and_purge_objects(cursor, type_, where, set_id, forced))

    return purged


def _check_and_purge_objects(cursor: DictCursor, type_:Type, where:QueryConditionType, set_id:int, forced: bool):
    purged = []

    cursor.execute(*get_query_and_args_for_purging(type_, 
        to_normalize_query_condition(where), set_id=set_id))

    row_ids = []

    for row in fetch_multiple_set(cursor):
        if row[_VALID_END_FIELD] == _BIG_INT_MAX and not forced:
            _logger.fatal(f'{row[_AUDIT_MODEL_ID_FIELD]=} of {type_=} is not deleted.')
            raise RuntimeError(L('The object was not deleted. for purging record, object should be deleted before or use forced'))
        _update_audit_model(cursor, row)
        purged.append(row[_AUDIT_MODEL_ID_FIELD])
        row_ids.append(row[_ROW_ID_FIELD])

    if row_ids:
        _delete_parts_and_externals(cursor, tuple(row_ids), type_)

    return purged


def get_current_version(pool:DatabaseConnectionPool, include_revert:bool = True)->int:
    return get_version_info(pool, include_revert=include_revert).version or 0


def get_version_info(pool:DatabaseConnectionPool, 
                     version_or_datetime: datetime | None | int = None,
                     include_revert:bool = True) -> VersionInfo:
    with pool.open_cursor(True) as cursor:
        if isinstance(version_or_datetime, datetime) or version_or_datetime is None:
            cursor.execute(*get_query_and_args_for_getting_version(version_or_datetime, include_revert))
            record = cursor.fetchone()

            assert record

            version_or_datetime = cast(int, record[_AUDIT_VERSION_FIELD])

        cursor.execute(*get_query_and_args_for_getting_version_info(version_or_datetime))
        record = cursor.fetchone()

        assert record

        return VersionInfo.from_dict(record)


def get_model_changes_of_version(pool:DatabaseConnectionPool, 
                                 version: int) -> Iterator[Dict[str, Any]]:
    with pool.open_cursor(True) as cursor:
        cursor.execute(*get_query_and_args_for_getting_model_changes_of_version(version))

        yield from iter(cursor.fetchall())


def load_object(pool:DatabaseConnectionPool, type_:Type[PersistentModelT], where:QueryConditionType, 
                set_id:int,
                *, 
                unwind:Tuple[str,...] | str = tuple(),
                version:int = 0,
                ref_date:date | None = None) -> PersistentModelT:
    found = find_object(pool, type_, where, 
                        unwind=unwind, version=version, 
                        ref_date=ref_date, set_id=set_id)

    if not found:
        _logger.fatal(f'cannot found {type_=} object for {where=} in {pool=}')
        raise RuntimeError(L('no such item from database.'))

    return found
        

def find_object(pool:DatabaseConnectionPool, type_:Type[PersistentModelT], where:QueryConditionType, 
                set_id:int,
                *, 
                unwind:Tuple[str,...] | str = tuple(),
                version:int | None = None,
                ref_date:date | None = None) ->Optional[PersistentModelT]:
    if version is None:
        version = get_current_version(pool)

    objs = list(query_records(pool, type_, where, set_id, 2, 
                              fields=(_JSON_FIELD, _ROW_ID_FIELD,
                                      _VALID_START_FIELD),
                              unwind=unwind, version=version, 
                              ref_date=ref_date))

    if len(objs) >= 2:
        _logger.fatal(f'More than one object found. {type_=} {where=} in {pool=}. {[obj[_ROW_ID_FIELD] for obj in objs]}')
        raise RuntimeError(L('more than one object is found of {0} condition {1}', type_.__name__, where))

    if len(objs) == 1: 
        return cast(PersistentModelT, _convert_model(type_, objs[0], set_id))

    return None


def find_objects(pool:DatabaseConnectionPool, type_:Type[PersistentModelT], where:QueryConditionType, 
                set_id:int,
                *, fetch_size: Optional[int] = None,
                unwind:Tuple[str, ...] | str = tuple(),
                version:int | None = None,
                ref_date:date | None = None,
                ) -> Iterator[PersistentModelT]:    
    if version is None:
        version = get_current_version(pool)

    for record in query_records(pool, type_, where, set_id, fetch_size, 
                                fields=(_JSON_FIELD, _ROW_ID_FIELD,
                                        _VALID_START_FIELD),
                                unwind=unwind, version=version, ref_date=ref_date, 
                                ):
        model = _convert_model(type_, record, set_id)

        yield cast(PersistentModelT, model)


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
                  where: QueryConditionType,
                  set_id: int,
                  fetch_size: Optional[int] = None,
                  fields: Tuple[str, ...] = (_JSON_FIELD, _ROW_ID_FIELD),
                  order_by: Tuple[str, ...] | str= tuple(),
                  limit: int | None = None,
                  offset: int | None = None,
                  unwind: Tuple[str,...] | str = tuple(),
                  joined: Dict[str, Type[PersistentModelT]] | None = None,
                  version: int | None = None,
                  ref_date: date | None = None,
                  ) -> Iterator[Dict[str, Any]]:

    if not fields:
        _logger.fatal('empty fields for query_records. it makes an invalid query.')
        raise RuntimeError(L('empty fields for querying'))

    if version is None:
        version = get_current_version(pool)

    query_and_param = get_query_and_args_for_reading(
        type_, fields, to_normalize_query_condition(where), 
        order_by=order_by, limit=limit, offset=offset, 
        unwind=unwind,
        ns_types=joined,
        version=version,
        current=ref_date, set_id=set_id)

    with pool.open_cursor() as cursor:
        cursor.execute(*query_and_param)

        while results := cursor.fetchmany(fetch_size):
            yield from results


def move_objects(pool:DatabaseConnectionPool, 
                 type_and_query_cond: Dict[Type, QueryConditionType],
                 src_id: int, dest_id: int, forced: bool,
                 version_info: VersionInfo):
    with pool.open_cursor() as cursor:
        new_version = allocate_audit_version(cursor, version_info)

        for tp, where in type_and_query_cond.items():
            fields = get_identifying_fields(tp)
            query_and_param = get_query_and_args_for_reading(
                tp, fields, to_normalize_query_condition(where),
                version=new_version, set_id=src_id)

            cursor.execute(*query_and_param)

            for result in fetch_multiple_set(cursor):
                sqls = get_query_for_copying_single_object(tp, result, src_id, dest_id)

                cursor.execute(*sqls)

                for row in fetch_multiple_set(cursor):
                    if row[_AUDIT_DATA_VERSION_FIELD] == new_version and not forced:
                        _logger.fatal(f'version of {row=} in destination is larger than source. '
                            f'It means that the data of destination is latest. '
                            f'If you would overwrite it, forced should be True.')
                        raise RuntimeError(L('cannot copy object because the destination '
                        'has new version of data. if you overwrite it, set forced as True.'))

                    _update_audit_model(cursor, row)
                    _upsert_parts_and_externals(cursor, row[_ROW_ID_FIELD], tp)

            cursor.execute(
                *get_query_and_args_for_deleting(
                    tp, to_normalize_query_condition(where), set_id=src_id))

            for row in fetch_multiple_set(cursor):
                row['op'] = row['op'] + ":MERGE_SET"
                _update_audit_model(cursor, row)


def revert_objects(pool:DatabaseConnectionPool, 
                   all_types:Dict[str, Type],
                   version_info:VersionInfo):

    with pool.open_cursor() as cursor:
        new_version = allocate_audit_version(cursor, version_info, True)

        target_version = get_current_version(pool, False)

        if new_version < target_version:
            raise RuntimeError(L('cannot revert object. target version is larger than new_version.'))

        cursor.execute(*get_query_and_args_for_getting_model_changes_of_version(target_version))

        for row in cursor.fetchall():
            row[_AUDIT_TABLE_NAME_FIELD]
            row[_ROW_ID_FIELD]
            row[_AUDIT_MODEL_ID_FIELD]


def delete_model_set(pool:DatabaseConnectionPool,
                     set_id: int, all_types:List[Type], forced:bool, 
                     version_info: VersionInfo):
    deleted = []

    if set_id == 0:
        _logger.fatal('The main model set could not deleted')
        raise RuntimeError(L('cannot delete main model set'))

    with pool.open_cursor() as cursor:
        allocate_audit_version(cursor, version_info)

        for type_ in all_types:
            deleted.extend(_delete_objects(cursor, type_, {}, set_id))

    return deleted


def purge_model_set(pool:DatabaseConnectionPool,
                    set_id: int, all_types:List[Type], forced:bool,
                    version_info: VersionInfo):

    purged = []

    if set_id == 0:
        _logger.fatal('The main model set.')
        raise RuntimeError(L('cannot purge main model set'))

    with pool.open_cursor() as cursor:
        allocate_audit_version(cursor, version_info)

        for type_ in all_types:
            purged.extend(_check_and_purge_objects(cursor, type_, {}, set_id, forced))

    return purged


def _traverse_all_part_types(type_:Type[ModelT]):
    for part_type in get_part_types(type_):
        yield part_type
        yield from _traverse_all_part_types(part_type)


def _traverse_all_shared_types(type_:Type[ModelT]):
    for shared_type in get_shared_content_types(type_):
        yield shared_type
        yield from _traverse_all_part_types(shared_type)
        yield from _traverse_all_shared_types(shared_type)


def _upsert_parts_and_externals(cursor, root_inserted_id:int, type_:Type) -> None:
    args = {
        '__root_row_id': root_inserted_id,
    }

    for sql in get_sql_for_upserting_external_index(type_):
        cursor.execute(sql, args)

    for (part_type, sqls) in get_sql_for_upserting_parts(type_).items():
        for sql in sqls:
            cursor.execute(sql, args)

        _upsert_parts_and_externals(cursor, root_inserted_id, part_type)


def _iterate_types_for_creating_order(types:Iterable[Type[ModelT]]) -> Iterator[Type[ModelT]]:
    to_be_created = deque(types)

    while to_be_created and (type_ := to_be_created.popleft()):
        container = get_container_type(type_)

        if container in to_be_created:
            to_be_created.append(type_)
        else:
            yield type_

            for sub_type in itertools.chain(_traverse_all_part_types(type_), 
                                            _traverse_all_shared_types(type_)):
                yield sub_type

                if sub_type in to_be_created:
                    to_be_created.remove(sub_type)


def _delete_parts_and_externals(cursor, root_deleted_ids:Tuple[int,...], type_:Type) -> None:
    args = {
        '__root_row_ids': root_deleted_ids,
    }

    for sql in get_sql_for_purging_external_index(type_):
        cursor.execute(sql, args)

    for (part_type, sqls) in get_sql_for_purging_parts(type_).items():
        for sql in sqls:
            cursor.execute(sql, args)

        _delete_parts_and_externals(cursor, root_deleted_ids, part_type)


def _convert_model(type_: Type[PersistentModelT], record: Dict[str, Any], set_id) -> PersistentModelT:
    model = type_.parse_raw(record[_JSON_FIELD].encode())
    model._set_id = set_id

    model._row_id = record[_ROW_ID_FIELD]
    model._valid_start = record[_VALID_START_FIELD]

    model._after_load()

    return model


def update_sequences(pool: DatabaseConnectionPool, 
                     types: Iterable[Type[PersistentModelT]]):

    with pool.open_cursor() as cursor:
        for type_ in types:
            for sql in get_query_for_adjust_seq(type_):
                cursor.execute(sql)
 