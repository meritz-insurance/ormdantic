from dataclasses import asdict
from typing import (
    Type, Iterator, overload, Iterable, List, Tuple, cast, Dict, 
    Any, DefaultDict, Optional, Set
)
from datetime import datetime, date
from decimal import Decimal
from collections import defaultdict
import itertools
import functools

from pydantic import ConstrainedStr, ConstrainedDecimal

from ormdantic.util import is_derived_from, convert_tuple, L
from ormdantic.util.hints import get_args_of_base_generic_alias

from ..util import get_logger, has_metadata, get_metadata_for
from ..schema.base import (
    DatedMixin, PersistentModelT, MetaReferenceField, 
    MetaFullTextSearchedField, MetaIndexField, SequenceStr, 
    StoredFieldDefinitions, PersistentModelT, PartOfMixin, 
    VersionMixin, 
    MetaUniqueIndexField, get_container_type, get_root_container_type,
    get_field_names_for, get_part_types, is_field_list_or_tuple_of,
    PersistentModel, is_list_or_tuple_of, get_stored_fields,
    get_stored_fields_for, get_identifying_field_values,
    get_identifying_fields, MetaStoredField, MetaIdentifyingField
)
from ..schema.typed import get_type_for_table
from ..schema.shareds import PersistentSharedContentModel
from ..schema.verinfo import VersionInfo
from ..schema.source import NormalizedQueryConditionType

_MAX_VAR_CHAR_LENGTH = 200
_MAX_DECIMAL_DIGITS = 65

_BIG_INT_MAX = 9223372036854775807

_JSON_TYPE = 'LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin'
_JSON_CHECK = 'CHECK (JSON_VALID({}))'

_PART_BASE_TABLE = 'pbase'

# field name for internal use.
_VALID_START_FIELD = '__valid_start'
_VALID_END_FIELD = '__valid_end'
_SQUASHED_FROM_FIELD = '__squashed_from'

# 'applied_at' is field which come from VersionMixin. so we don't use __ as prefix
_APPLIED_AT_FIELD = 'applied_at'
_APPLIED_START_FIELD = '__applied_start'
_APPLIED_END_FIELD = '__applied_end'

_AUDIT_VERSION_FIELD = 'version'
_AUDIT_DATA_VERSION_FIELD = 'data_version'

_AUDIT_WHO_FIELD = 'who'
_AUDIT_WHY_FIELD = 'why'
_AUDIT_WHERE_FIELD = 'where'
_AUDIT_WHEN_FIELD = 'when'
_AUDIT_TAG_FIELD = 'tag'

_VERSION_INFO_TABLE = '_version_info'
_VERSION_CHANGE_TABLE = '_model_changes'

_AUDIT_OP_FIELD = 'op'
_AUDIT_TABLE_NAME_FIELD = 'table_name'
# After we deleted model, we could not know which item 
# is delete because it contains __row_id only. so, we will keep id of model 
# if it is deleted.
_AUDIT_MODEL_ID_FIELD = 'model_id'

_ROW_ID_FIELD = '__row_id'
_SET_ID_FIELD = '__set_id'
_ORG_ROW_ID_FIELD = '__org_row_id'
_CONTAINER_ROW_ID_FIELD = '__container_row_id'
_ROOT_ROW_ID_FIELD = '__root_row_id'
_JSON_FIELD = '__json'
_JSON_PATH_FIELD = '__json_path'
_PART_ORDER_FIELD = '__part_order'
_RELEVANCE_FIELD = '__relevance'
_MODEL_TABLE_PREFIX = 'md'
_SEQ_PREFIX = 'sq'
_FUNC_PREFIX = 'fn'

# for cjk full text search (mroonga). It seemed to be slow that records are inserted in mroonga.
# _ENGINE = r""" ENGINE=mroonga COMMENT='engine "innodb" DEFAULT CHARSET=utf8'"""
_ENGINE = ""
_FULL_TEXT_SEARCH_OPTION = r"""COMMENT 'parser "TokenBigramIgnoreBlankSplitSymbolAlphaDigit"'"""


FieldOp = Tuple[Tuple[str, str]]

_logger = get_logger(__name__)

def get_table_name(type_:Type[PersistentModelT], postfix:str = ''):
    return f'{_MODEL_TABLE_PREFIX}_{_get_name_from_type(type_)}{"_" + postfix if postfix else ""}' 


def get_seq_name(type_:Type[PersistentModelT], postfix:str):
    return f'{_SEQ_PREFIX}_{_get_name_from_type(type_)}{"_" + postfix if postfix else ""}' 


def _get_name_from_type(type_:Type[PersistentModelT]) -> str:
    return get_type_for_table(type_).__name__


@overload
def field_exprs(fields:str, table_name:str='') -> str:
    ...

@overload
def field_exprs(fields:Iterable[str], table_name:str='') -> Iterator[str]:
    ...

def field_exprs(fields:str | Iterable[str], table_name:str='') -> str | Iterator[str]:
    if isinstance(fields, str):
        if table_name:
            return '.'.join([_f(table_name), _f(fields)])

        return _f(fields)
    else:
        return map(lambda f: field_exprs(f, table_name), fields)


def as_field_expr(field:str, table_name:str, ns:str) -> str:
    if ns:
        return f'{field_exprs(field, table_name)} AS {field_exprs(_add_namespace(field, ns))}'
    else:
        return f'{field_exprs(field, table_name)}'



def join_line(*lines:str | Iterable[str], 
              new_line: bool = True, use_comma: bool = False) -> str:
    sep = (',' if use_comma else '') + ('\n' if new_line else '')

    return (
        sep.join(
            line 
            for line in itertools.chain(*[[l] if isinstance(l, str) else l for l in lines]) 
            if line
        ) 
    )


def _alias_table_or_query(table_or_query:str, table_name:str) -> str:
    table_or_query = table_or_query.strip()

    normalized_name = _normalize_database_object_name(table_name)

    if ' ' in table_or_query:
        return join_line(
            f"(",
            tab_each_line(
                table_or_query
            ),
            f")",
            f"AS {normalized_name}"
        )

    return f"{table_or_query} AS {normalized_name}"



def tab_each_line(*statements:Iterable[str] | str, use_comma: bool = False) -> str:
    line = join_line(*statements, use_comma=use_comma, new_line=True)

    return join_line(['  ' + item for item in line.split('\n')], use_comma=False, new_line=True)


def get_sql_for_creating_version_info_table():
    yield _build_model_table_statement(
        _VERSION_INFO_TABLE,
        iter([
            f'{field_exprs(_AUDIT_VERSION_FIELD)} BIGINT AUTO_INCREMENT PRIMARY KEY',
            f'{field_exprs(_AUDIT_WHO_FIELD)} VARCHAR(80)',
            f'{field_exprs(_AUDIT_WHERE_FIELD)} VARCHAR(80)',
            f'{field_exprs(_AUDIT_WHEN_FIELD)} DATETIME(6)',
            f'{field_exprs(_AUDIT_WHY_FIELD)} VARCHAR(256)',
            f'{field_exprs(_AUDIT_TAG_FIELD)} VARCHAR(80)',
        ])
    )

    yield _build_model_table_statement(
        _VERSION_CHANGE_TABLE,
        iter([
            f'{field_exprs(_AUDIT_VERSION_FIELD)} BIGINT',
            f'{field_exprs(_AUDIT_DATA_VERSION_FIELD)} BIGINT',
            f'{field_exprs(_AUDIT_OP_FIELD)} VARCHAR(32)',
            f'{field_exprs(_AUDIT_TABLE_NAME_FIELD)} VARCHAR(80)',
            f'{field_exprs(_SET_ID_FIELD)} BIGINT',
            f'{field_exprs(_ROW_ID_FIELD)} BIGINT',
            f'{field_exprs(_AUDIT_MODEL_ID_FIELD)} VARCHAR(256)',
            f"KEY `__row_id_index` ({field_exprs(_ROW_ID_FIELD)})",
            f"KEY `__version_index` ({field_exprs(_AUDIT_VERSION_FIELD)})"
        ])
    )

@functools.cache
def _get_sql_for_auditing_model():
    return join_line(
        f'INSERT INTO {field_exprs(_VERSION_CHANGE_TABLE)}',
        '(',
        tab_each_line(
            field_exprs([
                _AUDIT_VERSION_FIELD, 
                _AUDIT_DATA_VERSION_FIELD, 
                _AUDIT_OP_FIELD, 
                _AUDIT_TABLE_NAME_FIELD, 
                _SET_ID_FIELD,
                _ROW_ID_FIELD,
                _AUDIT_MODEL_ID_FIELD
            ]),
            use_comma=True
        ),
        ')',
        'VALUES'
        '(',
        tab_each_line(
            '@VERSION',
            f'%({_AUDIT_DATA_VERSION_FIELD})s',
            f'%({_AUDIT_OP_FIELD})s',
            f'%({_AUDIT_TABLE_NAME_FIELD})s',
            f'%({_SET_ID_FIELD})s',
            f'%({_ROW_ID_FIELD})s',
            f'%({_AUDIT_MODEL_ID_FIELD})s',
            use_comma=True
        ),
        ')'
    )

 
@functools.cache
def _get_sql_for_allocating_version():
    return join_line(
        f'INSERT INTO {field_exprs(_VERSION_INFO_TABLE)}',
        '(',
        tab_each_line(
            field_exprs([
                _AUDIT_WHO_FIELD, 
                _AUDIT_WHERE_FIELD, 
                _AUDIT_WHEN_FIELD, 
                _AUDIT_WHY_FIELD, 
                _AUDIT_TAG_FIELD
            ]),
            use_comma=True
        ),
        ')',
        'VALUES'
        '(',
        tab_each_line(
            f'%({_AUDIT_WHO_FIELD})s',
            f'%({_AUDIT_WHERE_FIELD})s',
            'CURRENT_TIMESTAMP(6)',
            f'%({_AUDIT_WHY_FIELD})s',
            f'%({_AUDIT_TAG_FIELD})s',
            use_comma=True
        ),
        ')',
        'RETURNING',
        f'  @VERSION := {field_exprs(_AUDIT_VERSION_FIELD)} as {field_exprs(_AUDIT_VERSION_FIELD)}'
    )


def get_sql_for_creating_table(type_:Type[PersistentModelT]):
    stored = get_stored_fields(type_)

    if get_type_for_table(type_) != type_:
        # skip create table for BaseClassTableMixin.
        return

    if issubclass(type_, PartOfMixin):
        # The container field will be saved in container's table.
        # but for the full text search fields, we need to save them in part's table 
        # even it is in part's stored fields.
        part_stored = get_stored_fields_for_part_of(type_)

        yield _build_model_table_statement(
            get_table_name(type_, _PART_BASE_TABLE), 
            _get_part_table_fields(),
            _get_table_stored_fields(part_stored, False),
            _get_part_table_indexes(),
            _get_table_indexes(type_, part_stored, True),
        )

        part_table_name = get_table_name(type_, _PART_BASE_TABLE)

        container_type = get_container_type(type_)

        assert container_type

        if _is_version_type(type_):
            _logger.fatal(
                f'{type_=} is versioned type. but it was PartOfMxin. '
                'if the root container type is VersionMixin, the PartOfMixin may be handled as VersionMixin ')
            raise RuntimeError(L('VersionMixin is not support for PartOfMixin. check {0}', type_))

        additional_fields = (_SET_ID_FIELD, _VALID_START_FIELD, _VALID_END_FIELD)

        container_table_name = get_table_name(container_type)

        part_fields = (_ROW_ID_FIELD, _ROOT_ROW_ID_FIELD, _CONTAINER_ROW_ID_FIELD, 
                       *part_stored.keys())

        container_fields = tuple(set(stored.keys()) - set(part_fields)) + additional_fields

        joined_table = _build_join_table(
            (part_table_name, _CONTAINER_ROW_ID_FIELD), 
            (container_table_name, _ROW_ID_FIELD),
        )

        yield _build_part_of_model_view_statement(
            get_table_name(type_), 
            joined_table,
            iter([
                f'JSON_EXTRACT({field_exprs(_JSON_FIELD, container_table_name)}, '
                    f'{field_exprs(_JSON_PATH_FIELD, part_table_name)}) AS `__json`'
            ]),
            _get_view_fields({
                part_table_name: part_fields,
                container_table_name: container_fields
            })
        )
    else:
        if _is_version_type(type_):
            if not get_identifying_fields(type_):
                _logger.fatal(f'{type_=} does not have any id field.')
                raise RuntimeError(L('identifying fields need for VersionMixin type. check {0}', type_))

        if _is_dated_type(type_): 
            # check dated type has id fields except applied_at
            id_fields = [f for f in get_identifying_fields(type_) if f != _APPLIED_AT_FIELD]

            if not id_fields:
                _logger.fatal(f'{type_=} does not have any id field for finding MAX applied_date')
                raise RuntimeError(L('identifying fields need for DatedMixin type. check {0}', type_))

        yield _build_model_table_statement(
            get_table_name(type_), 
            _get_table_fields(),
            _get_table_version_fields(type_),
            _get_table_stored_fields(stored, True),
            _get_table_indexes(type_, stored, False)
        )

        yield from _build_code_seq_statement(type_)

    yield from _build_statement_for_external_index_tables(type_)


def get_stored_fields_for_full_text_search(type_:Type[PersistentModelT]):
    return get_stored_fields_for(type_, MetaFullTextSearchedField)


def get_stored_fields_for_part_of(type_:Type[PersistentModelT]):
    return get_stored_fields_for(type_, 
        lambda paths, type_: 
            not _is_come_from_container_field(paths) 
            or has_metadata(type_, MetaFullTextSearchedField)
    )


def _is_come_from_container_field(paths:Tuple[str,...]) -> bool:
    return bool(len(paths) >= 2 and paths[0] == '..' and not paths[1].startswith('$'))


def get_stored_fields_for_external_index(type_:Type[PersistentModelT]):
    return {name:(path, field_type)
        for name, (path, field_type) 
        in get_stored_fields_for(type_, MetaIndexField).items()
        if is_derived_from(field_type, (tuple, list))
    }


def _build_model_table_statement(table_name:str, 
                                 *field_definitions: Iterator[str]) -> str:
    return join_line(
        f'CREATE TABLE IF NOT EXISTS {field_exprs(table_name)} (',
        tab_each_line(
            *field_definitions, use_comma=True
        ),
        f'){_ENGINE}'
    )


def _build_code_seq_statement(type_:Type[PersistentModel]) -> Iterator[str]:
    for name, modle_field in type_.__fields__.items():
        field_element_type = modle_field.type_

        if is_derived_from(field_element_type, SequenceStr):
            prefix = field_element_type.prefix

            yield join_line(
                f'CREATE SEQUENCE {get_seq_name(type_, name)} START WITH 1 INCREMENT 1'
            )

    
def get_query_for_next_seq(type_:Type, field:str) -> str:
    return f'SELECT NEXTVAL({get_seq_name(type_, field)}) as NEXT_SEQ'


def get_query_for_adjust_seq(type_:Type[PersistentModel]) -> Iterator[str]:
    for name, modle_field in type_.__fields__.items():
        field_element_type = modle_field.type_

        if is_derived_from(field_element_type, SequenceStr):
            prefix = field_element_type.prefix

            yield ';\n'.join([
                join_line(
                    f'SET @MAX_VALUE = (SELECT',
                    f"    CAST(REPLACE(MAX({field_exprs(name)}), '{prefix}', '') as INTEGER)",
                    f"FROM {get_table_name(type_)})"
                ),
                f"EXECUTE IMMEDIATE CONCAT('SELECT SETVAL({field_exprs(get_seq_name(type_, name))}, ', @MAX_VALUE, ')')"
            ])


def get_query_for_copying_single_object(
        tp: Type, id_field_and_value: Dict[str, Any],
        src_id:int, dest_id:int) -> Tuple[str, Dict[str, Any]]:

    assert src_id != dest_id

    return _get_sql_for_copying_objects(tp), id_field_and_value | {'src_id':src_id, 'dest_id':dest_id}
    

def _get_sql_for_copying_objects(tp:Type):
    id_fields = get_identifying_fields(tp)
    table_name = get_table_name(tp)

    for_id_fields= tuple((field_exprs(f), '=', f'%({f})s') for f in id_fields)

    assert id_fields

    dest_set_where = ((_SET_ID_FIELD, '=', '%(dest_id)s'),)
    src_set_where = ((_SET_ID_FIELD, '=', '%(src_id)s'),)

    if _is_version_type(tp):
        return join_line(
            f'INSERT INTO {table_name}',
            f'(',
            tab_each_line(
                [_SET_ID_FIELD, _JSON_FIELD, _VALID_START_FIELD, _SQUASHED_FROM_FIELD],
                field_exprs(id_fields),
                use_comma=True
            ),
            f')',
            f'SELECT',
            tab_each_line(
                f'%(dest_id)s',
                field_exprs(_JSON_FIELD, 'SRC'),
                f'IF({field_exprs(_VALID_START_FIELD, "SRC")} - IFNULL({field_exprs(_VALID_START_FIELD, "DEST")}, 0) < 0, @VERSION, {field_exprs(_VALID_START_FIELD, "SRC")})',
                field_exprs(_SQUASHED_FROM_FIELD, 'DEST') if _is_version_type(tp) else '',
                field_exprs(id_fields),
                use_comma=True
            ),
            f'FROM',
            tab_each_line(
                _alias_table_or_query(join_line(
                    f"SELECT",
                    tab_each_line(
                        field_exprs([_JSON_FIELD, _VALID_START_FIELD]),
                        field_exprs(id_fields),
                        use_comma=True
                    ),
                    f'FROM {table_name}',
                    _build_where(
                        for_id_fields,
                        (
                            (f'{_SET_ID_FIELD}', '=', '%(src_id)s'),
                            (f'{_VALID_START_FIELD}', '<=', '@VERSION'),
                            (f'{_VALID_END_FIELD}', '>', '@VERSION')
                        )
                    )
                ), "SRC"),
                f"LEFT JOIN",
                _alias_table_or_query(join_line(
                    f"SELECT",
                    tab_each_line(
                        field_exprs([_SET_ID_FIELD, _VALID_START_FIELD, _SQUASHED_FROM_FIELD]),
                        field_exprs(id_fields),
                        use_comma=True
                    ),
                    f'FROM {table_name}',
                    _build_where(
                        (
                            (f'{_SET_ID_FIELD}', '=', '%(dest_id)s'),
                            (f'{_VALID_START_FIELD}', '<=', '@VERSION'),
                            (f'{_VALID_END_FIELD}', '>', '@VERSION')
                        )
                    )
                ), "DEST"),
                f"USING ({join_line(field_exprs(id_fields), use_comma=True, new_line=False)})"
            ),
            # if version of records are same, we don't need to copy them.
            f'WHERE {field_exprs(_VALID_START_FIELD, "SRC")} != IFNULL({field_exprs(_VALID_START_FIELD, "DEST")}, 0)',
            f'RETURNING',
            tab_each_line(
                field_exprs([_ROW_ID_FIELD, _SET_ID_FIELD]),
                f"""CONCAT_WS(',', {
                    join_line(
                        field_exprs(
                            itertools.chain(id_fields, 
                            [_VALID_START_FIELD, _VALID_END_FIELD])
                        ), use_comma=True, new_line=False
                        )
                }) as {field_exprs(_AUDIT_MODEL_ID_FIELD)}""",
                f"'INSERTED:MERGE_SET' as {field_exprs('op')}",
                f"'{table_name}' as {field_exprs('table_name')}",
                f"{field_exprs(_VALID_START_FIELD)} as {field_exprs(_AUDIT_DATA_VERSION_FIELD)}",
                use_comma=True
            ),
            f';',

            # update __valid_end by lookup table.
            f'UPDATE {table_name} JOIN (',
            tab_each_line(
                f'SELECT',
                tab_each_line(
                    f'{field_exprs(_ROW_ID_FIELD)}',
                    f'IFNULL(LEAD({field_exprs(_VALID_START_FIELD)}) over '
                    f'(PARTITION BY {join_line(field_exprs(id_fields), new_line=False)} '
                    f'ORDER BY {field_exprs(_VALID_START_FIELD)}), {_BIG_INT_MAX}) as __NEW_VALID_END',
                    use_comma=True
                ),
                f'FROM {table_name}',
                f'WHERE {field_exprs(_VALID_END_FIELD)} = {_BIG_INT_MAX}',
                f'AND {field_exprs(_SET_ID_FIELD)} = %(dest_id)s',
            ),
            f') as SRC USING ({field_exprs(_ROW_ID_FIELD)})',
            f'SET {field_exprs(_VALID_END_FIELD)} = __NEW_VALID_END',
            _build_where(
                for_id_fields, dest_set_where
            ),
            f';',
        )
    else:
        return join_line(
            f'IF ( SELECT 1 = 1 FROM {table_name} {_build_where(for_id_fields, dest_set_where)}) THEN',
            # update record
            tab_each_line(
                f'REPLACE INTO {table_name}', 
                '(',
                tab_each_line(
                    field_exprs([_SET_ID_FIELD, *id_fields, _JSON_FIELD, _VALID_START_FIELD]),
                    use_comma=True
                ),
                ')',
                f'SELECT',
                tab_each_line(
                    '*'
                ),
                f'FROM',
                _alias_table_or_query(
                    join_line(
                        f'SELECT',
                        tab_each_line(
                            '%(dest_id)s',
                            field_exprs(id_fields),
                            field_exprs(_JSON_FIELD, 'SRC'),
                            f'IF({field_exprs(_VALID_START_FIELD, "SRC")} - IFNULL({field_exprs(_VALID_START_FIELD, "DEST")}, 0) < 0, @VERSION, {field_exprs(_VALID_START_FIELD, "SRC")})',
                            use_comma=True
                        ),
                        f'FROM',
                        tab_each_line(
                            _alias_table_or_query(join_line(
                                f"SELECT",
                                tab_each_line(
                                    field_exprs([_JSON_FIELD, _VALID_START_FIELD]),
                                    field_exprs(id_fields),
                                    use_comma=True
                                ),
                                f'FROM {table_name}',
                                _build_where(
                                    for_id_fields,
                                    (
                                        (f'{field_exprs(_SET_ID_FIELD)}', '=', '%(src_id)s'),
                                        (f'{field_exprs(_VALID_START_FIELD)}', '<=', '@VERSION'),
                                        (f'{field_exprs(_VALID_END_FIELD)}', '>', '@VERSION')
                                    )
                                )
                            ), "SRC"),
                            f"LEFT JOIN",
                            _alias_table_or_query(join_line(
                                f"SELECT",
                                tab_each_line(
                                    field_exprs([_SET_ID_FIELD, _VALID_START_FIELD]),
                                    field_exprs(id_fields),
                                    use_comma=True
                                ),
                                f'FROM {table_name}',
                                _build_where(
                                    (
                                        (f'{field_exprs(_SET_ID_FIELD)}', '=', '%(dest_id)s'),
                                        (f'{field_exprs(_VALID_START_FIELD)}', '<=', '@VERSION'),
                                        (f'{field_exprs(_VALID_END_FIELD)}', '>', '@VERSION')
                                    )
                                )
                            ), "DEST"),
                            f"USING ({join_line(field_exprs(id_fields), use_comma=True, new_line=False)})"
                        ),
                        f'WHERE {field_exprs(_VALID_START_FIELD, "SRC")} != IFNULL({field_exprs(_VALID_START_FIELD, "DEST")}, 0)',
                    ),
                    "_T"
                ),
                f'RETURNING',
                tab_each_line(
                    field_exprs(_SET_ID_FIELD),
                    field_exprs(_ROW_ID_FIELD),
                    f"{field_exprs(_VALID_START_FIELD)} as {field_exprs(_AUDIT_DATA_VERSION_FIELD)}",
                    f"'INSERTED:MERGE_SET' as {field_exprs('op')}",
                    f"'{table_name}' as {field_exprs('table_name')}",
                    f"""CONCAT_WS(',', {
                        join_line(field_exprs(id_fields), use_comma=True, new_line=False) 
                        if id_fields
                        else 
                        "''"
                    }) as {field_exprs(_AUDIT_MODEL_ID_FIELD)}""",
                    use_comma=True
                ),
                f';',
                f'ELSE', 
                # insert record instead of using insert duplicate.
                tab_each_line(
                    f'INSERT INTO {table_name}', 
                    '(',
                    tab_each_line(
                        field_exprs([_JSON_FIELD, _SET_ID_FIELD, *id_fields, _VALID_START_FIELD]),
                        use_comma=True
                    ),
                    ')',
                    f'SELECT',
                    tab_each_line(
                        field_exprs(_JSON_FIELD),
                        f'%(dest_id)s',
                        field_exprs(id_fields),
                        field_exprs(_VALID_START_FIELD),
                        use_comma=True
                    ),
                    f'FROM {table_name}',
                    _build_where(
                        for_id_fields, src_set_where
                    ),
                    f'RETURNING',
                    tab_each_line(
                        field_exprs(_SET_ID_FIELD),
                        field_exprs(_ROW_ID_FIELD),
                        f"{field_exprs(_VALID_START_FIELD)} as {field_exprs(_AUDIT_DATA_VERSION_FIELD)}",
                        f"'INSERTED:MERGE_SET' as {field_exprs('op')}",
                        f"'{table_name}' as {field_exprs('table_name')}",
                        f"""CONCAT_WS(',', {
                            join_line(field_exprs(id_fields), use_comma=True, new_line=False) 
                            if id_fields
                            else 
                            "''"
                        }) as {field_exprs(_AUDIT_MODEL_ID_FIELD)}""",
                        use_comma=True
                    ),
                    f';',
                ),
                f'END IF', 
            )
        )


def _build_part_of_model_view_statement(view_name:str, joined_table:str,
                                               *view_fields:Iterator[str],
                                               ) -> str:
    return join_line(
        f'CREATE VIEW IF NOT EXISTS {field_exprs(view_name)} AS (',
        tab_each_line(
            'SELECT',
            tab_each_line(*view_fields, use_comma=True),
            f'FROM {joined_table}',
        ),
        ')'
    )


def _build_statement_for_external_index_tables(type_:Type):
    for field_name, (_, field_type) in get_stored_fields_for_external_index(type_).items():
        yield _build_model_table_statement(
            get_table_name(type_, field_name), 
            _get_external_index_table_fields(field_name, field_type),
            _get_external_index_table_indexes(field_name, field_type),
        )

 
def _build_join_table(*table_names:Tuple[str, str]) -> str:
    table_iter = iter(table_names)
    main_table, main_index = next(table_iter)

    target_tables = [field_exprs(main_table)]

    ref_index = field_exprs(main_index, main_table)

    for table, index_name in table_iter:
        target_tables.append(f'JOIN {field_exprs(table)} ON {field_exprs(index_name, table)} = {ref_index}')

    return join_line(target_tables)


def _get_table_fields() -> Iterator[str]:
    yield f'{field_exprs(_ROW_ID_FIELD)} BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY'
    yield f'{field_exprs(_SET_ID_FIELD)} BIGINT UNSIGNED NOT NULL DEFAULT 0'
    yield f'{field_exprs(_JSON_FIELD)} {_JSON_TYPE} {_JSON_CHECK.format(field_exprs(_JSON_FIELD))}'


def _get_table_version_fields(type_:Type) -> Iterator[str]:
    yield f'{field_exprs(_VALID_START_FIELD)} BIGINT'
    yield f'{field_exprs(_VALID_END_FIELD)} BIGINT DEFAULT {_BIG_INT_MAX}'

    if _is_version_type(type_):
        yield f'{field_exprs(_SQUASHED_FROM_FIELD)} BIGINT'


def _get_external_index_table_fields(field_name:str, field_type:Type) -> Iterator[str]:
    # we don't need primary key, because there is no field which is for full text searching.
    param_type = get_args_of_base_generic_alias(field_type, list, tuple)[0]

    yield f'{field_exprs(_ORG_ROW_ID_FIELD)} BIGINT'
    yield f'{field_exprs(_ROOT_ROW_ID_FIELD)} BIGINT'
    yield f'{field_exprs(field_name)} {_get_field_db_type(param_type)}'


def _get_part_table_fields() -> Iterator[str]:
    yield f'{field_exprs(_ROW_ID_FIELD)} BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY'
    yield f'{field_exprs(_ROOT_ROW_ID_FIELD)} BIGINT'
    yield f'{field_exprs(_CONTAINER_ROW_ID_FIELD)} BIGINT'
    yield f'{field_exprs(_JSON_PATH_FIELD)} VARCHAR(255)'


def _get_table_stored_fields(stored_fields:StoredFieldDefinitions, generated:bool = False) -> Iterator[str]:
    for field_name, (paths, field_type) in stored_fields.items():
        stored = '' if not generated or not paths else _generate_stored_for_json_path(paths, field_type)
        yield f"""{field_exprs(field_name)} {_get_field_db_type(field_type)}{stored}"""


def _get_view_fields(fields:Dict[str, Tuple[str, ...]]) -> Iterator[str]:
    for table_name, field_names in fields.items():
        for field_name in field_names:
            yield f'{field_exprs(field_name, table_name)}'


def _get_table_indexes(type_:Type, stored_fields:StoredFieldDefinitions, for_part:bool) -> Iterator[str]:
    identified_fields = [field_name 
                        for field_name, (_, field_type) in stored_fields.items()
                        if has_metadata(field_type, MetaIdentifyingField)]

    if _is_version_type(type_):
        identified_fields.append(_VALID_START_FIELD)

    if identified_fields:
        yield f"""UNIQUE KEY `identifying_index` ({join_line(
            field_exprs(
                itertools.chain([] if for_part else [_SET_ID_FIELD], identified_fields)
            ), new_line=False, use_comma=True)})"""
    
    for field_name, (_, field_type) in stored_fields.items():
        if has_metadata(field_type, MetaIdentifyingField):
            continue

        key_def = _generate_key_definition(field_type)

        if key_def: 
            if key_def == 'UNIQUE KEY' and not for_part:
                fields = [_SET_ID_FIELD, field_name]
            else:
                fields = field_name
            yield f"""{key_def} `{field_name}_index` ({join_line(
                field_exprs(fields), new_line=False, use_comma=True)})"""

    full_text_searched_fields = set(
        field_name for field_name, (_, field_type) in stored_fields.items()
        if has_metadata(field_type, MetaFullTextSearchedField)
    )

    if full_text_searched_fields:
        yield f'''FULLTEXT INDEX `ft_index` ({
            join_line(
                field_exprs(sorted(full_text_searched_fields)),
                new_line=False, use_comma=True
            )
        }) {_FULL_TEXT_SEARCH_OPTION}'''


def _get_part_table_indexes() -> Iterator[str]:
    yield f'KEY `{_ROOT_ROW_ID_FIELD}_index` ({field_exprs(_ROOT_ROW_ID_FIELD)})'


def _get_external_index_table_indexes(field_name:str, field_type:Type) -> Iterator[str]:
    yield f"""KEY `{_ORG_ROW_ID_FIELD}_index` ({field_exprs(_ORG_ROW_ID_FIELD)})"""
    yield f"""KEY `{field_name}_index` ({field_exprs(field_name)})"""

    # for external, we don't need full text search index.
    #if is_derived_from(field_type, FullTextSearchedMixin):
    #    yield f"""FULLTEXT INDEX `ft_index` ({field_exprs(field_name)}) {_FULL_TEXT_SEARCH_OPTION}"""


def _get_field_db_type(type_:Type) -> str:
    if type_ is bool:
        return 'BOOL'

    if type_ is int:
        return 'BIGINT'

    # if type_ is float:
    #     return 'FLOAT'

    if is_derived_from(type_, Decimal):
        if is_derived_from(type_, ConstrainedDecimal):
            max_digits = min(type_.max_digits or _MAX_DECIMAL_DIGITS, _MAX_DECIMAL_DIGITS)

            return f'DECIMAL({max_digits})'

        return 'DECIMAL(65)'

    if is_derived_from(type_, datetime):
        return 'DATETIME(6)'

    if is_derived_from(type_, date):
        return 'DATE'

    if is_derived_from(type_, str):
        if is_derived_from(type_, ConstrainedStr):
            max_length = min(type_.max_length or _MAX_VAR_CHAR_LENGTH, _MAX_VAR_CHAR_LENGTH)
            return f'VARCHAR({max_length})'

        meta = get_metadata_for(type_, MetaIndexField) or get_metadata_for(type_, MetaFullTextSearchedField)

        if meta:
            max_length = min(meta.max_length or _MAX_VAR_CHAR_LENGTH, _MAX_VAR_CHAR_LENGTH)
            return f'VARCHAR({max_length})'

        return 'TEXT'

    if get_metadata_for(type_, MetaStoredField):
        return 'TEXT'

    _logger.fatal(f'unsupported type: {type_}')
    raise RuntimeError(L('{0} is not the supported type in database.', type_))


def _f(field:str) -> str:
    field = field.strip()

    if field.startswith('`') and field.endswith('`'):
        return field
    
    if field == '*':
        return field

    if ' ' in field or '(' in field: # if field has function or "as" statement.
        return field

    return f'`{field}`'


def _generate_stored_for_json_path(json_paths: Tuple[str, ...], field_type:Type) -> str:
    if has_metadata(field_type, MetaIdentifyingField):
        # this value will be update by sql params.
        return ''

    json_path = '$.' + '.'.join([path.replace('$.', '') for path in json_paths if path != '$'])

    if is_derived_from(field_type, (list, tuple)):
        return f" AS (JSON_EXTRACT(`{_JSON_FIELD}`, '{json_path}')) STORED"
    else:
        return f" AS (JSON_VALUE(`{_JSON_FIELD}`, '{json_path}')) STORED"


def _generate_key_definition(type_:Type) -> str:
    if has_metadata(type_, MetaUniqueIndexField):
        return 'UNIQUE KEY'
    elif has_metadata(type_, MetaIndexField):
        return 'KEY'

    return ''


def get_query_and_args_for_inserting_audit(audit:VersionInfo):
    return _get_sql_for_allocating_version(), asdict(audit)


def get_query_and_args_for_auditing(item:Dict[str, Any]):
    return _get_sql_for_auditing_model(), item


def get_query_and_args_for_getting_version(when:datetime | None):
    if when is None:
        return f'SELECT MAX(version) as version FROM {field_exprs(_VERSION_INFO_TABLE)}', {}
    else:
        return (
            f'SELECT MAX(version) as version FROM {field_exprs(_VERSION_INFO_TABLE)} '
            f'WHERE `when` <= %(ref_date)s', 
            {'ref_date': when}
        )


def get_query_and_args_for_getting_version_info(version:int):
    return f'SELECT * FROM {field_exprs(_VERSION_INFO_TABLE)} WHERE version = %(version)s', {'version':version}


def get_query_and_args_for_getting_model_changes_of_version(version:int):
    return f'SELECT * FROM {field_exprs(_VERSION_CHANGE_TABLE)} WHERE version = %(version)s', {'version':version}


def get_query_and_args_for_upserting(model:PersistentModel, set_id:int):
    query_args : Dict[str, Any] = get_identifying_field_values(model)

    query_args[_JSON_FIELD] = model.json()
    query_args[_SET_ID_FIELD] = set_id

    return _get_sql_for_upserting_single_object(cast(Type, type(model))), query_args


def get_query_and_args_for_squashing(type_:Type[PersistentModelT], identifier:Dict[str, Any], set_id:int):
    if not _is_version_type(type_):
        _logger.fatal(f'{type_=} is not derived from VersinoMixin. it suppport to squash VersionMixin objects only.')
        raise RuntimeError(L('to squash is not supported for non version type. check {0}', type_))

    return _get_sql_for_squashing_single_object(cast(Type, type_)), identifier | {'__set_id': set_id}
    

@functools.lru_cache
def _get_sql_for_squashing_single_object(model_type:Type):
    table_name = get_table_name(model_type)
    id_fields = get_identifying_fields(model_type)
    for_id_fields = [f'{field_exprs(f)} = %({f})s' for f in id_fields]
    for_id_fields.append(f'{field_exprs(_SET_ID_FIELD)} = %({_SET_ID_FIELD})s')

    return join_line(
        # find SQUASHED_FROM from current 
        f'SELECT MIN({field_exprs(_SQUASHED_FROM_FIELD)})',
        f'INTO @SQUASHED_FROM',
        f'FROM {table_name}',
        f'WHERE',
        tab_each_line(
            '\n AND '.join(
                itertools.chain(
                    for_id_fields,
                    [
                        f'{field_exprs(_VALID_START_FIELD)} <= @VERSION',
                        f'@VERSION < {field_exprs(_VALID_END_FIELD)}',
                        f'{field_exprs(_SQUASHED_FROM_FIELD)} IS NOT NULL',
                    ]
                )
            )
        ),
        ';',

        # find new_valid_start
        # find minimum of _valid_start which have same @SQUASHED_FROM.
        f'SELECT MIN({field_exprs(_VALID_START_FIELD)})',
        f'INTO @NEW_VALID_START',
        f'FROM {table_name}',
        f'WHERE',
        tab_each_line(
            '\n AND '.join(
                itertools.chain(
                    for_id_fields,
                    [
                        f'{field_exprs(_SQUASHED_FROM_FIELD)} = @SQUASHED_FROM',
                    ]
                )
            )
        ),
        ';',

        # delete records from SQUASHED_FROM and previous of current
        f'DELETE FROM {table_name}',
        f'WHERE',
        tab_each_line(
            '\n AND '.join(
                itertools.chain(
                    for_id_fields,
                    [
                        f'@SQUASHED_FROM <= {field_exprs(_VALID_START_FIELD)}',
                        f'{field_exprs(_VALID_END_FIELD)} <= @VERSION',
                    ]
                )
            )
        ),
        # returning for update model_change
        f'RETURNING',
        tab_each_line(
            field_exprs(_ROW_ID_FIELD),
            field_exprs(_SET_ID_FIELD),
            f"""CONCAT_WS(',', {
                join_line(
                    field_exprs(
                        itertools.chain(id_fields, 
                        [_VALID_START_FIELD, _VALID_END_FIELD])
                    ), use_comma=True, new_line=False
                    )
            }) as {field_exprs(_AUDIT_MODEL_ID_FIELD)}""",
            f"'PURGED:SQUASHED' as {field_exprs('op')}",
            f"'{table_name}' as {field_exprs('table_name')}",
            f"NULL as {field_exprs(_AUDIT_DATA_VERSION_FIELD)}",
            use_comma=True
        ),
        ';',
 
        # reset SQAUSHED_FROM and extend the valid period of current record by
        # setting NEW_VALID_START
        f'UPDATE {table_name}',
        f'SET',
        tab_each_line(
            f'{field_exprs(_SQUASHED_FROM_FIELD)} = NULL',
            f'{field_exprs(_VALID_START_FIELD)} = @NEW_VALID_START',
            use_comma=True
        ),
        f'WHERE',
        tab_each_line(
            '\n AND '.join(
                itertools.chain(
                    for_id_fields,
                    [
                        f'{field_exprs(_VALID_START_FIELD)} <= @VERSION',
                        f'@VERSION < {field_exprs(_VALID_END_FIELD)}',
                    ]
                )
            )
        ),
        ';',
   )


@functools.lru_cache
def _get_sql_for_upserting_single_object(model_type:Type):
    table_name = get_table_name(model_type)
    id_fields = get_identifying_fields(model_type)

    for_id_fields = []
    for_id_fields.append(f'{field_exprs(_SET_ID_FIELD)} = %({_SET_ID_FIELD})s')
    for_id_fields.extend([f'{field_exprs(f)} = %({f})s' for f in id_fields])

    if _is_version_type(model_type):
        assert id_fields

        where_cond = tab_each_line(
            '\nAND '.join(
                itertools.chain(
                    for_id_fields,
                    [
                        f'{field_exprs(_VALID_START_FIELD)} <= @VERSION',
                        f'@VERSION < {field_exprs(_VALID_END_FIELD)}',
                    ]
                )
            )
        )

        return join_line(                    
            # get squashed_from.
            # MIN(squashed_from_field) of current record will be SQUASHED_FROM
            f'SELECT MIN({field_exprs(_SQUASHED_FROM_FIELD)})',
            f'INTO @SQUASHED_FROM',
            f'FROM {table_name}',
            f'WHERE',
            where_cond, 
            ';',

            # update valid_end of current record 
            f'UPDATE {table_name}',
            f'SET {field_exprs(_VALID_END_FIELD)} = @VERSION',
            f'WHERE',
            where_cond,
            ';',

            ## we will insert new record which is be new current record.
            # insert new record which start from current.
            f'INSERT INTO {table_name}',
            '(',
            tab_each_line(
                field_exprs([
                    _JSON_FIELD, 
                    _VALID_START_FIELD, 
                    _SQUASHED_FROM_FIELD, 
                    _SET_ID_FIELD,
                    *id_fields
                ]),
                use_comma=True
            ),
            ')',
            'VALUES',
            '(',
            tab_each_line(
                '%(__json)s',
                '@VERSION',
                'IFNULL(@SQUASHED_FROM, @VERSION)',
                f'%({_SET_ID_FIELD})s',
                [f'%({f})s' for f in id_fields],
                use_comma=True
            ),
            ')',
            f'RETURNING',
            tab_each_line(
                field_exprs(_SET_ID_FIELD),
                field_exprs(_ROW_ID_FIELD),
                f"""CONCAT_WS(',', {
                    join_line(field_exprs(
                        itertools.chain(id_fields, [_VALID_START_FIELD])
                    ), use_comma=True, new_line=False)
                }) as {field_exprs(_AUDIT_MODEL_ID_FIELD)}""",
                f"'INSERTED' as {field_exprs('op')}",
                f"'{table_name}' as {field_exprs('table_name')}",
                f"@VERSION as {field_exprs(_AUDIT_DATA_VERSION_FIELD)}",
                use_comma=True
            )
        )
    elif id_fields:        
        where_cond = tab_each_line(
            '\nAND '.join(
                for_id_fields,
            )
        )

        # Unique Index가 여러개 인 경우 INSERT INTO DUPLICATE를 하게 되면,
        # 오류를 내야 함에도 불구하고 UPDATE가 되는 경우가 있어 아래와 같이 
        # 해당 식별 필드에 대응하는 값이 있으면 UPDATE를 없으면 INSERT를
        # 하도록 한다. 

        return join_line(
            f'IF ( SELECT 1 = 1 FROM {table_name} WHERE {where_cond}) THEN',
            # update record
            tab_each_line(
                f'UPDATE {table_name}', 
                f'SET',
                tab_each_line(
                    f'{field_exprs(_JSON_FIELD)} = %({_JSON_FIELD})s',
                    f'{field_exprs(_VALID_START_FIELD)} = @VERSION',
                    use_comma=True
                ),
                f'WHERE',
                tab_each_line(
                    where_cond, 
                ),
                f';',
                f'SELECT',
                tab_each_line(
                    field_exprs([_SET_ID_FIELD, _ROW_ID_FIELD]),
                    f"'INSERTED' as {field_exprs('op')}",
                    f"'{table_name}' as {field_exprs('table_name')}",
                    f"""CONCAT_WS(',', {
                        join_line(field_exprs(id_fields), use_comma=True, new_line=False) 
                        if id_fields
                        else 
                        "''"
                    }) as {field_exprs(_AUDIT_MODEL_ID_FIELD)}""",
                    f"@VERSION as {field_exprs(_AUDIT_DATA_VERSION_FIELD)}",
                    use_comma=True
                ),
                f'FROM {table_name}',
                f'WHERE',
                tab_each_line(
                    where_cond, 
                ),
                f';',
            ),
            f'ELSE', 
            # insert record
            tab_each_line(
                f'INSERT INTO {table_name}', 
                '(',
                tab_each_line(
                    field_exprs([_JSON_FIELD, _SET_ID_FIELD, *id_fields, _VALID_START_FIELD]),
                    use_comma=True
                ),
                ')',
                f'VALUES',
                '(',
                tab_each_line(
                    f'%({_JSON_FIELD})s',
                    f'%({_SET_ID_FIELD})s',
                    [f'%({f})s' for f in id_fields],
                    '@VERSION',
                    use_comma=True
                ),
                ')',
                f'RETURNING',
                tab_each_line(
                    field_exprs(_SET_ID_FIELD),
                    field_exprs(_ROW_ID_FIELD),
                    f"'INSERTED' as {field_exprs('op')}",
                    f"'{table_name}' as {field_exprs('table_name')}",
                    f"""CONCAT_WS(',', {
                        join_line(field_exprs(id_fields), use_comma=True, new_line=False) 
                        if id_fields
                        else 
                        "''"
                    }) as {field_exprs(_AUDIT_MODEL_ID_FIELD)}""",
                    f"@VERSION as {field_exprs(_AUDIT_DATA_VERSION_FIELD)}",
                    use_comma=True
                ),
                f';',
            ),
            f'END IF', 
        )
    else:
        return join_line(
            f'INSERT INTO {table_name}', 
            '(',
            tab_each_line(
                field_exprs([_JSON_FIELD, _SET_ID_FIELD, *id_fields, _VALID_START_FIELD]),
                use_comma=True
            ),
            ')',
            f'VALUES',
            '(',
            tab_each_line(
                f'%({_JSON_FIELD})s',
                f'%({_SET_ID_FIELD})s',
                [f'%({f})s' for f in id_fields],
                '@VERSION',
                use_comma=True
            ),
            ')',
            f'RETURNING',
            tab_each_line(
                field_exprs(_SET_ID_FIELD),
                field_exprs(_ROW_ID_FIELD),
                f"'UPSERTED' as {field_exprs('op')}",
                f"'{table_name}' as {field_exprs('table_name')}",
                f"""CONCAT_WS(',', {
                    join_line(field_exprs(id_fields), use_comma=True, new_line=False) 
                    if id_fields
                    else 
                    "''"
                }) as {field_exprs(_AUDIT_MODEL_ID_FIELD)}""",
                f"@VERSION as {field_exprs(_AUDIT_DATA_VERSION_FIELD)}",
                use_comma=True
            )
        ) 




# At first, I would use the JSON_TABLE function in mariadb. 
# but it does not support the object or array type for field 
# value of result.  
# but, if NESTED PATH, JSON_ARRAYAGG would be used, it will be able to
# generate the array type as following example.
# 
# *JSON:
# {
#   "items": [  
#      {
#          "array": [0, 1, 2, 3]
#      }
#   ]
# }
#
# *SQL:
# SELECT 
#   JSON_ARRAYAGG(_array_field)
# FROM 
# JSON_TABLE(
#   @JSON, 
#   '$.items[*]' COLUMNS (
#      `_id` TEXT PATH '$.id',   
#      `_order` FOR ORDINALITY,     
#      NESTED PATH '$.array[*]' COLUMNS (       
#        `_array_field` INT PATH '$'     
#      ) 
#   ) 
# ) as T1
# GROUP BY
#   _order
# 

def get_sql_for_upserting_parts(model_type:Type) -> Dict[Type, Tuple[str, ...]]:
    type_sqls : Dict[str, Tuple[str, str]] = {}
    part_types = get_part_types(model_type)

    is_root = not is_derived_from(model_type, PartOfMixin)
    sqls = []
    
    for part_type in part_types:
        part_fields = get_field_names_for(model_type, part_type)

        assert part_fields

        delete_sql = join_line([
            f'DELETE FROM {get_table_name(part_type, _PART_BASE_TABLE)}',
            f'WHERE {field_exprs(_ROOT_ROW_ID_FIELD)} = %({_ROOT_ROW_ID_FIELD})s'
        ])

        sqls.append(delete_sql)

        fields = get_stored_fields_for_part_of(part_type)

        target_fields = tuple(itertools.chain(
            [_ROOT_ROW_ID_FIELD, _CONTAINER_ROW_ID_FIELD, _JSON_PATH_FIELD],
            fields.keys()
        ))

        root_field = 'CONTAINER.' + (
            field_exprs(_ROW_ID_FIELD) 
            if is_root else field_exprs(_ROOT_ROW_ID_FIELD)
        )

        for part_field in part_fields:
            is_collection = is_field_list_or_tuple_of(model_type, part_field)
            json_path = f'$.{part_field}'

            fields_from_json_table = [f for f in target_fields if f in fields and fields[f][0][0] != '..']
            fields_from_container_json = [f for f in target_fields if f in fields and fields[f][0][0] == '..']

            insert_sql = join_line([
                f'INSERT INTO {get_table_name(part_type, _PART_BASE_TABLE)}',
                f'(',
                tab_each_line(
                    field_exprs(target_fields),
                    use_comma=True
                ),
                f')',
                f'SELECT',
                tab_each_line(
                    _generate_select_field_of_json_table(target_fields, fields), 
                    use_comma=True
                ),
                f'FROM (',
                tab_each_line(
                    f'SELECT',
                    tab_each_line(
                        [ field_exprs(_PART_ORDER_FIELD) ],
                        [
                            f'{root_field} as {field_exprs(_ROOT_ROW_ID_FIELD)}',
                            f'CONTAINER.{field_exprs(_ROW_ID_FIELD)} as {field_exprs(_CONTAINER_ROW_ID_FIELD)}',
                            (
                                f"CONCAT('{json_path}[', {field_exprs(_PART_ORDER_FIELD)} - 1, ']') "
                                f"as {field_exprs(_JSON_PATH_FIELD)}"
                                if is_collection else 
                                f"'{json_path}' as {field_exprs(_JSON_PATH_FIELD)}"
                            )
                        ],
                        [_generate_json_eval(f, fields[f], 'CONTAINER') for f in fields_from_container_json],
                        field_exprs(fields_from_json_table, table_name='__PART_JSON_TABLE'),
                        use_comma=True
                    ),
                    f'FROM',
                    tab_each_line(
                        [f'{get_table_name(model_type)} as CONTAINER'],
                        _generate_json_table_for_part_of(json_path, is_collection, fields, '__PART_JSON_TABLE'),
                        use_comma=True
                    ),
                    f'WHERE',
                    tab_each_line(
                        f'{root_field} = %({_ROOT_ROW_ID_FIELD})s'
                    ),
                ),
                ') AS T1',
                # group by should be applied after table was generated. 
                # if you apply on JSON_TABLE, sometime it cannot generate data properly.
                f'GROUP BY',
                tab_each_line(
                    field_exprs([_CONTAINER_ROW_ID_FIELD, _PART_ORDER_FIELD]),
                    use_comma=True
                )
            ])

            sqls.append(insert_sql)

        type_sqls[part_type] = tuple(sqls)

    return type_sqls


def get_sql_for_upserting_external_index(model_type:Type) -> Iterator[str]:
    is_part = is_derived_from(model_type, PartOfMixin)

    for field_name, (json_paths, field_type) in get_stored_fields_for_external_index(model_type).items():

        if json_paths[0] == '..':
            root_path = '$[*]'
            json_field = field_name
            items = [(['$'], field_name, field_type) ]
        else:
            root_path = '$'
            json_field = _JSON_FIELD
            items = [(list(json_paths), field_name, field_type) ]

        table_name = get_table_name(model_type, field_name)

        # delete previous items.
        yield join_line([
            f'DELETE FROM {table_name}',
            f'WHERE {field_exprs(_ROOT_ROW_ID_FIELD)} = %({_ROOT_ROW_ID_FIELD})s'
        ])

        yield join_line(
            f'INSERT INTO {table_name}',
            f'(',
            tab_each_line(
                field_exprs([_ROOT_ROW_ID_FIELD, _ORG_ROW_ID_FIELD, field_name]),
                use_comma=True
            ),
            f')',
            f'SELECT',
            tab_each_line(
                field_exprs(_ROOT_ROW_ID_FIELD if is_part else _ROW_ID_FIELD, '__ORG') ,
                field_exprs(_ROW_ID_FIELD, '__ORG'),
                field_exprs(field_name, '__EXT_JSON_TABLE'),
                use_comma=True
            ),
            f'FROM',
            tab_each_line(
                f'{get_table_name(model_type)} AS __ORG',
                join_line(
                    'JSON_TABLE(',
                    tab_each_line(
                        field_exprs(json_field, '__ORG'),
                        _generate_nested_json_table(root_path, items),
                        use_comma=True
                    ),
                    ') AS __EXT_JSON_TABLE'
                ),
                use_comma=True
            ),
            f'WHERE',
            tab_each_line(
                f"""{field_exprs(_ROOT_ROW_ID_FIELD if is_part else _ROW_ID_FIELD, '__ORG')} = %({_ROOT_ROW_ID_FIELD})s"""
            )
        )


def _generate_json_table_for_part_of(json_path: str,
                                     is_collection: bool, fields: StoredFieldDefinitions,
                                     part_json_table_name: str) -> str:
    items = [
        (
            [json_path + ('[*]' if is_collection else ''), *paths],
            field_name, field_type
        )
        for field_name, (paths, field_type) in fields.items()
        if paths[0] != '..'
    ]

    if not items:
        items = [(
            [json_path + ('[*]' if is_collection else ''), '$'], 
            '', None
        )]

    return join_line(
        'JSON_TABLE(',
        tab_each_line(
            f'CONTAINER.{field_exprs(_JSON_FIELD)}', 
            _generate_nested_json_table('$', items), 
            use_comma=True
        ),
        f') AS {part_json_table_name}'
    )


def _generate_json_eval(field_name:str, paths_and_type:Tuple[Tuple[str,...], Type], table_name:str) -> str:
    paths, field_type = paths_and_type

    assert  paths and paths[0] == '..'

    if len(paths) != 2:
        _logger.fatal(f'{field_name=}, {paths=}, {field_type=}. check paths have 2 element.')
        raise RuntimeError(L('paths should have 2 items for generating value from container. check {0}', paths))

    if is_list_or_tuple_of(field_type):
        return f"JSON_EXTRACT({field_exprs(_JSON_FIELD, table_name)}, '{paths[1]}') as {field_exprs(field_name)}"
    else:
        return f"JSON_VALUE({field_exprs(_JSON_FIELD, table_name)}, '{paths[1]}') as {field_exprs(field_name)}"
        

def _generate_select_field_of_json_table(target_fields:Tuple[str,...], 
                                         fields: StoredFieldDefinitions
                                         ) -> Iterable[str]:
    for field_name in target_fields:
        if field_name in fields:
            json_path, field_type = fields[field_name]

            if is_list_or_tuple_of(field_type) and json_path[0] != '..':
                yield f"JSON_ARRAYAGG({field_exprs(field_name)}) AS {field_exprs(field_name)}"
                continue 

        yield field_exprs(field_name)


def _generate_nested_json_table(first_path:str, 
                                items: Iterable[Tuple[List[str], str, Type]], depth: int = 0) -> str:
    nested_items: DefaultDict[str, List[Tuple[List[str], str, Type]]] = defaultdict(list)

    columns = []

    if depth == 1:
        columns.append(f'{field_exprs(_PART_ORDER_FIELD)} FOR ORDINALITY')

    for paths, field_name, field_type in items:
        if len(paths) == 1:
            if field_name:
                columns.append(f"{field_exprs(field_name)} {_get_field_db_type(field_type)} PATH '{paths[0]}'")
        else:
            nested_items[paths[0]].append((paths[1:], field_name, field_type))

    for path, items in nested_items.items():
        columns.append(join_line(_generate_nested_json_table(path, items, depth + 1)))

    return join_line(
        f"{'' if depth == 0 else 'NESTED PATH '}'{first_path}' COLUMNS (",
        tab_each_line(
            columns, 
            use_comma=True
        ),
        f")"
    )


def get_sql_for_purging_parts(model_type:Type) -> Dict[Type, Tuple[str, ...]]:
    type_sqls : Dict[str, Tuple[str, str]] = {}
    part_types = get_part_types(model_type)

    sqls = []
    
    for part_type in part_types:
        part_fields = get_field_names_for(model_type, part_type)

        assert part_fields

        delete_sql = join_line([
            f'DELETE FROM {get_table_name(part_type, _PART_BASE_TABLE)}',
            f'WHERE {field_exprs(_ROOT_ROW_ID_FIELD)} in %(__root_row_ids)s'
        ])

        sqls.append(delete_sql)

        type_sqls[part_type] = tuple(sqls)

    return type_sqls


def get_sql_for_purging_external_index(model_type:Type) -> Iterator[str]:
    for field_name in get_stored_fields_for_external_index(model_type):
        table_name = get_table_name(model_type, field_name)

        # delete previous items.
        yield join_line([
            f'DELETE FROM {table_name}',
            f'WHERE {field_exprs(_ROOT_ROW_ID_FIELD)} in %(__root_row_ids)s'
        ])


# fields is the json key , it can contain '.' like product.name
def get_query_and_args_for_reading(type_:Type[PersistentModelT], 
                                   fields: Tuple[str, ...] | str, 
                                   where: NormalizedQueryConditionType,
                                   set_id: int,
                                   *,
                                   order_by: Tuple[str, ...] | str = tuple(), 
                                   offset : None | int = None, 
                                   limit : None | int = None,
                                   unwind: Tuple[str, ...] | str = tuple(),
                                   ns_types:Dict[str, Type[PersistentModelT]] | None = None,
                                   base_type:Optional[Type[PersistentModelT]] = None,
                                   for_count:bool = False,
                                   version:int = 0, 
                                   current:date | None = None,
                                   ):
    '''
        Get 'field' values of record which is all satified the where conditions.

        if the fields which has collection of item, the fields can be unwound.
        it means to duplicate the row after split array into each item.

        *join : will indicate the some model joined.
    '''

    if get_type_for_table(type_) != type_:
        _logger.fatal(f'cannot make query for BaseClassTableMixin {type_=}. use base class which is marked with BaseClassTableMixin')
        raise RuntimeError(L('cannot make query for BaseClassTableMixin. check {0}', type_))

    fields = convert_tuple(fields)

    unwind = convert_tuple(unwind)
    order_by = convert_tuple(order_by)

    where = _fill_empty_fields_for_match(
        where, get_stored_fields_for_full_text_search(type_))

    ns_types = dict(
        _build_namespace_types(
            type_, ns_types or {},
            _build_namespace_set(_merge_fields_and_where_fields(fields, where))
        )
    )

    return (
        _get_sql_for_reading(
            tuple(ns_types.items()), 
            fields, 
            tuple((f, o) for f, (o, _) in where.items()), 
            order_by, offset, limit, 
            unwind, 
            cast(Type, base_type), for_count, bool(current)), 
        _build_database_args(where, set_id) | {'version':version, 'current_date':current}
    )

    # first, we make a group for each table.
    # name 
    # person.name  
    # person.age
    # 
    # {'':['name'] 'person': ['name', 'age']}


# check fields and where for requiring the join.
def _merge_fields_and_where_fields(fields:Tuple[str,...], where:NormalizedQueryConditionType) -> Tuple[str, ...]:
    return tuple(itertools.chain(fields, where))


def _fill_empty_fields_for_match(where:NormalizedQueryConditionType, 
                                 fields: Iterable[str]) -> NormalizedQueryConditionType:
    fields_expr = ','.join(fields)

    return {
        (fields_expr if not field and value[0] == 'match' else field):value  
        for field, value in where.items()
    }


@functools.lru_cache()
def _get_sql_for_reading(ns_types:Tuple[Tuple[str, Type]], 
                         fields: Tuple[str, ...], 
                         field_ops: FieldOp, 
                         order_by: Tuple[str, ...],
                         offset: int | None,
                         limit: int | None,
                         unwind: Tuple[str, ...],
                         base_type: Type[PersistentModelT] | None,
                         for_count: bool,
                         has_current_date) -> str:

    # we will build the core table which has _row_id and fields which
    # is referenced from where clause or unwind.
    # we will join the core tables for making a base table which
    # contains the row_id of each table

    # we expect that the where condition will be applied on the table
    # joined on core tables. (base table)
    # but, To apply the where condition on core table makes better performance.

    # We extract the where conditions which be applied on core table.
    # if the where condition does not have 'IS NULL' condition,
    # and apply where condition on core table and make base_type as None
    # for inner join.

    core_query_and_fields : Dict[str, Tuple[str, Tuple[str,...]]] = {}
    field_ops_list = list(field_ops)

    base_table_ns = _get_main_table_namespace(base_type, ns_types)

    join_keys = _find_join_keys(ns_types)

    for ns, ns_type in ns_types:
        field_ops, core_fields = _extract_fields_and_ops_for_core(
            field_ops_list, ns)

        core_fields += _extract_fields_for_join(join_keys, ns_type)
        core_fields += _extract_fields_for_order_by(order_by, ns)

        if field_ops:
            base_table_ns = None

        core_query_and_fields[ns] = _build_query_and_fields_for_core_table(
            ns, ns_type, core_fields,
            field_ops, _extract_fields(unwind, ns), has_current_date)

    field_ops = tuple(field_ops_list)

    if for_count:
        query_for_base, _ = _build_query_for_base_table(
            ns_types, core_query_and_fields, field_ops,
            tuple(), None, None,
            base_table_ns
        )

        return _count_row_query(query_for_base)
    else:
        query_for_base, base_fields = _build_query_for_base_table(
            ns_types, core_query_and_fields, field_ops,
            order_by, offset, limit,
            base_table_ns
        )

        return _get_populated_table_query_from_base(
            query_for_base, base_fields, fields, ns_types)


@functools.cache
def _split_namespace(field:str) -> Tuple[str, str]:
    if '.' in field:
        index = field.rindex('.') 
        return (field[:index], field[index+1:])
    
    return ('', field)


def _extract_fields_and_ops_for_core(
        field_ops:List[Tuple[str, str]], 
        ns: str) -> Tuple[Tuple[Tuple[str, str], ...], List[str]]:

    ns_field_ops = [fo for fo in field_ops if _split_namespace(fo[0])[0] == ns]

    fields = [_ROW_ID_FIELD]

    # update join fields
        # we will join base table with each type's table by _ROW_ID _FIELD.
    if any(fo[1].lower() == 'is null' for fo in ns_field_ops):
        fields.extend(_split_namespace(f)[1] for f, o in ns_field_ops)
        return tuple(), fields

    for fo in ns_field_ops:
        field_ops.remove(fo)

    return tuple((_split_namespace(f)[1], o) for f,o in ns_field_ops), fields 


def _extract_fields_for_order_by(order_by:Tuple[str], ns:str) -> List[str]:
    fields = []

    for f in order_by:
        f = f.lower().replace('desc', '').replace('asc', '').strip()

        field_ns, field = _split_namespace(f)

        if field_ns == ns:
            fields.append(field)

    return fields


def _extract_fields_for_join(
    join_keys:Dict[Tuple[Type, Type], Tuple[str, str]], 
    ns_type:Type) -> List[str]:
    fields = []

    for (tl, tr), (kl, kr) in join_keys.items():
        if tl is ns_type:
            found = kl
        else:
            found = None

        if tr is ns_type:
            found = kr

        if found and found not in fields:
            fields.append(found)

    return fields


def _extract_fields(items:Iterable[str], ns:str) -> Tuple[str,...]:
    if ns == '':
        return tuple(item for item in items if '.' not in item)
    else:
        removed = ns + '.'
        started = len(removed)

        return tuple(item[started:] for item in items 
            if item.startswith(removed) and '.' not in item[started:])


def get_query_and_args_for_purging(type_:Type, where:NormalizedQueryConditionType, set_id:int):
    # TODO delete parts and externals.
    query_args = _build_database_args(where, set_id)

    field_and_op = tuple((f, o) for f, (o, _) in where.items()) + ((_SET_ID_FIELD, '='),)

    return _get_sql_for_purging(type_, field_and_op), query_args


@functools.lru_cache
def _get_sql_for_purging(type_:Type, field_and_value:FieldOp):
    table_name = get_table_name(type_)
    id_fields = get_identifying_fields(type_)

    return join_line(
        f"DELETE FROM {table_name}",
        _build_where(field_and_value),
        f"RETURNING",
        tab_each_line(
            _ROW_ID_FIELD, 
            _SET_ID_FIELD, 
            f"'PURGED' as {field_exprs('op')}", 
            f"'{table_name}' as {field_exprs('table_name')}",
            f"""CONCAT_WS(',', {
                join_line(field_exprs(id_fields), use_comma=True, new_line=False)
                if id_fields else 
                "''"
            }) as {field_exprs(_AUDIT_MODEL_ID_FIELD)}""",
            f"NULL as {field_exprs(_AUDIT_DATA_VERSION_FIELD)}",
            use_comma=True
        )
    )


def get_query_and_args_for_deleting(type_:Type, where:NormalizedQueryConditionType, set_id:int):
    # TODO delete parts and externals.
    query_args = _build_database_args(where, set_id)

    field_and_op = tuple((f, o) for f, (o, _) in where.items()) + ((_SET_ID_FIELD, '='),)

    return _get_sql_for_deleting(type_, field_and_op), query_args


@functools.lru_cache
def _get_sql_for_checking_model_set_can_be_deleted(tp:Type, set_id:int):
    id_fields = get_identifying_fields(tp)
    table_name = get_table_name(tp)

    # check all deleted model was copied in other set.
    return join_line(
        f"SELECT",
        tab_each_line(
            field_exprs(id_fields),
        ),
        f"FROM",
        _alias_table_or_query(
            join_line(
                f"SELECT",
                tab_each_line(
                    field_exprs(id_fields),
                    use_comma=True
                ),
                f'FROM {table_name}',
                _build_where(
                    (
                        (f'{field_exprs(_SET_ID_FIELD)}', '=', '%(set_id)s'),
                        (f'{field_exprs(_VALID_START_FIELD)}', '<=', '@VERSION'),
                        (f'{field_exprs(_VALID_END_FIELD)}', '>', '@VERSION')
                    )
                )
            ), "SRC"),
            f"LEFT JOIN",
            _alias_table_or_query(join_line(
                f"SELECT",
                tab_each_line(
                    field_exprs(id_fields),
                    field_exprs(_SET_ID_FIELD),
                    use_comma=True
                ),
                f'FROM {table_name}',
                _build_where(
                    (
                        (f'{field_exprs(_SET_ID_FIELD)}', '!=', '%(set_id)s'),
                        (f'{field_exprs(_VALID_START_FIELD)}', '<=', '@VERSION'),
                        (f'{field_exprs(_VALID_END_FIELD)}', '>', '@VERSION')
                    )
                )
            ), "DEST"
        ),
        f"USING ({join_line(field_exprs(id_fields), use_comma=True, new_line=False)})",
        f"WHERE {field_exprs(_SET_ID_FIELD)} IS NULL"
    )


def get_query_and_args_for_deleting_model_set(table_name:str, set_id:int):
    return _get_sql_for_deleting_model_set(table_name), {'set_id':set_id}

@functools.lru_cache
def _get_sql_for_deleting_model_set(table_name:str):
    return join_line(
        f"UPDATE {table_name}",
        f"SET",
        tab_each_line(
            f'{field_exprs(_VALID_END_FIELD)} = @VERSION',
        ),
        f"WHERE",
        tab_each_line(
            f'{field_exprs(_SET_ID_FIELD)} = %(set_id)s',
            f'AND {field_exprs(_VALID_START_FIELD)} <= @VERSION',
            f'AND {field_exprs(_VALID_END_FIELD)} > @VERSION',
        ),
        ";",
        "SELECT",
        tab_each_line(
            _ROW_ID_FIELD, 
            _SET_ID_FIELD, 
            f"'DELETED:DELETE_MODEL_SET' as {field_exprs('op')}", 
            f"'{table_name}' as {field_exprs('table_name')}",
            f"""'' as {field_exprs(_AUDIT_MODEL_ID_FIELD)}""",
            f"NULL as {field_exprs(_AUDIT_DATA_VERSION_FIELD)}",
            use_comma=True
        ),
        f"FROM {table_name}",
        f"WHERE",
        tab_each_line(
            f'{field_exprs(_SET_ID_FIELD)} = %(set_id)s',
            f'AND {field_exprs(_VALID_END_FIELD)} = @VERSION',
        )
    )

def get_query_and_args_for_purging_model_set(table_name:str, set_id:int):
    return _get_sql_for_purging_model_set(table_name), {'set_id':set_id}

@functools.lru_cache
def _get_sql_for_purging_model_set(table_name:str):
    return join_line(
        f"DELETE FROM {table_name}",
        f"WHERE",
        tab_each_line(
            f'{field_exprs(_SET_ID_FIELD)} = %(set_id)s',
        ),
        "RETURNING",
        tab_each_line(
            _ROW_ID_FIELD, 
            _SET_ID_FIELD, 
            f"'PURGED:PURGE_MODEL_SET' as {field_exprs('op')}", 
            f"'{table_name}' as {field_exprs('table_name')}",
            f"""'' as {field_exprs(_AUDIT_MODEL_ID_FIELD)}""",
            f"NULL as {field_exprs(_AUDIT_DATA_VERSION_FIELD)}",
            use_comma=True
        )
    )


@functools.lru_cache
def _get_sql_for_deleting(type_:Type, field_and_value:FieldOp):
    table_name = get_table_name(type_)
    id_fields = get_identifying_fields(type_)

    return join_line(
        f"UPDATE {table_name}",
        f"SET",
        tab_each_line(
            f'{field_exprs(_VALID_END_FIELD)} = @VERSION',
        ),
        _build_where(field_and_value, ((_VALID_END_FIELD, '=', f'{_BIG_INT_MAX}'),)),
        ";",
        "SELECT",
        tab_each_line(
            _ROW_ID_FIELD, 
            _SET_ID_FIELD, 
            f"'DELETED' as {field_exprs('op')}", 
            f"'{table_name}' as {field_exprs('table_name')}",
            f"""CONCAT_WS(',', {
                    join_line(field_exprs(id_fields), use_comma=True, new_line=False)
                    if id_fields else 
                    "''"
            }) as {field_exprs(_AUDIT_MODEL_ID_FIELD)}""",
            f"NULL as {field_exprs(_AUDIT_DATA_VERSION_FIELD)}",
            use_comma=True
        ),
        f"FROM {table_name}",
        _build_where(field_and_value, ((_VALID_END_FIELD, '=', '@VERSION'),)),
    )


def _build_database_args(where:NormalizedQueryConditionType, set_id:int | None = None) -> Dict[str, Any]:
    return {
        _get_parameter_variable_for_multiple_fields(field):value for field, (_, value) in where.items()
    } | ({} if set_id is None else {'__set_id': set_id} )


def _build_where(*items:Tuple[Tuple[str, str, str] | Tuple[str, str],...], ns:str = '') -> str:
    
    where_ops = [_build_where_op(item, ns) for item in itertools.chain(*items)]

    if where_ops:
        return join_line(
            'WHERE',
            tab_each_line(
                '\nAND '.join(where_ops),
            )
        )

    return ''


def _build_limit_and_offset(limit:int| None, offset:int|None) -> str:
    query = f'LIMIT {limit}' if limit else ''
    if query:
        query += ' ' 
    query +=f'OFFSET {offset}' if offset else ''

    return query


def _build_order_by(order_by:Tuple[str,...]) -> str:
    assert order_by
    processed = [
        ' '.join(field_exprs(item) if index == 0 else item for index, item in enumerate(line.split(' '))) 
        for line in order_by 
    ]

    return join_line(
        'ORDER BY',
        tab_each_line(
            processed, use_comma=True
        )
    )

def _drop_table_prefix(item:str)->str:
    if '.' in item:
       return item.split('.')[1].strip('`')

    return item


def _build_where_op(fields_op:Tuple[str, str] | Tuple[str, str, str], ns:str = '') -> str:
    ns  = ns + '.' if ns else ''

    field = fields_op[0]
    op = fields_op[1].lower()

    if field.lower() in [_SET_ID_FIELD, 'version']:
        ns = ''

    if len(fields_op) == 3:
        variable = fields_op[2]
    else:
        variable = f'%({_normalize_database_object_name(ns + _drop_table_prefix(fields_op[0]))})s'

    assert op != 'match'

    if op == '':
        return field_exprs(field)
    if op == 'is null':
        return f'{field_exprs(field)} {op}'
    #if op == 'match':
    #    return _build_match(fields_op[0], variable, variable)
    else:
        return f'{field_exprs(field)} {op} {variable}'


def _build_match(fields:str, variable:str, table_name:str = ''):
    variable = _get_parameter_variable_for_multiple_fields(variable)
    field_items = field_exprs([f for f in fields.split(',')], table_name)

    return (f'MATCH ({join_line(field_items, new_line=False, use_comma=True)}) '
            f'AGAINST (%({variable})s IN BOOLEAN MODE)')


def _get_parameter_variable_for_multiple_fields(fields:str):
    variable = _normalize_database_object_name(fields)
    return variable


def _build_query_and_fields_for_core_table(
        ns:str, 
        target_type: Type[PersistentModelT],
        fields: List[str],
        field_ops: Tuple[Tuple[str, str], ...],
        unwind: Tuple[str, ...],
        has_current_date: bool) -> Tuple[str, Tuple[str, ...]]:

    # in core table, following item will be handled.
    # 1. unwind
    # 2. where : 
    #  2-1) VersionMixin, first we should apply version for getting correct applied_end
    #  2-2) DatedMixin 
    # 3. match op
    # 4. fields which is looked up from base table by field_ops.

    matches = []
    prefix_fields : DefaultDict[str, Dict[str, None]] = defaultdict(dict)

    prefix_fields['__ORG'][_ROW_ID_FIELD] = None

    for f in itertools.chain(fields, unwind):
        prefix_fields[_get_alias_for_unwind(f, unwind)][f] = None

    for f, op in field_ops:
        if op == 'match':
            matches.append(_build_match(f, f, '__ORG'))
        else:
            prefix_fields[_get_alias_for_unwind(f, unwind)][f] = None

    field_op_var = tuple(
        (field_exprs(f, _get_alias_for_unwind(f, unwind)), o)
        for f, o in field_ops if o != 'match'
    )

    origin = _alias_table_or_query(get_table_name(target_type), '__ORG')

    # if version
    if _is_dated_type(target_type):
        if has_current_date:
            field_op_var = field_op_var + (
                (field_exprs(_APPLIED_START_FIELD, '__ORG'), '<=', '%(current_date)s'),
                (field_exprs(_APPLIED_END_FIELD, '__ORG'), '>', '%(current_date)s')
            )

        id_fields = [
            f for f in get_identifying_fields(target_type) 
            if f != _APPLIED_AT_FIELD
        ]

        org_field_op_var: Tuple[Tuple[str, str, str],...]= (
            (field_exprs(_SET_ID_FIELD), '=', '%(__set_id)s'),
        )

        # we should apply the version at first. then generate applied_end.
        if _is_version_type(get_root_container_type(target_type) or target_type):
            org_field_op_var += (
                (field_exprs(_VALID_START_FIELD), '<=', '%(version)s'),
                (field_exprs(_VALID_END_FIELD), '>', '%(version)s'),
            )
 
        origin = _alias_table_or_query(join_line(
            "SELECT",
            tab_each_line(
                [
                    '*',
                    f"{field_exprs(_APPLIED_AT_FIELD)} as {field_exprs(_APPLIED_START_FIELD)}",
                    (
                        f"IFNULL(LEAD({field_exprs(_APPLIED_AT_FIELD)}) over "
                        f"(PARTITION BY {join_line(field_exprs(id_fields), new_line=False)} "
                        f"ORDER BY {field_exprs(_APPLIED_AT_FIELD)}), '9999-12-31')"
                        f" as {field_exprs(_APPLIED_END_FIELD)}"
                    )
                ],
                use_comma=True
            ),
            "FROM",
            get_table_name(target_type),
            _build_where(org_field_op_var, ns=ns)
        ), '__ORG')
    else:
        field_op_var = field_op_var + (
            (field_exprs(_SET_ID_FIELD, '__ORG'), '=', '%(__set_id)s'),
        )

        if not is_derived_from(target_type, PersistentSharedContentModel):
            field_op_var = field_op_var + (
                (field_exprs(_VALID_START_FIELD, '__ORG'), '<=', '%(version)s'),
                (field_exprs(_VALID_END_FIELD, '__ORG'), '>', '%(version)s')
            )
         
    field_list = [] 

    for prefix, sub_fields in prefix_fields.items():
        field_list.extend(as_field_expr(f, prefix, ns) for f in sub_fields)

    if matches:
        field_list.append(
            ' + '.join(matches) + f" as {field_exprs(_add_namespace(_RELEVANCE_FIELD, ns))}"
        )
        prefix_fields['__ORG'][_RELEVANCE_FIELD] = None

    # handle unwind record.
    source = '\nLEFT JOIN '.join(
        itertools.chain(
            [ origin ],
            [
                _alias_table_or_query(get_table_name(target_type, f), _get_alias_for_unwind(f, unwind)) 
                + f' ON {field_exprs(_ROW_ID_FIELD, "__ORG")} = '
                + f'{field_exprs(_ORG_ROW_ID_FIELD, _get_alias_for_unwind(f, unwind))}'
                for f in unwind
            ]
        )
    )

    return join_line(
        f"SELECT",
        tab_each_line(
            field_list,
            use_comma=True
        ),
        f"FROM",
        tab_each_line(
            source
        ),
        _build_where(field_op_var, ns=ns)
    ), tuple(_add_namespace(f, ns) for f in itertools.chain(*prefix_fields.values()))


def _get_alias_for_unwind(f:str, unwind:Tuple[str], other:str = '__ORG') -> str:
    if f in unwind:
        f = _normalize_database_object_name(f)
        return f'__UNWIND_{f}'
    else:
        return other


def _build_query_for_base_table(ns_types: Tuple[Tuple[str, PersistentModelT],...],
                                core_table_queries: Dict[str, Tuple[str, Tuple[str,...]]],
                                field_ops: Tuple[Tuple[str, str]],
                                order_by: Tuple[str, ...],
                                offset: int | None,
                                limit: int | None,
                                base_table_ns: str | None):

    joined, fields = _build_join_for_ns(ns_types, core_table_queries, base_table_ns) 

    nested_where : Tuple[Tuple[str, str]] = tuple()

    if _RELEVANCE_FIELD in tuple(_split_namespace(f)[1] for f in fields):
        nested_where = ((_RELEVANCE_FIELD, ''), )
        order_by += order_by + (f"{_RELEVANCE_FIELD} DESC" ,)

    if order_by or nested_where:
        return join_line(
            'SELECT',
            tab_each_line(
                field_exprs(fields),
                use_comma=True
            ),
            'FROM',
            _alias_table_or_query(
                join_line(
                    'SELECT',
                    tab_each_line(
                        field_exprs(_merge_relevance_fields(fields)),
                        use_comma=True
                    ),
                    'FROM',
                    tab_each_line(
                        joined
                    )
                ), "FOR_ORDERING"
            ),
            _build_where(field_ops, nested_where),
            _build_order_by(order_by),
            _build_limit_and_offset(limit or _BIG_INT_MAX, offset)
        ), fields

    return join_line(
        'SELECT',
        tab_each_line(
            field_exprs(_merge_relevance_fields(fields)),
            use_comma=True
        ),
        'FROM',
        tab_each_line(
            joined
        ),
        _build_where(field_ops),
        _build_limit_and_offset(limit, offset)
    ), fields


def _merge_relevance_fields(fields:Iterable[str]) -> List[str]:
    merged = []
    relevance_fields = []

    for f in fields:
        _, field = _split_namespace(f)

        if field == _RELEVANCE_FIELD:
            relevance_fields.append(f)
        else:
            merged.append(f)

    if relevance_fields:
        merged.append(
            ' + '.join(field_exprs(relevance_fields)) + f' AS {field_exprs(_RELEVANCE_FIELD)}'
        )

    return merged


def _build_join_for_ns(ns_types: Tuple[Tuple[str, PersistentModelT],...], 
                       core_table_queries: Dict[str, Tuple[str, Tuple[str,...]]],
                       main_table_ns: str | None) -> Tuple[str, List[str]]:
    join_dict = dict(ns_types)
    join_keys = _find_join_keys(ns_types)

    joined_queries = []
    total_fields = []

    join_scope = ''
    prev_ns = None

    for current_ns in sorted(core_table_queries):
        query, fields = core_table_queries[current_ns]
        current_type = join_dict[current_ns]

        total_fields.extend(fields)

        if current_ns == main_table_ns:
            join_scope = 'RIGHT '
        elif prev_ns is not None and prev_ns == main_table_ns:
            join_scope = 'LEFT '

        if current_ns:
            base_ns, _ = _split_namespace(current_ns)
            base_type = join_dict[base_ns]

            keys = join_keys[(base_type, current_type)]

            left_key = _add_namespace(keys[0], base_ns)
            right_key = _add_namespace(keys[1], current_ns)
            joined_queries.append(f'{join_scope}JOIN {_alias_table_or_query(query, f"__{current_ns.upper()}")}'
                f' ON {field_exprs(left_key)} = {field_exprs(right_key)}')
        else:
            joined_queries.append(_alias_table_or_query(query, '_BASE_CORE'))

        prev_ns = current_ns

    return join_line(joined_queries), total_fields


def _count_row_query(query:str) -> str:
    return join_line(
        'SELECT',
        '  COUNT(*) AS COUNT',
        'FROM',
        _alias_table_or_query(query, 'FOR_COUNT')
    )
    

def _get_main_table_namespace(base_type:Type, ns_types:Tuple[Tuple[str, Type]]) -> str | None:
    for prefix, type_ in ns_types:
        if type_ == base_type:
            return prefix

    return None 


def _build_namespace_set(fields:Iterable[str]) -> Set[str]:
    field_tree = defaultdict(list)

    for f in fields:
        ns, field = _split_namespace(f)
        field_tree[ns].append(field)

    return set(field_tree)


def _add_namespace(field:str, ns:str) -> str:
    if ns:
        return f'{ns}.{field}'

    return field


def _build_namespace_types(base_type:Type, join:Dict[str, Type], 
                           namespaces: Set[str]) -> Iterator[Tuple[str, Type]]:
    yield ('', base_type)

    yield from _build_join_from_refs(base_type, '', namespaces)

    for field_name, field_type in join.items():
        if any(field_name in ns for ns in namespaces):
            yield field_name, field_type
            yield from _build_join_from_refs(field_type, field_name + '.', namespaces)


def _build_join_from_refs(current_type:Type, current_ns:str, 
                          namespaces: Set[str]) -> Iterator[Tuple[str, Type]]:
    refs = get_stored_fields_for(current_type, MetaReferenceField)

    for field_name, (_, field_type) in refs.items():
        if any(field_name in ns for ns in namespaces):
            metadata = get_metadata_for(field_type, MetaReferenceField)

            if metadata:
                yield current_ns + field_name, metadata.target_type
                yield from _build_join_from_refs(metadata.target_type, current_ns + field_name + '.', namespaces)


def _find_join_keys(join:Tuple[Tuple[str, Type],...]
                    ) -> Dict[Tuple[Type, Type], Tuple[str, str]]:
    keys : Dict[Tuple[Type, Type], Tuple[str, str]] = {}

    targets = dict(join)

    for ns, joined_type in join:
        if not ns:
            continue

        ref_ns, _ = _split_namespace(ns)

        base_type = targets[ref_ns]
        fields = _find_join_key(base_type, joined_type) or _find_join_key(joined_type, base_type, True)

        if fields is None:
            _logger.fatal(
                f'{base_type=}, {joined_type=} does not have reference.\n'
                f'{get_stored_fields(base_type)=} {get_stored_fields(joined_type)=}')
            raise RuntimeError(L(f'{0} or {1} does not have reference which links both.', base_type, joined_type))

        keys[(base_type, joined_type)] = (fields[0], fields[1])

    return keys


def _find_join_key(base_type:Type, target_type:Type, reversed:bool = False) -> Tuple[str, str] | None:
    refs = get_stored_fields_for(base_type, MetaReferenceField)

    for field_name, (_, field_type) in refs.items():
        ref_field = get_metadata_for(field_type, MetaReferenceField)

        if ref_field and ref_field.target_type == target_type:
            target_field = ref_field.target_field

            if reversed:
                return (target_field,  field_name)
            else:
                return (field_name, target_field)

    return None


def _get_table_name_of(ns:str) -> str:
    if ns:
        ns = _normalize_database_object_name(ns)
        return f'_{ns}'
    else:
        return f'_MAIN'


_BASE_TABLE_NAME = '_BASE'

def _get_populated_table_query_from_base(query_for_base:str, 
                                         base_fields: List[str],
                                         target_fields: Tuple[str, ...],
                                         ns_types: Tuple[Tuple[str, Type[PersistentModelT]]]):

    joined_tables = [
        _alias_table_or_query(query_for_base, _BASE_TABLE_NAME)
    ]

    scope_fields = {}

    fields = [f for f in target_fields if f not in base_fields]

    for ns, ns_type in ns_types:
        ns_fields = _extract_fields(fields, ns)

        if not ns_fields:
            continue

        ns_table_name = _get_table_name_of(ns)

        if ns_fields:
            scope_fields[ns] = ns_fields
            joined_tables.append(
                f'JOIN {_alias_table_or_query(get_table_name(ns_type), ns_table_name)} ON '
                f'{field_exprs(_add_namespace(_ROW_ID_FIELD, ns), _BASE_TABLE_NAME)} = '
                f'{field_exprs(_ROW_ID_FIELD, ns_table_name)}')

    return join_line(
        "SELECT",
        tab_each_line(
            field_exprs([f for f in base_fields if f in target_fields], _BASE_TABLE_NAME),
            *(
                tuple(as_field_expr(f, _get_table_name_of(ns), ns) for f in ns_fields) 
                for ns, ns_fields in scope_fields.items()
            ),
            use_comma=True
        ),
        "FROM", 
        tab_each_line(
            joined_tables
        )
    )


def _normalize_database_object_name(value:str) -> str:
    normalized = value.replace('.', '_').replace(',', '_').replace(' ', '')

    return normalized


def _is_version_type(type_:Type) -> bool:
    return is_derived_from(type_, VersionMixin)


def _is_dated_type(type_:Type) -> bool:
    return is_derived_from(type_, DatedMixin)