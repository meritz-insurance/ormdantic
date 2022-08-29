from typing import (
    Type, Iterator, overload, Iterable, List, Tuple, cast, Dict, 
    Any, DefaultDict, Optional, get_args, Set
)
from datetime import datetime, date
from decimal import Decimal
from collections import defaultdict
import itertools
import functools

from pydantic import ConstrainedStr, ConstrainedDecimal

from ormdantic.util import is_derived_from, convert_tuple
from ormdantic.util.hints import get_base_generic_alias_of

from ..util import get_logger
from ..schema.base import (
    ArrayIndexMixin, PersistentModelT, ReferenceMixin, FullTextSearchedMixin, 
    IdentifyingMixin, IndexMixin, SequenceIdStr, 
    StoredFieldDefinitions, PersistentModelT, StoredMixin, PartOfMixin, 
    UniqueIndexMixin, get_container_type, 
    get_field_names_for, get_part_types, is_field_list_or_tuple_of,
    PersistentModel, is_list_or_tuple_of, get_stored_fields,
    get_stored_fields_for

)

_MAX_VAR_CHAR_LENGTH = 200
_MAX_DECIMAL_DIGITS = 65

_JSON_TYPE = 'LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin'
_JSON_CHECK = 'CHECK (JSON_VALID({}))'

_PART_BASE_TABLE = 'pbase'

# field name for internal use.
_ROW_ID_FIELD = '__row_id'
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


Where = Tuple[Tuple[str, str, Any], ...]
FieldOp = Tuple[Tuple[str, str]]

_logger = get_logger(__name__)

def get_table_name(type_:Type[PersistentModelT], postfix:str = ''):
    return f'{_MODEL_TABLE_PREFIX}_{type_.__name__}{"_" + postfix if postfix else ""}' 


def get_seq_name(type_:Type[PersistentModelT], postfix:str):
    return f'{_SEQ_PREFIX}_{type_.__name__}{"_" + postfix if postfix else ""}' 

def get_seq_func_name(type_:Type[PersistentModelT], postfix:str):
    return f'{_FUNC_PREFIX}_{type_.__name__}{"_" + postfix if postfix else ""}' 


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


def _alias_table(old_table:str, table_name:str) -> str:
    old_table = old_table.strip()

    #if old_table.startswith('(') and old_table.endswith(')'):
    #    old_table = old_table[1:-1]

    if ' ' in old_table:
        return join_line(
            f"(",
            tab_each_line(
                old_table
            ),
            f")",
            f"AS {_normalize_database_object_name(table_name)}"
        )

    return f"{old_table} AS {_normalize_database_object_name(table_name)}"



def tab_each_line(*statements:Iterable[str] | str, use_comma: bool = False) -> str:
    line = join_line(*statements, use_comma=use_comma, new_line=True)

    return join_line(['  ' + item for item in line.split('\n')], use_comma=False, new_line=True)


def get_sql_for_creating_table(type_:Type[PersistentModelT]):
    stored = get_stored_fields(type_)

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
            _get_table_indexes(part_stored),
        )

        part_table_name = get_table_name(type_, _PART_BASE_TABLE)

        container_type = get_container_type(type_)
        assert container_type 

        container_table_name = get_table_name(container_type)

        part_fields = (_ROW_ID_FIELD, _ROOT_ROW_ID_FIELD, _CONTAINER_ROW_ID_FIELD, 
                       *part_stored.keys())
        container_fields = tuple(set(stored.keys()) - set(part_fields))

        joined_table = _build_join_table(
            (part_table_name, _CONTAINER_ROW_ID_FIELD), 
            (container_table_name, _ROW_ID_FIELD)
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
        yield _build_model_table_statement(
            get_table_name(type_), 
            _get_table_fields(),
            _get_table_stored_fields(stored, True),
            _get_table_indexes(stored)
        )

        yield from _build_code_seq_statement(type_)

    yield from _build_statement_for_external_index_tables(type_)


def get_stored_fields_for_full_text_search(type_:Type[PersistentModelT]):
    return get_stored_fields_for(type_, FullTextSearchedMixin)


def get_stored_fields_for_part_of(type_:Type[PersistentModelT]):
    return get_stored_fields_for(type_, 
        lambda paths, type_: 
            not _is_come_from_container_field(paths) 
            or is_derived_from(type_, FullTextSearchedMixin)
            or is_list_or_tuple_of(type_, FullTextSearchedMixin)
    )


def _is_come_from_container_field(paths:Tuple[str,...]) -> bool:
    return bool(len(paths) >= 2 and paths[0] == '..' and not paths[1].startswith('$'))


def get_stored_fields_for_external_index(type_:Type[PersistentModelT]):
    return get_stored_fields_for(type_, ArrayIndexMixin)


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

        if is_derived_from(field_element_type, SequenceIdStr):
            prefix = field_element_type.prefix

            yield join_line(
                f'CREATE SEQUENCE {get_seq_name(type_, name)} START WITH 1 INCREMENT 1'
            )
            yield join_line(
                f'CREATE FUNCTION {get_seq_func_name(type_, name)}() RETURNS VARCHAR(16)',
                'BEGIN',
                tab_each_line(
                    f"SELECT CONCAT('{prefix}', NEXTVAL({get_seq_name(type_, name)})) INTO @R;"
                    f'RETURN @R;'
                ),
                'END'
            )


def get_query_for_next_seq(type_:Type, field:str) -> str:
    return f'SELECT {get_seq_func_name(type_, field)}() as NEXT_SEQ'


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
    yield f'{field_exprs(_JSON_FIELD)} {_JSON_TYPE} {_JSON_CHECK.format(field_exprs(_JSON_FIELD))}'


def _get_external_index_table_fields(field_name:str, field_type:Type) -> Iterator[str]:
    # we don't need primary key, because there is no field which is for full text searching.
    type = get_base_generic_alias_of(field_type, ArrayIndexMixin)

    param_type = get_args(type)[0]

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
        stored = '' if not generated else _generate_stored_for_json_path(paths, field_type)
        yield f"""{field_exprs(field_name)} {_get_field_db_type(field_type)}{stored}"""


def _get_view_fields(fields:Dict[str, Tuple[str, ...]]) -> Iterator[str]:
    for table_name, field_names in fields.items():
        for field_name in field_names:
            yield f'{field_exprs(field_name, table_name)}'


def _get_table_indexes(stored_fields:StoredFieldDefinitions) -> Iterator[str]:
    for field_name, (_, field_type) in stored_fields.items():
        key_def = _generate_key_definition(field_type)

        if key_def: 
            yield f"""{key_def} `{field_name}_index` ({field_exprs(field_name)})"""

    full_text_searched_fields = set(
        field_name for field_name, (_, field_type) in stored_fields.items()
        if is_derived_from(field_type, FullTextSearchedMixin)
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

    if issubclass(type_, Decimal):
        if issubclass(type_, ConstrainedDecimal):
            max_digits = min(type_.max_digits or _MAX_DECIMAL_DIGITS, _MAX_DECIMAL_DIGITS)

            return f'DECIMAL({max_digits})'

        return 'DECIMAL(65)'

    if issubclass(type_, datetime):
        return 'DATETIME(6)'

    if issubclass(type_, date):
        return 'DATE'

    if issubclass(type_, (str, StoredMixin)):
        if issubclass(type_, ConstrainedStr):
            max_length = min(type_.max_length or _MAX_VAR_CHAR_LENGTH, _MAX_VAR_CHAR_LENGTH)

            return f'VARCHAR({max_length})'

        return 'TEXT'

    _logger.fatal(f'unsupported type: {type_}')
    raise RuntimeError(f'{type_} is not the supported type in database.')


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
    if is_derived_from(field_type, IdentifyingMixin):
        # this value will be update by sql params.
        return ''

    json_path = '$.' + '.'.join([path.replace('$.', '') for path in json_paths if path != '$'])

    if is_derived_from(field_type, ArrayIndexMixin):
        return f" AS (JSON_EXTRACT(`{_JSON_FIELD}`, '{json_path}')) STORED"
    else:
        return f" AS (JSON_VALUE(`{_JSON_FIELD}`, '{json_path}')) STORED"


def _generate_key_definition(type_:Type) -> str:
    if issubclass(type_, (UniqueIndexMixin, IdentifyingMixin)):
        return 'UNIQUE KEY'
    elif issubclass(type_, IndexMixin):
        return 'KEY'

    return ''


def get_query_and_args_for_upserting(model:PersistentModel):
    query_args : Dict[str, Any] = {}

    for f in _get_identifying_fields(type(model)):
        query_args[f] = getattr(model, f) 

    query_args[_JSON_FIELD] = model.json()

    return _get_sql_for_upserting(cast(Type, type(model))), query_args


def _get_identifying_fields(model_type:Type[PersistentModelT]) -> Tuple[str]:
    stored_fields = get_stored_fields_for(model_type, IdentifyingMixin)

    return tuple(field_name for field_name, _ in stored_fields.items())
            

@functools.lru_cache
def _get_sql_for_upserting(model_type:Type):
    fields = _get_identifying_fields(model_type)

    return join_line(
        f'INSERT INTO {get_table_name(model_type)} (',
        tab_each_line(
            field_exprs([_JSON_FIELD, *fields]),
            use_comma=True
        ),
        ')',
        f'VALUES (',
        tab_each_line(
            '%(__json)s',
            [f'%({f})s' for f in fields],
            use_comma=True
        ),
        ')',
        f'ON DUPLICATE KEY UPDATE',
        tab_each_line(
            f'{field_exprs(_JSON_FIELD)} = %(__json)s'
        ),
        f'RETURNING',
        tab_each_line(
            _ROW_ID_FIELD,
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
            f'WHERE {field_exprs(_ROOT_ROW_ID_FIELD)} = %(__root_row_id)s'
        ])

        sqls.append(delete_sql)

        fields = get_stored_fields_for_part_of(part_type)

        target_fields = tuple(itertools.chain(
            [_ROOT_ROW_ID_FIELD, _CONTAINER_ROW_ID_FIELD, _JSON_PATH_FIELD],
            fields.keys()
        ))

        root_field = 'CONTAINER.' + (field_exprs(_ROW_ID_FIELD) if is_root else field_exprs(_ROOT_ROW_ID_FIELD))

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
                                f"CONCAT('{json_path}[', {field_exprs(_PART_ORDER_FIELD)} - 1, ']') as {field_exprs(_JSON_PATH_FIELD)}"
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
                        f'{root_field} = %(__root_row_id)s'
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
            f'WHERE {field_exprs(_ROOT_ROW_ID_FIELD)} = %(__root_row_id)s'
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
                f"""{field_exprs(_ROOT_ROW_ID_FIELD if is_part else _ROW_ID_FIELD, '__ORG')} = %(__root_row_id)s"""
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
        raise RuntimeError('paths should have 2 items for generating value from container.') 

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


def get_sql_for_deleting_parts(model_type:Type) -> Dict[Type, Tuple[str, ...]]:
    type_sqls : Dict[str, Tuple[str, str]] = {}
    part_types = get_part_types(model_type)

    is_root = not is_derived_from(model_type, PartOfMixin)
    sqls = []
    
    for part_type in part_types:
        part_fields = get_field_names_for(model_type, part_type)

        assert part_fields

        delete_sql = join_line([
            f'DELETE FROM {get_table_name(part_type, _PART_BASE_TABLE)}',
            f'WHERE {field_exprs(_ROOT_ROW_ID_FIELD)} in %(__root_row_ids)s'
        ])

        sqls.append(delete_sql)

        fields = get_stored_fields_for_part_of(part_type)

        type_sqls[part_type] = tuple(sqls)

    return type_sqls


def get_sql_for_deleting_external_index(model_type:Type) -> Iterator[str]:
    is_part = is_derived_from(model_type, PartOfMixin)

    for field_name, (json_paths, field_type) in get_stored_fields_for_external_index(model_type).items():
        table_name = get_table_name(model_type, field_name)

        # delete previous items.
        yield join_line([
            f'DELETE FROM {table_name}',
            f'WHERE {field_exprs(_ROOT_ROW_ID_FIELD)} in %(__root_row_ids)s'
        ])


# fields is the json key , it can contain '.' like product.name
def get_query_and_args_for_reading(type_:Type[PersistentModelT], fields:Tuple[str,...] | str, where:Where, 
                                   *,
                                   order_by: Tuple[str, ...] | str = tuple(), 
                                   offset : None | int = None, 
                                   limit : None | int = None,
                                   unwind: Tuple[str, ...] | str = tuple(),
                                   ns_types:Dict[str, Type[PersistentModelT]] | None = None,
                                   base_type:Optional[Type[PersistentModelT]] = None,
                                   for_count:bool = False
                                   ):
    '''
        Get 'field' values of record which is all satified the where conditions.

        if the fields which has collection of item, the fields can be unwound.
        it means to duplicate the row after split array into each item.

        *join : will indicate the some model joined.
    '''
    fields = convert_tuple(fields)

    unwind = convert_tuple(unwind)
    order_by = convert_tuple(order_by)

    where = _fill_empty_fields_for_match(where, 
                                         get_stored_fields_for_full_text_search(type_))

    ns_types = dict(
        _build_namespace_types(
            type_, ns_types or {},
            _build_namespace_set(
                _merge_fields_and_where_fields(fields, where))
        )
    )

    return (
        _get_sql_for_reading(
            tuple(ns_types.items()), 
            fields, 
            tuple((f, o) for f, o, _ in where), 
            order_by, offset, limit, 
            unwind, 
            cast(Type, base_type), for_count), 
        _build_database_args(where)
    )

    # first, we make a group for each table.
    # name 
    # person.name  
    # person.age
    # 
    # {'':['name'] 'person': ['name', 'age']}


# check fields and where for requiring the join.
def _merge_fields_and_where_fields(fields:Tuple[str,...], where:Where) -> Tuple[str, ...]:
    return tuple(itertools.chain(fields, (w[0] for w in where)))


def _fill_empty_fields_for_match(where:Where, fields:Iterable[str]) -> Where:
    fields_expr = ','.join(fields)

    return tuple(
        (fields_expr, w[1], w[2]) if w[1] == 'match' and not w[0] else w
        for w in where
    )


@functools.lru_cache()
def _get_sql_for_reading(ns_types:Tuple[Tuple[str, Type]], 
                         fields: Tuple[str, ...], 
                         field_ops: FieldOp, 
                         order_by: Tuple[str, ...],
                         offset: int | None,
                         limit: int | None,
                         unwind: Tuple[str, ...],
                         base_type: Type[PersistentModelT] | None,
                         for_count: bool) -> str:

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
            field_ops, _extract_fields(unwind, ns))

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


def get_query_and_args_for_deleting(type_:Type, where:Where):
    # TODO delete parts and externals.
    query_args = _build_database_args(where)

    return _get_sql_for_deleting(type_, tuple((f, o) for f, o, _ in where)), query_args


@functools.lru_cache
def _get_sql_for_deleting(type_:Type, field_and_value:FieldOp):
    return f'DELETE FROM {get_table_name(type_)} {_build_where(field_and_value)} RETURNING {_ROW_ID_FIELD}'


def _build_database_args(where:Where) -> Dict[str, Any]:
    return {_get_parameter_variable_for_multiple_fields(field):value for field, _, value in where}


def _build_where(field_and_ops:FieldOp | Tuple[Tuple[str, str, str]], ns:str = '') -> str:
    if field_and_ops:
        return join_line(
            'WHERE',
            tab_each_line(
                '\nAND '.join(_build_where_op(item, ns) for item in field_and_ops)
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


def _build_where_op(fields_op:Tuple[str, str] | Tuple[str, str, str], ns:str = '') -> str:
    ns  = ns + '.' if ns else ''

    field = fields_op[0]
    op = fields_op[1]
    variable = _normalize_database_object_name(ns + (fields_op[2] if len(fields_op) == 3 else fields_op[0]))

    assert op != 'match'

    if op == '':
        return field_exprs(field)
    if op == 'is null':
        return f'{field_exprs(field)} {op}'
    #if op == 'match':
    #    return _build_match(fields_op[0], variable, variable)
    else:
        return f'{field_exprs(field)} {op} %({variable})s'


def _build_match(fields:str, variable:str, table_name:str = ''):
    variable = _get_parameter_variable_for_multiple_fields(variable)
    field_items = field_exprs([f for f in fields.split(',')], table_name)

    return f'MATCH ({join_line(field_items, new_line=False, use_comma=True)}) AGAINST (%({variable})s IN BOOLEAN MODE)'


def _get_parameter_variable_for_multiple_fields(fields:str):
    variable = _normalize_database_object_name(fields)
    return variable


def _build_query_and_fields_for_core_table(
        ns:str, 
        target_type: Type[PersistentModelT],
        fields: List[str],
        field_ops: Tuple[Tuple[str, str], ...],
        unwind: Tuple[str, ...]) -> Tuple[str, Tuple[str, ...]]:
    # in core table, following item will be handled.
    # 1. unwind
    # 2. where
    # 3. match op
    # 4. fields which is looked up from base table by field_ops.

    matches = []
    prefix_fields : DefaultDict[str, Dict[str, None]]= defaultdict(dict)

    prefix_fields['__ORG'][_ROW_ID_FIELD] = None

    for f in itertools.chain(fields, unwind):
        prefix_fields[_get_alias_for_unwind(f, unwind)][f] = None

    for f, op in field_ops:
        if op == 'match':
            matches.append(_build_match(f, f, '__ORG'))
        else:
            prefix_fields[_get_alias_for_unwind(f, unwind)][f] = None

    field_op_var = tuple(
        (field_exprs(f, _get_alias_for_unwind(f, unwind)), o, f)
        for f, o in field_ops if o != 'match'
    )

    field_list = [] 

    for prefix, sub_fields in prefix_fields.items():
        field_list.extend(as_field_expr(f, prefix, ns) for f in sub_fields)

    if matches:
        field_list.append(
            ' + '.join(matches) + f" as {field_exprs(_add_namespace(_RELEVANCE_FIELD, ns))}"
        )
        prefix_fields['__ORG'][_RELEVANCE_FIELD] = None

    source = '\nLEFT JOIN '.join(
        itertools.chain(
            [_alias_table(get_table_name(target_type), '__ORG')],
            [
                _alias_table(get_table_name(target_type, f), _get_alias_for_unwind(f, unwind)) 
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
        _build_where(field_op_var, ns)
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
            _alias_table(
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
            _build_where(tuple(itertools.chain(field_ops, nested_where))),
            _build_order_by(order_by),
            _build_limit_and_offset(limit or 100_000_000_000, offset)
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
            joined_queries.append(f'{join_scope}JOIN {_alias_table(query, f"__{current_ns.upper()}")}'
                f' ON {field_exprs(left_key)} = {field_exprs(right_key)}')
        else:
            joined_queries.append(_alias_table(query, '__BASE'))

        prev_ns = current_ns

    return join_line(joined_queries), total_fields


def _count_row_query(query:str) -> str:
    return join_line(
        'SELECT',
        '  COUNT(*) AS COUNT',
        'FROM',
        _alias_table(query, 'FOR_COUNT')
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


def _build_namespace_types(base_type:Type, join:Dict[str, Type], namespaces:Set[str]) -> Iterator[Tuple[str, Type]]:
    yield ('', base_type)

    yield from _build_join_from_refs(base_type, '', namespaces)

    for field_name, field_type in join.items():
        if any(field_name in ns for ns in namespaces):
            yield field_name, field_type
            yield from _build_join_from_refs(field_type, field_name + '.', namespaces)


def _build_join_from_refs(current_type:Type, current_ns:str, namespaces:Set[str]) -> Iterator[Tuple[str, Type]]:
    refs = get_stored_fields_for(current_type, ReferenceMixin)

    for field_name, (_, field_type) in refs.items():
        if any(field_name in ns for ns in namespaces):
            generic_type = get_base_generic_alias_of(field_type, ReferenceMixin)

            ref_type = get_args(generic_type)[0]

            yield current_ns + field_name, ref_type

            yield from _build_join_from_refs(ref_type, current_ns + field_name + '.', namespaces)


def _find_join_keys(join:Tuple[Tuple[str, Type],...]) -> Dict[Tuple[Type, Type], Tuple[str, str]]:
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
            raise RuntimeError(f'{base_type} or {joined_type} does not have reference which links both.')

        keys[(base_type, joined_type)] = (fields[0], fields[1])

    return keys


def _find_join_key(base_type:Type, target_type:Type, reversed:bool = False) -> Tuple[str, str] | None:
    refs = get_stored_fields_for(base_type, ReferenceMixin)

    for field_name, (_, field_type) in refs.items():
        generic_type = get_base_generic_alias_of(field_type, ReferenceMixin)

        if get_args(generic_type)[0] == target_type:
            target_field = field_type._target_field

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
        _alias_table(query_for_base, _BASE_TABLE_NAME)
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
                f'JOIN {_alias_table(get_table_name(ns_type), ns_table_name)} ON '
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
    return value.replace('.', '_').replace(',', '_').replace(' ', '').upper()
