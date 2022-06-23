from typing import (
    Type, Iterator, overload, Iterable, List, Tuple, cast, Dict, 
    Any, DefaultDict
)
from datetime import datetime, date
from decimal import Decimal
from collections import defaultdict
import itertools
import functools
import inspect

from pydantic import ConstrainedStr, ConstrainedDecimal
from pymysql.cursors import DictCursor

from ormdantic.util import is_derived_from, convert_tuple

from ..util import get_logger
from ..schema.base import (
    ArrayIndexMixin, FullTextSearchedMixin, IdentifyingMixin, IndexMixin, 
    MaterializedFieldDefinitions, ModelT, MaterializedMixin, PartOfMixin, 
    UniqueIndexMixin, get_container_type, get_field_name_and_type, 
    get_part_field_names, get_part_types, is_field_collection_type,
    PersistentModel, is_collection_type_of
)

_MAX_VAR_CHAR_LENGTH = 200
_MAX_DECIMAL_DIGITS = 65

_JSON_TYPE = 'LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin'
_JSON_CHECK = 'CHECK (JSON_VALID({}))'

_PART_BASE_TABLE = 'pbase'

# field name for internal use.
_ROW_ID_FIELD = '__row_id'
_CONTAINER_ROW_ID_FIELD = '__container_row_id'
_ROOT_ROW_ID_FIELD = '__root_row_id'
_JSON_FIELD = '__json'
_JSON_PATH_FIELD = '__json_path'
_PART_ORDER_FIELD = '__part_order'
_RELEVANCE_FIELD = '__relevance'

# for cjk full text search. It seemed to insert record is slow.
# _ENGINE = r""" ENGINE=mroonga COMMENT='engine "innodb" DEFAULT CHARSET=utf8'"""
_ENGINE = ""
_FULL_TEXT_SEARCH_OPTION = r"""COMMENT 'parser "TokenBigramIgnoreBlankSplitSymbolAlphaDigit"'"""


Where = Tuple[Tuple[str, str, Any]]
FieldOp = Tuple[Tuple[str, str]]

_logger = get_logger(__name__)

def get_table_name(type_:Type[ModelT], postfix:str = ''):
    return f'model_{type_.__name__}{"_" + postfix if postfix else ""}' 



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


def join_line(*lines:str | Iterable[str], 
              new_line: bool = True, use_comma: bool = False) -> str:
    sep = (',' if use_comma else '') + ('\n' if new_line else '')

    return (
        sep.join(line for line in itertools.chain(*[[l] if isinstance(l, str) else l for l in lines]) if line) 
    )


def tab_each_line(*statements:Iterable[str] | str, use_comma: bool = False) -> str:
    line = join_line(*statements, use_comma=use_comma, new_line=True)

    return join_line(['  ' + item for item in line.split('\n')], use_comma=False, new_line=True)


def get_sql_for_creating_table(type_:Type[ModelT]):
    materialized = get_materialized_fields(type_)

    if issubclass(type_, PartOfMixin):
        # The container field will be saved in container's table.
        # but for the full text search fields, we need to save them in part's table 
        # even it is in part's materialized fields.
        part_materialized = get_materialized_fields_for_part_of(type_)

        yield _build_create_model_table_statement(
            get_table_name(type_, _PART_BASE_TABLE), 
            _get_part_table_fields(),
            _get_table_materialized_fields(part_materialized, False),
            _get_part_table_indexes(),
            _get_table_indexes(materialized),
        )

        part_table_name = get_table_name(type_, _PART_BASE_TABLE)

        container_type = get_container_type(type_)
        assert container_type 

        container_table_name = get_table_name(container_type)

        part_fields = (_ROW_ID_FIELD, _ROOT_ROW_ID_FIELD, _CONTAINER_ROW_ID_FIELD, 
                       *part_materialized.keys())
        container_fields = tuple(set(materialized.keys()) - set(part_fields))

        joined_table = _build_join_table(
            (part_table_name, _CONTAINER_ROW_ID_FIELD), 
            (container_table_name, _ROW_ID_FIELD)
        )

        yield _build_create_part_of_model_view_statement(
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
        yield _build_create_model_table_statement(
            get_table_name(type_), 
            _get_table_fields(),
            _get_table_materialized_fields(materialized, True),
            _get_table_indexes(materialized)
        )


def get_materialized_fields_for_full_text_search(type_:Type[ModelT]):
    materialized = get_materialized_fields(type_)

    full_text_search_materialized = {
        k:(paths, type_) for k, (paths, type_) in materialized.items() 
        if is_derived_from(type_, FullTextSearchedMixin) 
            or is_collection_type_of(type_, FullTextSearchedMixin)
    }

    return full_text_search_materialized


def get_materialized_fields_for_part_of(type_:Type[ModelT]):
    materialized = get_materialized_fields(type_)

    part_materialized = {
        k:(paths, type_) for k, (paths, type_) in materialized.items() 
        if not _check_from_container(paths) 
            or is_derived_from(type_, FullTextSearchedMixin)
            or is_collection_type_of(type_, FullTextSearchedMixin)
    }

    return part_materialized


def _check_from_container(paths:Tuple[str]) -> bool:
    return paths and paths[0] == '..' 


def get_materialized_fields(type_:Type[ModelT]):
    return _get_materialized_fields(cast(Type, type_))


@functools.lru_cache()
def _get_materialized_fields(type_:Type):
    materialized : MaterializedFieldDefinitions = {
        field_name:(_get_json_paths(type_, field_name, field_type), field_type)
        for field_name, field_type 
        in get_field_name_and_type(type_, 
            lambda t: is_derived_from(t, MaterializedMixin))
    }

    for fields in reversed([cast(PersistentModel, base)._materialized_fields
        for base in inspect.getmro(type_) if is_derived_from(base, PersistentModel)]):
        materialized.update(fields)

    for field, (paths, field_type) in materialized.items():
        _validate_json_paths(paths, is_collection_type_of(field_type))

    return materialized
        

def _build_create_model_table_statement(table_name:str, 
                                        *field_definitions: Iterator[str]) -> str:
    return join_line(
        f'CREATE TABLE IF NOT EXISTS {field_exprs(table_name)} (',
        tab_each_line(
            *field_definitions, use_comma=True
        ),
        f'){_ENGINE}'
    )


def _build_create_part_of_model_view_statement(view_name:str, joined_table:str,
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


def _build_join_table(*table_names:Tuple[str, str]) -> str:
    table_iter = iter(table_names)
    main_table, main_index = next(table_iter)

    target_tables = [field_exprs(main_table)]

    ref_index = field_exprs(main_index, main_table)

    for table, index_name in table_iter:
        target_tables.append(f'  JOIN {field_exprs(table)} ON {field_exprs(index_name, table)} = {ref_index}')

    return join_line(target_tables)


def _get_table_fields() -> Iterator[str]:
    yield f'{field_exprs(_ROW_ID_FIELD)} BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY'
    yield f'{field_exprs(_JSON_FIELD)} {_JSON_TYPE} {_JSON_CHECK.format(field_exprs(_JSON_FIELD))}'


def _get_part_table_fields() -> Iterator[str]:
    yield f'{field_exprs(_ROW_ID_FIELD)} BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY'
    yield f'{field_exprs(_ROOT_ROW_ID_FIELD)} BIGINT'
    yield f'{field_exprs(_CONTAINER_ROW_ID_FIELD)} BIGINT'
    yield f'{field_exprs(_JSON_PATH_FIELD)} VARCHAR(255)'


def _get_part_table_indexes() -> Iterator[str]:
    yield f'KEY `{_ROOT_ROW_ID_FIELD}_index` ({field_exprs(_ROOT_ROW_ID_FIELD)})'


def _get_table_materialized_fields(materialized:MaterializedFieldDefinitions, json_stored:bool = False) -> Iterator[str]:
    for field_name, (paths, field_type) in materialized.items():
        stored = '' if not json_stored else _generate_stored_for_json_path(paths, field_type)
        yield f"""{field_exprs(field_name)} {_get_field_db_type(field_type)}{stored}"""


def _get_view_fields(fields:Dict[str, Tuple[str, ...]]) -> Iterator[str]:
    for table_name, field_names in fields.items():
        for field_name in field_names:
            yield f'{field_exprs(field_name, table_name)}'


def _get_table_indexes(materialized:MaterializedFieldDefinitions) -> Iterator[str]:
    for field_name, (_, field_type) in materialized.items():
        key_def = _generate_key_definition(field_type)

        if key_def: 
            yield f"""{key_def} `{field_name}_index` ({field_exprs(field_name)})"""

    full_text_searched_fields = set(
        field_name for field_name, (_, field_type) in materialized.items()
        if is_derived_from(field_type, FullTextSearchedMixin)
    )

    if full_text_searched_fields:
        yield f'''FULLTEXT INDEX `ft_index` ({
            join_line(
                field_exprs(sorted(full_text_searched_fields)),
                new_line=False, use_comma=True
            )
        }) {_FULL_TEXT_SEARCH_OPTION}'''


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

    if issubclass(type_, (str, MaterializedMixin)):
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

    if ' ' in field and '(' in field: # if field has function or "as" statement.
        return field

    return f'`{field}`'


def _get_json_paths(type_, field_name, field_type) -> Tuple[str,...]:
    paths: List[str] = []

    if is_derived_from(field_type, ArrayIndexMixin):
        paths.extend([f'$.{field_name}[*]', '$'])
    else:
        paths.append(f'$.{field_name}')

    return tuple(paths)


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
    query_args = {'__json': model.json()}

    materialized = get_materialized_fields(type(model))

    for field_name, (_, field_type)  in materialized.items():
        if is_derived_from(field_type, IdentifyingMixin):
            query_args[field_name] = getattr(model, field_name)

    return _get_sql_for_upserting(cast(Type, type(model))), query_args


def _get_identifying_fields(model_type:Type[ModelT]) -> Tuple[str]:
    fields = []
    materialized = get_materialized_fields(model_type)

    for field_name, (_, field_type)  in materialized.items():
        if is_derived_from(field_type, IdentifyingMixin):
            fields.append(field_name)

    return tuple(fields)
            

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

def get_sql_for_inserting_parts_table(model_type:Type) -> Dict[Type, Tuple[str, ...]]:
    type_sqls : Dict[str, Tuple[str, str]] = {}
    part_types = get_part_types(model_type)

    is_root = not is_derived_from(model_type, PartOfMixin)
    sqls = []
    
    for part_type in part_types:
        part_fields = get_part_field_names(model_type, part_type)

        assert part_fields

        delete_sql = join_line([
            f'DELETE FROM {get_table_name(part_type, _PART_BASE_TABLE)}',
            f'WHERE {field_exprs(_ROOT_ROW_ID_FIELD)} = %(__root_row_id)s'
        ])

        sqls.append(delete_sql)

        fields = get_materialized_fields_for_part_of(part_type)

        target_fields = tuple(itertools.chain(
            [_ROOT_ROW_ID_FIELD, _CONTAINER_ROW_ID_FIELD, _JSON_PATH_FIELD],
            fields.keys()
        ))

        root_field = 'CONTAINER.' + (field_exprs(_ROW_ID_FIELD) if is_root else field_exprs(_ROOT_ROW_ID_FIELD))

        for part_field in part_fields:
            is_collection = is_field_collection_type(model_type, part_field)
            json_path = f'$.{part_field}'

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
                    _generate_select_field_of_json_table(target_fields, fields), use_comma=True
                ),
                f'FROM (',
                tab_each_line([
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
                        field_exprs(fields, table_name='__PART_JSON_TABLE'),
                        use_comma=True
                    ),
                    f'FROM',
                    tab_each_line(
                        [f'{get_table_name(model_type)} as CONTAINER'],
                        _generate_json_table_for_part_of(json_path, is_collection, fields),
                        use_comma=True
                    ),
                    f'WHERE',
                    tab_each_line(
                        f'{root_field} = %(__root_row_id)s'
                    ),
                ]),
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


def _generate_json_table_for_part_of(json_path: str,
                                     is_collection: bool, fields: MaterializedFieldDefinitions) -> str:
    items = [
        (
            _resolve_paths_for_part_of(paths, json_path + ('[*]' if is_collection else '')), 
            field_name, field_type
        )
        for field_name, (paths, field_type) in fields.items()
    ]

    return join_line(
        'JSON_TABLE(',
        tab_each_line(
            f'CONTAINER.{field_exprs(_JSON_FIELD)}', 
            _generate_nested_json_table('$', items), 
            use_comma=True
        ),
        ') AS __PART_JSON_TABLE'
    )


def _generate_select_field_of_json_table(target_fields:Tuple[str,...], 
                                         fields: MaterializedFieldDefinitions
                                         ) -> Iterable[str]:
    for field_name in target_fields:
        if field_name in fields:
            _, field_type = fields[field_name]

            if is_collection_type_of(field_type):
                yield f"JSON_ARRAYAGG({field_exprs(field_name)}) AS {field_exprs(field_name)}"
                continue

        yield field_exprs(field_name)


def _generate_nested_json_table(first_path:str, items:Iterable[Tuple[List[str], str, Type]], depth:int = 0) -> str:
    nested_items: DefaultDict[str, List[Tuple[List[str], str, Type]]] = defaultdict(list)

    columns = []

    if depth == 1:
        columns.append(f'{field_exprs(_PART_ORDER_FIELD)} FOR ORDINALITY')

    for paths, field_name, field_type in items:
        if len(paths) == 1:
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


def _validate_json_paths(paths:Tuple[str], is_collection:bool):
    if any(not (p == '..' or p.startswith('$.') or p == '$') for p in paths):
        _logger.fatal('{paths} has one item which did not starts with .. or $.')
        raise RuntimeError('Invalid path expression. the path must start with $')

    if is_collection and paths[-1] != '$':
        _logger.fatal('{paths} should end with $ for collection type')
        raise RuntimeError('Invalid path expression. collection type should end with $.')


def _resolve_paths_for_part_of(paths:Tuple[str, ...], json_path:str) -> List[str]:
    if paths[0] == '..':
        resolved = [*paths[1:]]
    else:
        resolved = [json_path, *paths]

    return resolved


def get_query_and_args_for_reading(type_:Type[ModelT], fields:Tuple[str,...] | str, where:Where, 
                                   *,
                                   unwind: Tuple[str,...] | str = tuple(),
                                   order_by: Tuple[str, ...] | str = tuple(), 
                                   offset : None | int = None, 
                                   limit : None | int = None):
    fields = convert_tuple(fields)
    # unwind = _make_tuple(unwind)
    order_by = convert_tuple(order_by)

    where = _fill_empty_fields_for_match(
        where, 
        get_materialized_fields_for_full_text_search(type_))

    if match_where := _has_match_in(where):
        order_by = order_by + (f"{_RELEVANCE_FIELD} DESC",)
        fields = fields + (_build_match(match_where[0]) + f" as {field_exprs(_RELEVANCE_FIELD)}",)

    return (
        _get_sql_for_reading(
            cast(Type, type_), fields, 
            tuple((f, o) for f, o, _ in where), order_by), 
        _build_args(where)
    )


def _has_match_in(where:Where):
    return next(filter(lambda w: w[1].lower() == 'match', where), None)


def _fill_empty_fields_for_match(where:Where, fields:Iterable[str]) -> Where:
    fields_expr = ','.join(fields)

    return tuple(
        (fields_expr, w[1], w[2]) if w[1] == 'match' and not w[0] else w
        for w in where
    )


@functools.lru_cache
def _get_sql_for_reading(type_:Type, fields:Tuple[str,...], field_ops:FieldOp, order_by: Tuple[str, ...]) -> str:

    sql = join_line(
        'SELECT',
        tab_each_line(
            field_exprs(fields),
            use_comma=True
        ),
        f'FROM {get_table_name(type_)}',
        _build_where(field_ops)
    )

    if order_by:
        sql = join_line(
            'SELECT',
            '  *',
            'FROM (',
            tab_each_line(
                sql
            ),
            ') AS FOR_ORDER_BY',
            _build_order_by(order_by)
        )

    return sql

def get_query_and_args_for_updating(model:PersistentModel, where:Where):
    query_args = _build_args(where)
    query_args['__json'] = model.json()
    
    return _get_sql_for_updating(cast(Type, type(model)), tuple((f, o) for f, o, _ in where)), query_args


@functools.lru_cache
def _get_sql_for_updating(type_:Type, field_ops:FieldOp):
    return (
        f'UPDATE {get_table_name(type_)} SET __json=%(__json)s {_build_where(field_ops)}'
    )


def get_query_and_args_for_deleting(type_:Type, where:Where):
    query_args = _build_args(where)

    return _get_sql_for_deleting(type_, tuple((f, o) for f, o, _ in where)), query_args


@functools.lru_cache
def _get_sql_for_deleting(type_:Type, field_and_value:FieldOp):
    return f'DELETE FROM {get_table_name(type_)} {_build_where(field_and_value)}'


def _build_args(where:Where) -> Dict[str, Any]:
    return {_get_parameter_variable_for_multiple_fields(field):value for field, _, value in where}


def _build_where(field_and_ops:FieldOp) -> str:
    if field_and_ops:
        return join_line(
            'WHERE',
            tab_each_line(
                ' AND \n'.join(_build_where_op(field, op) for field, op in field_and_ops)
            )
        )

    return ''

def _build_order_by(order_by:Tuple[str,...]) -> str:
    if order_by:
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

    return ''


def _build_where_op(fields:str, op:str):
    if op == 'match':
        return _build_match(fields)
    else:
        return f'{fields} {op} %({fields})s'


def _build_match(fields:str):
    variable = _get_parameter_variable_for_multiple_fields(fields)
    field_items = field_exprs([f for f in fields.split(',')])

    return f'MATCH ({join_line(field_items, new_line=False, use_comma=True)}) AGAINST (%({variable})s IN BOOLEAN MODE)'


def _get_parameter_variable_for_multiple_fields(fields:str):
    variable = fields.replace(',', '_').replace(' ', '')
    return variable

def execute_and_get_last_id(cursor:DictCursor, sql:str, params:Dict[str, Any]) -> int:
    _logger.debug(sql)
    cursor.execute(sql, params)
    cursor.execute('SELECT LAST_INSERT_ID() as inserted_id')
    row = cursor.fetchone()

    assert row

    return row['inserted_id']

