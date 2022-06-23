from typing import Type, List
import pytest
from decimal import Decimal
from datetime import date, datetime

from pydantic import condecimal, constr, Field

from ormdantic.schema import PersistentModel
from ormdantic.db.queries import (
    get_materialized_fields, get_table_name, 
    get_sql_for_creating_table, 
    _get_field_db_type, 
    _generate_json_table_for_part_of,
    field_exprs,
    get_query_and_args_for_upserting,
    get_query_and_args_for_reading,
    get_query_and_args_for_updating,
    get_query_and_args_for_deleting,
    get_sql_for_inserting_parts_table, 
    join_line, 
    _ENGINE
)
from ormdantic.schema.base import (
    ArrayStringIndex, FullTextSearchedStringIndex, FullTextSearchedStr, PartOfMixin, 
    UniqueStringIndex, StringIndex, DecimalIndex, IntIndex, DateIndex,
    DateTimeIndex, update_part_of_forward_refs, IdentifiedModel, UuidStr, MaterializedFieldDefinitions
)

from .tools import (
    use_temp_database_cursor_with_model, 
)

ConStr200 = constr(max_length=200)
ConStr10 = constr(max_length=10)

ConDec66 = condecimal(max_digits=66)

def test_get_table_name():
    class SimpleBaseModel(PersistentModel):
        pass

    assert 'model_SimpleBaseModel' == get_table_name(SimpleBaseModel)
    assert 'model_SimpleBaseModel_part' == get_table_name(SimpleBaseModel, 'part')


def test_get_sql_for_create_table():
    class SimpleBaseModel(PersistentModel):
        pass

    assert (
        'CREATE TABLE IF NOT EXISTS `model_SimpleBaseModel` (\n'
        '  `__row_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,\n'
        '  `__json` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin CHECK (JSON_VALID(`__json`))\n'
        f'){_ENGINE}'
    ) == next(get_sql_for_creating_table(SimpleBaseModel))


def test_get_sql_for_create_table_with_index():
    class SampleModel(PersistentModel):
        i1: FullTextSearchedStr
        i2: FullTextSearchedStringIndex
        i3: UniqueStringIndex
        i4: StringIndex
        i5: ArrayStringIndex
        i6: DecimalIndex
        i7: IntIndex
        i8: DateIndex
        i9: DateTimeIndex
        i10: UuidStr

    assert (
f"""CREATE TABLE IF NOT EXISTS `model_SampleModel` (
  `__row_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `__json` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin CHECK (JSON_VALID(`__json`)),
  `i1` VARCHAR(200) AS (JSON_VALUE(`__json`, '$.i1')) STORED,
  `i2` VARCHAR(200) AS (JSON_VALUE(`__json`, '$.i2')) STORED,
  `i3` VARCHAR(200) AS (JSON_VALUE(`__json`, '$.i3')) STORED,
  `i4` VARCHAR(200) AS (JSON_VALUE(`__json`, '$.i4')) STORED,
  `i5` TEXT AS (JSON_EXTRACT(`__json`, '$.i5[*]')) STORED,
  `i6` DECIMAL(65) AS (JSON_VALUE(`__json`, '$.i6')) STORED,
  `i7` TEXT AS (JSON_VALUE(`__json`, '$.i7')) STORED,
  `i8` DATE AS (JSON_VALUE(`__json`, '$.i8')) STORED,
  `i9` DATETIME(6) AS (JSON_VALUE(`__json`, '$.i9')) STORED,
  `i10` VARCHAR(36),
  KEY `i2_index` (`i2`),
  UNIQUE KEY `i3_index` (`i3`),
  KEY `i4_index` (`i4`),
  KEY `i5_index` (`i5`),
  KEY `i6_index` (`i6`),
  KEY `i7_index` (`i7`),
  KEY `i8_index` (`i8`),
  KEY `i9_index` (`i9`),
  UNIQUE KEY `i10_index` (`i10`),
  FULLTEXT INDEX `ft_index` (`i1`,`i2`) COMMENT 'parser "TokenBigramIgnoreBlankSplitSymbolAlphaDigit"'
){_ENGINE}"""
) == next(get_sql_for_creating_table(SampleModel))


def test_get_sql_for_create_part_of_table():
    class Part(PersistentModel, PartOfMixin['Container']):
        order: StringIndex

    class Container(PersistentModel):
        parts: List[Part] = Field(default=[])

    update_part_of_forward_refs(Part, locals())
    sqls = get_sql_for_creating_table(Part)

    assert (
        'CREATE TABLE IF NOT EXISTS `model_Part_pbase` (\n'
        '  `__row_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,\n'
        '  `__root_row_id` BIGINT,\n'
        '  `__container_row_id` BIGINT,\n'
        '  `__json_path` VARCHAR(255),\n'
        '  `order` VARCHAR(200),\n'
        '  KEY `__root_row_id_index` (`__root_row_id`),\n'
        '  KEY `order_index` (`order`)\n'
        ')'
    ) == next(sqls, None)

    assert (
        'CREATE VIEW IF NOT EXISTS `model_Part` AS (\n'
        '  SELECT\n'
        '    JSON_EXTRACT(`model_Container`.`__json`, `model_Part_pbase`.`__json_path`) AS `__json`,\n'
        '    `model_Part_pbase`.`__row_id`,\n'
        '    `model_Part_pbase`.`__root_row_id`,\n'
        '    `model_Part_pbase`.`__container_row_id`,\n'
        '    `model_Part_pbase`.`order`\n'
        '  FROM `model_Part_pbase`\n'
        '    JOIN `model_Container` ON `model_Container`.`__row_id` = `model_Part_pbase`.`__container_row_id`\n'
        ')'
    ) == next(sqls, None)

    assert None is next(sqls, None)


@pytest.mark.parametrize('type_, expected', [
    (str, 'TEXT'),
    (StringIndex, 'VARCHAR(200)'),
    (ConStr200, 'VARCHAR(200)'),
    (constr(), 'VARCHAR(200)'),
    (ConStr10, 'VARCHAR(10)'),
    (ConDec66, 'DECIMAL(65)'),
    (condecimal(), 'DECIMAL(65)'),
    (Decimal, 'DECIMAL(65)'),
    (int, 'BIGINT'),
    (date, 'DATE'),
    (datetime, 'DATETIME(6)'),
    (bool, 'BOOL'),
])
def test_get_field_type(type_:Type, expected:str):
    assert expected == _get_field_db_type(type_)


@pytest.mark.parametrize('items, expected', [
    (
        (
            '$.items',
            False,
            {
                'title':  (('..', '$.title'), str),
                'name':  (('$.name',), str),
                'item':  (('$.des.item',), str),
                'id':  (('$.id',), str),
            }
        ),
f"""JSON_TABLE(
  CONTAINER.`__json`,
  '$' COLUMNS (
    `title` TEXT PATH '$.title',
    NESTED PATH '$.items' COLUMNS (
      `__part_order` FOR ORDINALITY,
      `name` TEXT PATH '$.name',
      `item` TEXT PATH '$.des.item',
      `id` TEXT PATH '$.id'
    )
  )
) AS __PART_JSON_TABLE""",
    ),
    (
        (
            '$.items',
            True,
            {
                'title':  (('..', '$.title'), str),
                'name':  (('$.data', '$.name'), str),
                'item':  (('$.data', '$.des.item'), str),
                'id':  (('$.id',), str),
            }
        ),
f"""JSON_TABLE(
  CONTAINER.`__json`,
  '$' COLUMNS (
    `title` TEXT PATH '$.title',
    NESTED PATH '$.items[*]' COLUMNS (
      `__part_order` FOR ORDINALITY,
      `id` TEXT PATH '$.id',
      NESTED PATH '$.data' COLUMNS (
        `name` TEXT PATH '$.name',
        `item` TEXT PATH '$.des.item'
      )
    )
  )
) AS __PART_JSON_TABLE"""
    ),
])
def test_generate_json_table(items, expected:str):
    assert expected == join_line(_generate_json_table_for_part_of(*items))


def test_get_field_type_with_exception():
    class WrongType():
        pass

    with pytest.raises(RuntimeError):
        _get_field_db_type(WrongType)


def test_field_exprs():
    assert '*' == field_exprs('*')
    assert ['*', '`f`', '`f`', 'T() as T'] == list(field_exprs(['*', 'f', '`f`', 'T() as T']))


def test_get_materialized_fields():
    class MyModel(PersistentModel):
        order: StringIndex

    # Container에서 시작함으로 $.part.에 $.order가 된다.
    assert {'order': (('$.order',), StringIndex)} == get_materialized_fields(MyModel)


def test_get_materialized_fields_for_mro():
    class OrderModel(PersistentModel):
        _materialized_fields: MaterializedFieldDefinitions = {
            'order': (('$.order_name',), StringIndex)
        }

    class BaseOrderModel(PersistentModel):
        _materialized_fields: MaterializedFieldDefinitions = {
            'name': (('$.name',), StringIndex),
            'order': (('$.order',), StringIndex)
        }

    class DerivedModel1(BaseOrderModel, OrderModel):
        _materialized_fields: MaterializedFieldDefinitions = {
            'hello': (('$.hello',), StringIndex),
        }
        pass

    # Container에서 시작함으로 $.part.에 $.order가 된다.
    assert {
        'hello': (('$.hello',), StringIndex),
        'order': (('$.order',), StringIndex), 
        'name': (('$.name',), StringIndex)
    } == get_materialized_fields(DerivedModel1)

    class DerivedModel2(OrderModel, BaseOrderModel):
        pass

    assert {
        'order': (('$.order_name',), StringIndex), 
        'name': (('$.name',), StringIndex)
    } == get_materialized_fields(DerivedModel2)


def test_get_materialized_fields_of_parts():
    class Part(PersistentModel, PartOfMixin['Container']):
        order: StringIndex

    class Container(PersistentModel):
        parts: List['Part'] = Field(default=[])

    update_part_of_forward_refs(Part, locals())
    update_part_of_forward_refs(Container, locals())

    # Container에서 시작함으로 $.part.에 $.order가 된다.
    assert {'order': (('$.order',), StringIndex)} == get_materialized_fields(Part)
    assert {} == get_materialized_fields(Container)


def test_get_materialized_fields_of_single_part():
    class SinglePart(PersistentModel, PartOfMixin['ContainerForSingle']):
        order: StringIndex

    class ContainerForSingle(PersistentModel):
        part: SinglePart 

    update_part_of_forward_refs(SinglePart, locals())

    # Container에서 시작함으로 $.part.에 $.order가 된다.
    assert {'order': (('$.order',), StringIndex)} == get_materialized_fields(SinglePart)


def test_get_materialized_fields_raise_exception():
    class PathNotStartWithDollar(PersistentModel):
        _materialized_fields: MaterializedFieldDefinitions = {
            'order': (('order_name',), StringIndex)
        }

    with pytest.raises(RuntimeError, match='.*path must start with \\$.*'):
        get_materialized_fields(PathNotStartWithDollar)

    class PathsNotEndWithDollar(PersistentModel):
        _materialized_fields: MaterializedFieldDefinitions = {
            'order': (('..', '$.order'), ArrayStringIndex)
        }

    with pytest.raises(RuntimeError, match='.*collection type should end with.*'):
        get_materialized_fields(PathsNotEndWithDollar)


class SimpleBaseModel(IdentifiedModel):
    pass

def test_get_query_and_args_for_reading():
    model = SimpleBaseModel(id=UuidStr('@'), version='0.1.0')

    with use_temp_database_cursor_with_model(model, model_created=False) as cursor:
        model.id = UuidStr("0")
        query_and_args = get_query_and_args_for_upserting(model)

        cursor.execute(*query_and_args)

        model.id = UuidStr("1")
        query_and_args = get_query_and_args_for_upserting(model)

        cursor.execute(*query_and_args)

        query_and_args = get_query_and_args_for_reading(
            SimpleBaseModel, '*', tuple())

        cursor.execute(*query_and_args)
        assert [
            {'__row_id': 1, 'id': '0', '__json': '{"version":"0.1.0","id":"0"}'},
            {'__row_id': 2, 'id': '1', '__json': '{"version":"0.1.0","id":"1"}'}
        ] == cursor.fetchall()

        query_and_args = get_query_and_args_for_reading(
            SimpleBaseModel, ('__row_id',), (('__row_id', '=', 2),)
        )

        cursor.execute(*query_and_args)
        assert [{'__row_id': 2}] == cursor.fetchall()


def test_get_query_and_args_for_updating():
    model = SimpleBaseModel(id=UuidStr('@'), version='0.1.0')
    with use_temp_database_cursor_with_model(model) as cursor:
        query_and_args = get_query_and_args_for_reading(
            SimpleBaseModel, '*', tuple())

        cursor.execute(*query_and_args)
        assert [
            {'__row_id': 1, 'id': '@', '__json': '{"version":"0.1.0","id":"@"}'},
        ] == cursor.fetchall()

        model.version = "0.2.0" 
        query_and_args = get_query_and_args_for_updating(model, (('__row_id', '=', 1),))

        cursor.execute(*query_and_args)
        
        query_and_args = get_query_and_args_for_reading(
            SimpleBaseModel, '*', tuple())

        cursor.execute(*query_and_args)
        assert [
            {'__row_id': 1, 'id': '@', '__json': '{"version":"0.2.0","id":"@"}'},
        ] == cursor.fetchall()


def test_get_query_and_args_for_deleting():
    model = SimpleBaseModel(id=UuidStr('@'), version='0.1.0')
    with use_temp_database_cursor_with_model(model) as cursor:
        query_and_args = get_query_and_args_for_deleting(
            SimpleBaseModel, tuple())

        cursor.execute(*query_and_args)
        assert tuple() == cursor.fetchall()


def test_get_query_and_args_for_reading_for_materialized_fields():
    class SampleMaterializedModel(IdentifiedModel):
        name: FullTextSearchedStringIndex

    model = SampleMaterializedModel(id=UuidStr('@'), version='0.1.0', 
                                    name=FullTextSearchedStringIndex('sample'))

    with use_temp_database_cursor_with_model(model) as cursor:
        model.id = UuidStr("@")
        query_and_args = get_query_and_args_for_reading(
            SampleMaterializedModel, ('id', 'name'), (('name', '=', 'sample'),))

        cursor.execute(*query_and_args)

        assert [{'id':'@', 'name':'sample'}] == cursor.fetchall()


def test_get_query_and_args_for_reading_for_parts():
    class ContainerModel(IdentifiedModel):
        name: FullTextSearchedStringIndex
        parts: List['PartModel']
     
    class PartModel(PersistentModel, PartOfMixin[ContainerModel]):
        name: FullTextSearchedStringIndex

    update_part_of_forward_refs(ContainerModel, locals())

    model = ContainerModel(id=UuidStr('@'), 
                           version='0.1.0',
                           name=FullTextSearchedStringIndex('sample'),
                           parts=[
                               PartModel(name=FullTextSearchedStringIndex('part1')),
                               PartModel(name=FullTextSearchedStringIndex('part2')),
                           ])

    with use_temp_database_cursor_with_model(model, 
                                             keep_database_when_except=False) as cursor:
        model.id = UuidStr("@")
        query_and_args = get_query_and_args_for_reading(
            ContainerModel, ('id', 'name'), (('name', '=', 'sample'),))

        cursor.execute(*query_and_args)

        assert [{'id':'@', 'name':'sample'}] == cursor.fetchall()

        query_and_args = get_query_and_args_for_reading(
            PartModel, ('__row_id', '__json', 'name'), tuple())

        cursor.execute(*query_and_args)

        assert [
            {'__row_id':1, '__json':'{"name": "part1"}', 'name':'part1'}, 
            {'__row_id':2, '__json':'{"name": "part2"}', 'name':'part2'}
        ] == cursor.fetchall()


def test_get_query_and_args_for_reading_for_nested_parts():
    class ContainerModel(IdentifiedModel):
        name: FullTextSearchedStringIndex
        part: 'PartModel'
     
    class PartModel(PersistentModel, PartOfMixin[ContainerModel]):
        name: FullTextSearchedStringIndex
        members: List['MemberModel']

    class MemberModel(PersistentModel, PartOfMixin[PartModel]):
        descriptions: ArrayStringIndex

    update_part_of_forward_refs(ContainerModel, locals())
    update_part_of_forward_refs(PartModel, locals())

    model = ContainerModel(id=UuidStr('@'), 
                           version='0.1.0',
                           name=FullTextSearchedStringIndex('sample'),
                           part=PartModel(
                                name=FullTextSearchedStringIndex('part1'),
                                members=[
                                    MemberModel(descriptions=ArrayStringIndex(['desc1'])),
                                    MemberModel(descriptions=ArrayStringIndex(['desc1', 'desc2']))
                                ]
                           ))

    with use_temp_database_cursor_with_model(model, 
                                             keep_database_when_except=False) as cursor:
        model.id = UuidStr("@")
        query_and_args = get_query_and_args_for_reading(
            ContainerModel, ('id', 'name'), (('name', '=', 'sample'),))

        cursor.execute(*query_and_args)

        assert [{'id':'@', 'name':'sample'}] == cursor.fetchall()

        query_and_args = get_query_and_args_for_reading(
            PartModel, ('__row_id', 'name'), tuple())

        cursor.execute(*query_and_args)

        assert [
            {'__row_id':1, 'name':'part1'}, 
        ] == cursor.fetchall()

        query_and_args = get_query_and_args_for_reading(
            MemberModel, ('__row_id', '__json', 'descriptions'), tuple())

        cursor.execute(*query_and_args)

        assert [
            {'__row_id':1, '__json':'{"descriptions": ["desc1"]}', 'descriptions':'["desc1"]'}, 
            {'__row_id':2, '__json':'{"descriptions": ["desc1", "desc2"]}', 'descriptions':'["desc1","desc2"]'}, 
        ] == cursor.fetchall()



def test_get_sql_for_inserting_parts_table():
    class Part(PersistentModel, PartOfMixin['Container']):
        order: StringIndex
        codes: ArrayStringIndex

    class Container(PersistentModel):
        parts: List[Part] = Field(default=[])

    update_part_of_forward_refs(Part, locals())
    sqls = get_sql_for_inserting_parts_table(Container)

    assert len(sqls[Part]) == 2
    assert join_line(
        "DELETE FROM model_Part_pbase",
        "WHERE `__root_row_id` = %(__root_row_id)s"
    ) == sqls[Part][0]

    assert join_line(
        "INSERT INTO model_Part_pbase",
        "(",
        "  `__root_row_id`,",
        "  `__container_row_id`,",
        "  `__json_path`,",
        "  `order`,",
        "  `codes`",
        ")",
        "SELECT",
        "  `__root_row_id`,",
        "  `__container_row_id`,",
        "  `__json_path`,",
        "  `order`,",
        "  JSON_ARRAYAGG(`codes`) AS `codes`",
        "FROM (",  
        "  SELECT",
        "    `__part_order`,",
        "    CONTAINER.`__row_id` as `__root_row_id`,",
        "    CONTAINER.`__row_id` as `__container_row_id`,",
        "    CONCAT('$.parts[', `__part_order` - 1, ']') as `__json_path`,",
        "    `__PART_JSON_TABLE`.`order`,",
        "    `__PART_JSON_TABLE`.`codes`",
        "  FROM",
        "    model_Container as CONTAINER,",
        "    JSON_TABLE(",
        "      CONTAINER.`__json`,",
        "      '$' COLUMNS (",
        "        NESTED PATH '$.parts[*]' COLUMNS (",
        "          `__part_order` FOR ORDINALITY,",
        "          `order` VARCHAR(200) PATH '$.order',",
        "          NESTED PATH '$.codes[*]' COLUMNS (",
        "            `codes` TEXT PATH '$'",
        "          )",
        "        )",
        "      )",    
        "    ) AS __PART_JSON_TABLE",
        "  WHERE",
        "    CONTAINER.`__row_id` = %(__root_row_id)s",
        ") AS T1",
        "GROUP BY",
        "  `__container_row_id`,",
        "  `__part_order`",
    ) == sqls[Part][1]


def test_get_query_and_args_for_reading_for_matching():
    class MyModel(PersistentModel):
        order: FullTextSearchedStr
        name: FullTextSearchedStr

    sql, args = get_query_and_args_for_reading(MyModel, ('order',), (('', 'match', '+FAST'),))

    assert join_line(
        "SELECT",
        "  *",
        "FROM (",
        "  SELECT",
        "    `order`,",
        "    MATCH (`order`,`name`) AGAINST (%(order_name)s IN BOOLEAN MODE) as `__relevance`",
        "  FROM model_MyModel",
        "  WHERE",
        "    MATCH (`order`,`name`) AGAINST (%(order_name)s IN BOOLEAN MODE)",
        ") AS FOR_ORDER_BY",
        "ORDER BY",
        "  `__relevance` DESC"
    ) == sql

    assert {
        "order_name": "+FAST"
    } == args


def test_get_query_and_args_for_reading_for_order_by():
    class MyModel(PersistentModel):
        order: FullTextSearchedStr
        name: FullTextSearchedStr

    sql, _ = get_query_and_args_for_reading(MyModel, ('order',), tuple(), order_by=('order desc', 'name'))

    assert join_line(
        "SELECT",
        "  *",
        "FROM (",
        "  SELECT",
        "    `order`",
        "  FROM model_MyModel",
        ") AS FOR_ORDER_BY",
        "ORDER BY",
        "  `order` desc,",
        "  `name`"
    ) == sql


