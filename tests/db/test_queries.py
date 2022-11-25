from typing import Type, List, ClassVar, cast, Any, Annotated
import pytest
from decimal import Decimal
from datetime import date, datetime

from pydantic import condecimal, constr, Field

from ormdantic.schema import PersistentModel
from ormdantic.database.queries import (
    _get_sql_for_upserting_single_object, get_query_and_args_for_deleting, get_query_and_args_for_purging, get_sql_for_creating_version_info_table, get_sql_for_upserting_external_index, get_stored_fields, get_table_name, 
    get_sql_for_creating_table, _get_field_db_type, _generate_json_table_for_part_of,
    _build_query_and_fields_for_core_table, field_exprs,
    get_query_and_args_for_reading,
    get_sql_for_upserting_parts, 
    join_line, 
    _build_namespace_types, _find_join_keys, _extract_fields,
    get_query_for_adjust_seq,
    _ENGINE, _RELEVANCE_FIELD,
    _get_sql_for_copying_objects
)
from ormdantic.schema.base import (
    DatedMixin, IntegerArrayIndex, StringArrayIndex, FullTextSearchedStringIndex, 
    FullTextSearchedString, PartOfMixin, VersionMixin, 
    UniqueStringIndex, StringIndex, DecimalIndex, IntIndex, DateIndex,
    DateTimeIndex, update_forward_refs, UuidStr, 
    StoredFieldDefinitions, SequenceStr, MetaIndexField,
    MetaReferenceField, MetaIdentifyingField
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

    assert 'md_SimpleBaseModel' == get_table_name(SimpleBaseModel)
    assert 'md_SimpleBaseModel_part' == get_table_name(SimpleBaseModel, 'part')


def test_get_sql_for_create_table():
    class SimpleBaseModel(PersistentModel):
        pass

    assert (
        'CREATE TABLE IF NOT EXISTS `md_SimpleBaseModel` (\n'
        '  `__row_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,\n'
        '  `__set_id` BIGINT UNSIGNED NOT NULL DEFAULT 0,\n'
        '  `__json` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin CHECK (JSON_VALID(`__json`)),\n'
        '  `__valid_start` BIGINT,\n'
        '  `__valid_end` BIGINT DEFAULT 9223372036854775807\n'
        f'){_ENGINE}'
    ) == next(get_sql_for_creating_table(SimpleBaseModel))


def test_get_sql_for_create_table_for_version():
    class SimpleBaseModel(PersistentModel, VersionMixin):
        id: Annotated[UuidStr, MetaIdentifyingField()]

    assert (
        'CREATE TABLE IF NOT EXISTS `md_SimpleBaseModel` (\n'
        '  `__row_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,\n'
        '  `__set_id` BIGINT UNSIGNED NOT NULL DEFAULT 0,\n'
        '  `__json` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin CHECK (JSON_VALID(`__json`)),\n'
        '  `__valid_start` BIGINT,\n'
        '  `__valid_end` BIGINT DEFAULT 9223372036854775807,\n'
        '  `__squashed_from` BIGINT,\n'
        '  `id` VARCHAR(64),\n'
        '  UNIQUE KEY `identifying_index` (`__set_id`,`id`,`__valid_start`)\n'
        f'){_ENGINE}'
    ) == next(get_sql_for_creating_table(SimpleBaseModel))

    class PartModel(PersistentModel, PartOfMixin['RootModel']):
        order: StringIndex

    class RootModel(PersistentModel, VersionMixin):
        id: Annotated[UuidStr, MetaIdentifyingField()]

    update_forward_refs(PartModel, locals())

    assert [
        'CREATE TABLE IF NOT EXISTS `md_PartModel_pbase` (\n'
        '  `__row_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,\n'
        '  `__root_row_id` BIGINT,\n'
        '  `__container_row_id` BIGINT,\n'
        '  `__json_path` VARCHAR(255),\n'
        '  `order` VARCHAR(200),\n'
        '  KEY `__root_row_id_index` (`__root_row_id`),\n'
        '  KEY `order_index` (`order`)\n'
        f'){_ENGINE}',
        'CREATE VIEW IF NOT EXISTS `md_PartModel` AS (\n'
        '  SELECT\n'
        '    JSON_EXTRACT(`md_RootModel`.`__json`, `md_PartModel_pbase`.`__json_path`) AS `__json`,\n'
        '    `md_PartModel_pbase`.`__row_id`,\n'
        '    `md_PartModel_pbase`.`__root_row_id`,\n'
        '    `md_PartModel_pbase`.`__container_row_id`,\n'
        '    `md_PartModel_pbase`.`order`,\n'
        '    `md_RootModel`.`__set_id`,\n'
        '    `md_RootModel`.`__valid_start`,\n'
        '    `md_RootModel`.`__valid_end`\n'
        '  FROM `md_PartModel_pbase`\n'
        '  JOIN `md_RootModel` ON `md_RootModel`.`__row_id` = `md_PartModel_pbase`.`__container_row_id`\n'
        ')'
    ] == list(get_sql_for_creating_table(PartModel))


def test_get_sql_for_create_table_for_version_date():
    class VersionDateModel(PersistentModel, DatedMixin, VersionMixin):
        id: Annotated[UuidStr, MetaIdentifyingField()]

    assert (
        'CREATE TABLE IF NOT EXISTS `md_VersionDateModel` (\n'
        '  `__row_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,\n'
        '  `__set_id` BIGINT UNSIGNED NOT NULL DEFAULT 0,\n'
        '  `__json` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin CHECK (JSON_VALID(`__json`)),\n'
        '  `__valid_start` BIGINT,\n'
        '  `__valid_end` BIGINT DEFAULT 9223372036854775807,\n'
        '  `__squashed_from` BIGINT,\n'
        '  `applied_at` DATE,\n'
        '  `id` VARCHAR(64),\n'
        '  UNIQUE KEY `identifying_index` (`__set_id`,`applied_at`,`id`,`__valid_start`)\n'
        f'){_ENGINE}'
    ) == next(get_sql_for_creating_table(VersionDateModel))


def test_get_sql_for_create_table_with_index():
    class SampleModel(PersistentModel):
        i1: FullTextSearchedString
        i2: FullTextSearchedStringIndex
        i3: UniqueStringIndex
        i4: StringIndex
        i5: StringArrayIndex
        i6: DecimalIndex
        i7: IntIndex
        i8: DateIndex
        i9: DateTimeIndex
        i10: Annotated[UuidStr, MetaIdentifyingField()]

    assert (
f"""CREATE TABLE IF NOT EXISTS `md_SampleModel` (
  `__row_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `__set_id` BIGINT UNSIGNED NOT NULL DEFAULT 0,
  `__json` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin CHECK (JSON_VALID(`__json`)),
  `__valid_start` BIGINT,
  `__valid_end` BIGINT DEFAULT 9223372036854775807,
  `i1` VARCHAR(200) AS (JSON_VALUE(`__json`, '$.i1')) STORED,
  `i2` VARCHAR(200) AS (JSON_VALUE(`__json`, '$.i2')) STORED,
  `i3` VARCHAR(200) AS (JSON_VALUE(`__json`, '$.i3')) STORED,
  `i4` VARCHAR(200) AS (JSON_VALUE(`__json`, '$.i4')) STORED,
  `i5` TEXT AS (JSON_EXTRACT(`__json`, '$.i5[*]')) STORED,
  `i6` DECIMAL(65) AS (JSON_VALUE(`__json`, '$.i6')) STORED,
  `i7` TEXT AS (JSON_VALUE(`__json`, '$.i7')) STORED,
  `i8` DATE AS (JSON_VALUE(`__json`, '$.i8')) STORED,
  `i9` DATETIME(6) AS (JSON_VALUE(`__json`, '$.i9')) STORED,
  `i10` VARCHAR(64),
  UNIQUE KEY `identifying_index` (`__set_id`,`i10`),
  KEY `i2_index` (`i2`),
  UNIQUE KEY `i3_index` (`__set_id`,`i3`),
  KEY `i4_index` (`i4`),
  KEY `i5_index` (`i5`),
  KEY `i6_index` (`i6`),
  KEY `i7_index` (`i7`),
  KEY `i8_index` (`i8`),
  KEY `i9_index` (`i9`),
  FULLTEXT INDEX `ft_index` (`i1`,`i2`) COMMENT 'parser "TokenBigramIgnoreBlankSplitSymbolAlphaDigit"'
){_ENGINE}"""
) == next(get_sql_for_creating_table(SampleModel))


def test_get_sql_for_create_part_of_table():
    class Part(PersistentModel, PartOfMixin['Container']):
        order: StringIndex

    class Container(PersistentModel):
        parts: List[Part] = Field(default=[])

    update_forward_refs(Part, locals())
    sqls = get_sql_for_creating_table(Part)

    assert (
        'CREATE TABLE IF NOT EXISTS `md_Part_pbase` (\n'
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
        'CREATE VIEW IF NOT EXISTS `md_Part` AS (\n'
        '  SELECT\n'
        '    JSON_EXTRACT(`md_Container`.`__json`, `md_Part_pbase`.`__json_path`) AS `__json`,\n'
        '    `md_Part_pbase`.`__row_id`,\n'
        '    `md_Part_pbase`.`__root_row_id`,\n'
        '    `md_Part_pbase`.`__container_row_id`,\n'
        '    `md_Part_pbase`.`order`,\n'
        '    `md_Container`.`__set_id`,\n'
        '    `md_Container`.`__valid_start`,\n'
        '    `md_Container`.`__valid_end`\n'
        '  FROM `md_Part_pbase`\n'
        '  JOIN `md_Container` ON `md_Container`.`__row_id` = `md_Part_pbase`.`__container_row_id`\n'
        ')'
    ) == next(sqls, None)

    assert None is next(sqls, None)


def test_get_sql_for_create_part_of_part_table():
    
    class PartOfPart(PersistentModel, PartOfMixin['Part']):
        order: StringIndex

    class Part(PersistentModel, PartOfMixin['Container']):
        order: StringIndex

    class Container(PersistentModel):
        parts: List[Part] = Field(default=[])

    update_forward_refs(Part, locals())
    update_forward_refs(PartOfPart, locals())

    sqls = get_sql_for_creating_table(PartOfPart)

    assert (
        'CREATE TABLE IF NOT EXISTS `md_PartOfPart_pbase` (\n'
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
        'CREATE VIEW IF NOT EXISTS `md_PartOfPart` AS (\n'
        '  SELECT\n'
        '    JSON_EXTRACT(`md_Part`.`__json`, `md_PartOfPart_pbase`.`__json_path`) AS `__json`,\n'
        '    `md_PartOfPart_pbase`.`__row_id`,\n'
        '    `md_PartOfPart_pbase`.`__root_row_id`,\n'
        '    `md_PartOfPart_pbase`.`__container_row_id`,\n'
        '    `md_PartOfPart_pbase`.`order`,\n'
        '    `md_Part`.`__set_id`,\n'
        '    `md_Part`.`__valid_start`,\n'
        '    `md_Part`.`__valid_end`\n'
        '  FROM `md_PartOfPart_pbase`\n'
        '  JOIN `md_Part` ON `md_Part`.`__row_id` = `md_PartOfPart_pbase`.`__container_row_id`\n'
        ')'
    ) == next(sqls, None)

    assert None is next(sqls, None)

def test_get_sql_for_create_table_raises():
    class Part(PersistentModel, PartOfMixin['Container'], VersionMixin):
        order: StringIndex

    class Container(PersistentModel, VersionMixin):
        parts: List[Part] = Field(default=[])

    class DatedModelWithoutIds(PersistentModel, DatedMixin):
        parts: List[Part] = Field(default=[])


    update_forward_refs(Part, locals())

    with pytest.raises(RuntimeError, match='VersionMixin is not support for PartOfMixin.'):
        list(get_sql_for_creating_table(Part))

    with pytest.raises(RuntimeError, match='identifying fields need for VersionMixin type.'):
        list(get_sql_for_creating_table(Container))

    with pytest.raises(RuntimeError, match='identifying fields need for DatedMixin type.'):
        list(get_sql_for_creating_table(DatedModelWithoutIds))

def test_get_sql_for_creating_external_index_table():
    class Target(PersistentModel):
        codes: StringArrayIndex
        ids: IntegerArrayIndex

    assert [
        join_line(
            "CREATE TABLE IF NOT EXISTS `md_Target` (",
            "  `__row_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,",
            "  `__set_id` BIGINT UNSIGNED NOT NULL DEFAULT 0,",
            "  `__json` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin CHECK (JSON_VALID(`__json`)),",
            "  `__valid_start` BIGINT,",
            "  `__valid_end` BIGINT DEFAULT 9223372036854775807,",
            "  `codes` TEXT AS (JSON_EXTRACT(`__json`, '$.codes[*]')) STORED,",
            "  `ids` TEXT AS (JSON_EXTRACT(`__json`, '$.ids[*]')) STORED,",
            "  KEY `codes_index` (`codes`),",
            "  KEY `ids_index` (`ids`)",
            ")"
        ),
        join_line(
            "CREATE TABLE IF NOT EXISTS `md_Target_codes` (",
            "  `__org_row_id` BIGINT,",
            "  `__root_row_id` BIGINT,",
            "  `codes` TEXT,",
            "  KEY `__org_row_id_index` (`__org_row_id`),",
            "  KEY `codes_index` (`codes`)",
            ")"
        ),
        join_line(
            "CREATE TABLE IF NOT EXISTS `md_Target_ids` (",
            "  `__org_row_id` BIGINT,",
            "  `__root_row_id` BIGINT,",
            "  `ids` BIGINT,",
            "  KEY `__org_row_id_index` (`__org_row_id`),",
            "  KEY `ids_index` (`ids`)",
            ")"
        )
    ] == list(get_sql_for_creating_table(Target))


def test_get_sql_for_creating_audit_version_table():
    assert [
        join_line(
            'CREATE TABLE IF NOT EXISTS `_version_info` (',
            '  `version` BIGINT AUTO_INCREMENT PRIMARY KEY,',
            '  `who` VARCHAR(80),',
            '  `where` VARCHAR(80),',
            '  `when` DATETIME(6),',
            '  `why` VARCHAR(256),',
            '  `tag` VARCHAR(80)',
            ')'
        ),
        join_line(
            'CREATE TABLE IF NOT EXISTS `_model_changes` (',
            '  `version` BIGINT,',
            '  `data_version` BIGINT,',
            '  `op` VARCHAR(32),',
            '  `table_name` VARCHAR(80),',
            '  `__set_id` BIGINT,',
            '  `__row_id` BIGINT,',
            '  `model_id` VARCHAR(256),',
            '  KEY `__row_id_index` (`__row_id`),',
            '  KEY `__version_index` (`version`)',
            ')'
        ),
    ] == list(get_sql_for_creating_version_info_table())


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
            },
            '_JSON_TABLE'
        ),
f"""JSON_TABLE(
  CONTAINER.`__json`,
  '$' COLUMNS (
    NESTED PATH '$.items' COLUMNS (
      `__part_order` FOR ORDINALITY,
      `name` TEXT PATH '$.name',
      `item` TEXT PATH '$.des.item',
      `id` TEXT PATH '$.id'
    )
  )
) AS _JSON_TABLE""",
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
            },
            '_JSON_TABLE'
        ),
f"""JSON_TABLE(
  CONTAINER.`__json`,
  '$' COLUMNS (
    NESTED PATH '$.items[*]' COLUMNS (
      `__part_order` FOR ORDINALITY,
      `id` TEXT PATH '$.id',
      NESTED PATH '$.data' COLUMNS (
        `name` TEXT PATH '$.name',
        `item` TEXT PATH '$.des.item'
      )
    )
  )
) AS _JSON_TABLE"""
    ),
    (
        (
            '$.items',
            True,
            {
            },
            '_JSON_TABLE'
        ),
f"""JSON_TABLE(
  CONTAINER.`__json`,
  '$' COLUMNS (
    NESTED PATH '$.items[*]' COLUMNS (
      `__part_order` FOR ORDINALITY
    )
  )
) AS _JSON_TABLE"""
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


def test_get_stored_fields():
    class MyModel(PersistentModel):
        order: StringIndex

    # Container에서 시작함으로 $.part.에 $.order가 된다.
    assert {'order': (('$.order',), StringIndex)} == get_stored_fields(MyModel)


def test_get_stored_fields_for_mro():
    class OrderModel(PersistentModel):
        _stored_fields: StoredFieldDefinitions = {
            'order': (('$.order_name',), StringIndex)
        }

    class BaseOrderModel(PersistentModel):
        _stored_fields: StoredFieldDefinitions = {
            'name': (('$.name',), StringIndex),
            'order': (('$.order',), StringIndex)
        }

    class DerivedModel1(BaseOrderModel, OrderModel):
        _stored_fields: StoredFieldDefinitions = {
            'hello': (('$.hello',), StringIndex),
        }
        pass

    # Container에서 시작함으로 $.part.에 $.order가 된다.
    assert {
        'hello': (('$.hello',), StringIndex),
        'order': (('$.order',), StringIndex), 
        'name': (('$.name',), StringIndex)
    } == get_stored_fields(DerivedModel1)

    class DerivedModel2(OrderModel, BaseOrderModel):
        pass

    assert {
        'order': (('$.order_name',), StringIndex), 
        'name': (('$.name',), StringIndex)
    } == get_stored_fields(DerivedModel2)


def test_get_stored_fields_of_parts():
    class Part(PersistentModel, PartOfMixin['Container']):
        order: StringIndex

    class Container(PersistentModel):
        parts: List['Part'] = Field(default=[])

    update_forward_refs(Part, locals())
    update_forward_refs(Container, locals())

    # Container에서 시작함으로 $.part.에 $.order가 된다.
    assert {'order': (('$.order',), StringIndex)} == get_stored_fields(Part)
    assert {} == get_stored_fields(Container)


def test_get_sql_for_upserting():
    class SimpleModel(PersistentModel):
        order: StringIndex
        
    sql = _get_sql_for_upserting_single_object(cast(Any, SimpleModel))

    assert join_line(
        "INSERT INTO md_SimpleModel",
        "(",
        "  `__json`,",
        "  `__set_id`,",
        "  `__valid_start`",
        ")",
        "VALUES",
        "(",
        "  %(__json)s,",
        "  %(__set_id)s,",
        "  @VERSION",
        ")",
        "RETURNING",
        "  `__set_id`,",
        "  `__row_id`,",
        "  'UPSERTED' as `op`,",
        "  'md_SimpleModel' as `table_name`,",
        "  CONCAT_WS(',', '') as `model_id`,",
        "  @VERSION as `data_version`"
    ) == sql


def test_get_sql_for_upserting_versioning():
    class VersionModel(PersistentModel, VersionMixin):
        id: Annotated[UuidStr, MetaIdentifyingField()]
        order: StringIndex
        
    sql = _get_sql_for_upserting_single_object(cast(Any, VersionModel))

    assert join_line(
        "SELECT MIN(`__squashed_from`)",
        "INTO @SQUASHED_FROM",
        "FROM md_VersionModel",
        "WHERE",
        "  `__set_id` = %(__set_id)s",
        "  AND `id` = %(id)s",
        "  AND `__valid_start` <= @VERSION",
        "  AND @VERSION < `__valid_end`",
        ";",
        "UPDATE md_VersionModel",
        "SET `__valid_end` = @VERSION",
        "WHERE",
        "  `__set_id` = %(__set_id)s",
        "  AND `id` = %(id)s",
        "  AND `__valid_start` <= @VERSION",
        "  AND @VERSION < `__valid_end`",
        ";",
        "INSERT INTO md_VersionModel",
        "(",
        "  `__json`,",
        "  `__valid_start`,",
        "  `__squashed_from`,",
        "  `__set_id`,",
        "  `id`",
        ")",
        "VALUES",
        "(",
        "  %(__json)s,",
        "  @VERSION,",
        "  IFNULL(@SQUASHED_FROM, @VERSION),",
        "  %(__set_id)s,",
        "  %(id)s",
        ")",
        "RETURNING",
        "  `__set_id`,",
        "  `__row_id`,",
        "  CONCAT_WS(',', `id`,`__valid_start`) as `model_id`,",
        "  'INSERTED' as `op`,",
        "  'md_VersionModel' as `table_name`,",
        "  @VERSION as `data_version`"
    ) == sql


def test_get_sql_for_upserting_dated():
    class DatedModel(PersistentModel, DatedMixin):
        id: Annotated[UuidStr, MetaIdentifyingField()]
        
    sql = _get_sql_for_upserting_single_object(cast(Any, DatedModel))

    assert join_line(
        "IF ( SELECT 1 = 1 FROM md_DatedModel WHERE   `__set_id` = %(__set_id)s",
        "  AND `applied_at` = %(applied_at)s",
        "  AND `id` = %(id)s) THEN",
        "  UPDATE md_DatedModel",
        "  SET",
        "    `__json` = %(__json)s,",
        "    `__valid_start` = @VERSION",
        "  WHERE",
        "      `__set_id` = %(__set_id)s",
        "      AND `applied_at` = %(applied_at)s",
        "      AND `id` = %(id)s",
        "  ;",
        "  SELECT",
        "    `__set_id`,",
        "    `__row_id`,",
        "    'INSERTED' as `op`,",
        "    'md_DatedModel' as `table_name`,",
        "    CONCAT_WS(',', `applied_at`,`id`) as `model_id`,",
        "    @VERSION as `data_version`",
        "  FROM md_DatedModel",
        "  WHERE",
        "      `__set_id` = %(__set_id)s",
        "      AND `applied_at` = %(applied_at)s",
        "      AND `id` = %(id)s",
        "  ;",
        "ELSE",
        "  INSERT INTO md_DatedModel",
        "  (",
        "    `__json`,",
        "    `__set_id`,",
        "    `applied_at`,",
        "    `id`,",
        "    `__valid_start`",
        "  )",
        "  VALUES",
        "  (",
        "    %(__json)s,",
        "    %(__set_id)s,",
        "    %(applied_at)s,",
        "    %(id)s,",
        "    @VERSION",
        "  )",
        "  RETURNING",
        "    `__set_id`,",
        "    `__row_id`,",
        "    'INSERTED' as `op`,",
        "    'md_DatedModel' as `table_name`,",
        "    CONCAT_WS(',', `applied_at`,`id`) as `model_id`,",
        "    @VERSION as `data_version`",
        "  ;",
        "END IF"
    ) == sql


def test_get_sql_for_upserting_versioned_dated():
    class VersionDateModel(PersistentModel, VersionMixin, DatedMixin):
        id: Annotated[UuidStr, MetaIdentifyingField()]
        
    sql = _get_sql_for_upserting_single_object(cast(Any, VersionDateModel))

    assert join_line(
        "SELECT MIN(`__squashed_from`)",
        "INTO @SQUASHED_FROM",
        "FROM md_VersionDateModel",
        "WHERE",
        "  `__set_id` = %(__set_id)s",
        "  AND `applied_at` = %(applied_at)s",
        "  AND `id` = %(id)s",
        "  AND `__valid_start` <= @VERSION",
        "  AND @VERSION < `__valid_end`",
        ";",
        "UPDATE md_VersionDateModel",
        "SET `__valid_end` = @VERSION",
        "WHERE",
        "  `__set_id` = %(__set_id)s",
        "  AND `applied_at` = %(applied_at)s",
        "  AND `id` = %(id)s",
        "  AND `__valid_start` <= @VERSION",
        "  AND @VERSION < `__valid_end`",
        ";",
        "INSERT INTO md_VersionDateModel",
        "(",
        "  `__json`,",
        "  `__valid_start`,",
        "  `__squashed_from`,",
        "  `__set_id`,",
        "  `applied_at`,",
        "  `id`",
        ")",
        "VALUES",
        "(",
        "  %(__json)s,",
        "  @VERSION,",
        "  IFNULL(@SQUASHED_FROM, @VERSION),",
        "  %(__set_id)s,",
        "  %(applied_at)s,",
        "  %(id)s",
        ")",
        "RETURNING",
        "  `__set_id`,",
        "  `__row_id`,",
        "  CONCAT_WS(',', `applied_at`,`id`,`__valid_start`) as `model_id`,",
        "  'INSERTED' as `op`,",
        "  'md_VersionDateModel' as `table_name`,",
        "  @VERSION as `data_version`"
   ) == sql



def test_get_stored_fields_of_single_part():
    class SinglePart(PersistentModel, PartOfMixin['ContainerForSingle']):
        order: StringIndex

    class ContainerForSingle(PersistentModel):
        part: SinglePart 

    update_forward_refs(SinglePart, locals())

    # Container에서 시작함으로 $.part.에 $.order가 된다.
    assert {'order': (('$.order',), StringIndex)} == get_stored_fields(SinglePart)


def test_get_stored_fields_raise_exception():
    class PathNotStartWithDollar(PersistentModel):
        _stored_fields: StoredFieldDefinitions = {
            'order': (('order_name',), StringIndex)
        }

    with pytest.raises(RuntimeError, match='.*path must start with \\$.*'):
        get_stored_fields(PathNotStartWithDollar)


def test_get_sql_for_upserting_parts_table():
    class Part(PersistentModel, PartOfMixin['Container']):
        order: StringIndex
        codes: StringArrayIndex

    class Container(PersistentModel):
        parts: List[Part] = Field(default=[])

    update_forward_refs(Part, locals())
    sqls = get_sql_for_upserting_parts(Container)

    assert len(sqls[Part]) == 2
    assert join_line(
        "DELETE FROM md_Part_pbase",
        "WHERE `__root_row_id` = %(__root_row_id)s"
    ) == sqls[Part][0]

    assert join_line(
        "INSERT INTO md_Part_pbase",
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
        "    md_Container as CONTAINER,",
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


def test_get_sql_for_upserting_parts_table_with_container_fields():
    class Part(PersistentModel, PartOfMixin['Container']):
        _stored_fields: ClassVar[StoredFieldDefinitions]  = {
            '_container_name': (('..', '$.name'), StringIndex)
        }

    class Container(PersistentModel):
        name: StringIndex
        parts: List[Part] = Field(default=[])

    update_forward_refs(Part, locals())
    sqls = get_sql_for_upserting_parts(Container)

    assert len(sqls[Part]) == 2
    assert join_line(
        "DELETE FROM md_Part_pbase",
        "WHERE `__root_row_id` = %(__root_row_id)s"
    ) == sqls[Part][0]

    assert join_line(
        "INSERT INTO md_Part_pbase",
        "(",
        "  `__root_row_id`,",
        "  `__container_row_id`,",
        "  `__json_path`,",
        "  `_container_name`",
        ")",
        "SELECT",
        "  `__root_row_id`,",
        "  `__container_row_id`,",
        "  `__json_path`,",
        "  `_container_name`",
        "FROM (",  
        "  SELECT",
        "    `__part_order`,",
        "    CONTAINER.`__row_id` as `__root_row_id`,",
        "    CONTAINER.`__row_id` as `__container_row_id`,",
        "    CONCAT('$.parts[', `__part_order` - 1, ']') as `__json_path`,",
        "    JSON_VALUE(`CONTAINER`.`__json`, '$.name') as `_container_name`",
        "  FROM",
        "    md_Container as CONTAINER,",
        "    JSON_TABLE(",
        "      CONTAINER.`__json`,",
        "      '$' COLUMNS (",
        "        NESTED PATH '$.parts[*]' COLUMNS (",
        "          `__part_order` FOR ORDINALITY",
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



def test_get_sql_for_upserting_external_index_table():
    class Part(PersistentModel, PartOfMixin['Container']):
        _stored_fields: StoredFieldDefinitions = {
            '_names': (('..', '$.names', '$'), StringArrayIndex)
        }

        order: StringIndex
        codes: StringArrayIndex

    class Container(PersistentModel):
        names: List[str]
        parts: List[Part] = Field(default=[])

    update_forward_refs(Part, locals())
    sqls = list(get_sql_for_upserting_external_index(Part))

    assert len(sqls) == 4
    assert join_line(
        "DELETE FROM md_Part_codes",
        "WHERE `__root_row_id` = %(__root_row_id)s"
    ) == sqls[0]

    assert join_line(
        "INSERT INTO md_Part_codes",
        "(",
        "  `__root_row_id`,",
        "  `__org_row_id`,",
        "  `codes`",
        ")",
        "SELECT",
        "  `__ORG`.`__root_row_id`,",
        "  `__ORG`.`__row_id`,",
        "  `__EXT_JSON_TABLE`.`codes`",
        "FROM",  
        "  md_Part AS __ORG,",
        "  JSON_TABLE(",
        "    `__ORG`.`__json`,",
        "    '$' COLUMNS (",
        "      NESTED PATH '$.codes[*]' COLUMNS (",
        "        `__part_order` FOR ORDINALITY,",
        "        `codes` TEXT PATH '$'",
        "      )",    
        "    )",    
        "  ) AS __EXT_JSON_TABLE",
        "WHERE",
        "  `__ORG`.`__root_row_id` = %(__root_row_id)s",
    ) == sqls[1]

    assert join_line(
        "DELETE FROM md_Part__names",
        "WHERE `__root_row_id` = %(__root_row_id)s"
    ) == sqls[2]

    assert join_line(
        "INSERT INTO md_Part__names",
        "(",
        "  `__root_row_id`,",
        "  `__org_row_id`,",
        "  `_names`",
        ")",
        "SELECT",
        "  `__ORG`.`__root_row_id`,",
        "  `__ORG`.`__row_id`,",
        "  `__EXT_JSON_TABLE`.`_names`",
        "FROM",  
        "  md_Part AS __ORG,",
        "  JSON_TABLE(",
        "    `__ORG`.`_names`,",
        "    '$[*]' COLUMNS (",
        "      `_names` TEXT PATH '$'",
        "    )",    
        "  ) AS __EXT_JSON_TABLE",
        "WHERE",
        "  `__ORG`.`__root_row_id` = %(__root_row_id)s",
    ) == sqls[3]


def test_get_sql_for_upserting_parts_table_throws_if_invalid_path():
    class InvalidPath(PersistentModel, PartOfMixin['MyModel']):
        _stored_fields: ClassVar[StoredFieldDefinitions] = {
            '_invalid': (('..',), StringIndex)
        }

    class MyModel(PersistentModel):
        paths: InvalidPath

    update_forward_refs(InvalidPath, locals())

    with pytest.raises(RuntimeError, match='.*2.*items'):
        get_sql_for_upserting_parts(MyModel)


def test_get_query_and_args_for_reading_for_matching():
    class MyModel(PersistentModel):
        order: FullTextSearchedString
        name: FullTextSearchedString

    sql, args = get_query_and_args_for_reading(MyModel, ('order',), {'': ('match', '+FAST')}, set_id=1)

    assert join_line(
        "SELECT",
        "  `_MAIN`.`order`",
        "FROM",
        "  (",
        "    SELECT",
        "      `__row_id`,",
        "      `__relevance`",
        "    FROM",    
        "    (",
        "      SELECT",
        "        `__row_id`,",
        "        `__relevance` AS `__relevance`",
        "      FROM",
        "        (",
        "          SELECT",
        "            `__ORG`.`__row_id`,",
        "            MATCH (`__ORG`.`order`,`__ORG`.`name`) AGAINST (%(order_name)s IN BOOLEAN MODE) as `__relevance`",
        "          FROM",
        "            md_MyModel AS __ORG",
        "          WHERE",
        "            `__ORG`.`__set_id` = %(__set_id)s",
        "            AND `__ORG`.`__valid_start` <= %(version)s",
        "            AND `__ORG`.`__valid_end` > %(version)s",
        "        )",
        "        AS _BASE_CORE",
        "    )",
        "    AS FOR_ORDERING",
        "    WHERE",
        "      `__relevance`",
        "    ORDER BY",
        "      `__relevance` DESC",
        "    LIMIT 9223372036854775807",
        "  )",
        "  AS _BASE",
        "  JOIN md_MyModel AS _MAIN ON `_BASE`.`__row_id` = `_MAIN`.`__row_id`"
    ) == sql

    assert {
        "order_name": "+FAST",
        "version": 0,
        "current_date": None,
        "__set_id": 1
    } == args


def test_get_query_and_args_for_reading_for_order_by():
    class MyModel(PersistentModel):
        order: FullTextSearchedString
        name: FullTextSearchedString

    sql, _ = get_query_and_args_for_reading(MyModel, ('order',), {}, 0, order_by=('order desc', 'name'))

    print(sql)

    assert join_line(
        "SELECT",
        "  `_BASE`.`order`",
        "FROM",
        "  (",
        "    SELECT",
        "      `__row_id`,",
        "      `order`,",
        "      `name`",
        "    FROM",
        "    (",
        "      SELECT",
        "        `__row_id`,",
        "        `order`,",
        "        `name`",
        "      FROM",
        "        (",
        "          SELECT",
        "            `__ORG`.`__row_id`,",
        "            `__ORG`.`order`,",
        "            `__ORG`.`name`",
        "          FROM",
        "            md_MyModel AS __ORG",
        "          WHERE",
        "            `__ORG`.`__set_id` = %(__set_id)s",
        "            AND `__ORG`.`__valid_start` <= %(version)s",
        "            AND `__ORG`.`__valid_end` > %(version)s",
        "        )",
        "        AS _BASE_CORE",
        "    )",
        "    AS FOR_ORDERING",
        "    ORDER BY",
        "      `order` desc,",
        "      `name`",
        "    LIMIT 9223372036854775807",
        "  )",
        "  AS _BASE"
    ) == sql


def test_get_query_and_args_for_reading_for_dated():
    class MyModel(PersistentModel, DatedMixin):
        id: Annotated[UuidStr, MetaIdentifyingField()]
        order: FullTextSearchedString
        name: FullTextSearchedString

    sql, _ = get_query_and_args_for_reading(
        MyModel, ('name',), {'name': ('=', 'ab')}, 0, current=date.today())

    print(sql)

    assert join_line(
        "SELECT",
        "  `_BASE`.`name`",
        "FROM",
        "  (",
        "    SELECT",
        "      `__row_id`,",
        "      `name`",
        "    FROM",
        "      (",
        "        SELECT",
        "          `__ORG`.`__row_id`,",
        "          `__ORG`.`name`",
        "        FROM",
        "          (",
        "            SELECT",
        "              *,",
        "              `applied_at` as `__applied_start`,",
        "              IFNULL(LEAD(`applied_at`) over (PARTITION BY `id` ORDER BY `applied_at`), "
        "'9999-12-31') as `__applied_end`",
        "            FROM",
        "            md_MyModel",
        "            WHERE",
        "              `__set_id` = %(__set_id)s",
        "          )",
        "          AS __ORG",
        "        WHERE",
        "          `__ORG`.`name` = %(name)s",
        "          AND `__ORG`.`__applied_start` <= %(current_date)s",
        "          AND `__ORG`.`__applied_end` > %(current_date)s",
        "      )",
        "      AS _BASE_CORE",
        "  )",
        "  AS _BASE"
    ) == sql


def test_build_query_for_core_table():
    class Model(PersistentModel):
        codes: StringArrayIndex
        name: FullTextSearchedString
        description: FullTextSearchedString

    query, fields = _build_query_and_fields_for_core_table('', Model, 
        ['description'], 
        (('codes', '='), ('name', '!=')),
        tuple(), False
    )

    assert join_line(
        'SELECT',
        '  `__ORG`.`__row_id`,',
        '  `__ORG`.`description`,',
        '  `__ORG`.`codes`,',
        '  `__ORG`.`name`',
        'FROM',
        '  md_Model AS __ORG',
        'WHERE',
        '  `__ORG`.`codes` = %(codes)s',
        '  AND `__ORG`.`name` != %(name)s',
        '  AND `__ORG`.`__set_id` = %(__set_id)s',
        '  AND `__ORG`.`__valid_start` <= %(version)s',
        '  AND `__ORG`.`__valid_end` > %(version)s',
    ) == query
    assert ('__row_id', 'description', 'codes', 'name') == fields


def test_build_query_for_core_table_for_unwind():
    class Model(PersistentModel):
        codes: StringArrayIndex
        name: FullTextSearchedString
        description: FullTextSearchedString

    query, fields = _build_query_and_fields_for_core_table('ns', Model, 
        ['description'],
        (('codes', '='), ('name', '!=')),
        ('codes',), False
    )

    assert join_line(
        'SELECT',
        '  `__ORG`.`__row_id` AS `ns.__row_id`,',
        '  `__ORG`.`description` AS `ns.description`,',
        '  `__ORG`.`name` AS `ns.name`,',
        '  `__UNWIND_codes`.`codes` AS `ns.codes`',
        'FROM',
        '  md_Model AS __ORG',
        '  LEFT JOIN md_Model_codes AS __UNWIND_codes ON `__ORG`.`__row_id` = `__UNWIND_codes`.`__org_row_id`',
        'WHERE',
        '  `__UNWIND_codes`.`codes` = %(ns_codes)s',
        '  AND `__ORG`.`name` != %(ns_name)s',
        '  AND `__ORG`.`__set_id` = %(__set_id)s',
        '  AND `__ORG`.`__valid_start` <= %(version)s',
        '  AND `__ORG`.`__valid_end` > %(version)s',
    ) == query

    assert ('ns.__row_id', 'ns.description', 'ns.name', 'ns.codes') == fields


def test_build_query_for_core_table_for_match():
    class Model(PersistentModel):
        codes: StringArrayIndex
        name: FullTextSearchedString
        description: FullTextSearchedString
    
    query, fields = _build_query_and_fields_for_core_table('ns', Model, 
        [],
        (('codes', '='), ('name,description', 'match')),
        ('codes',), False
    )

    assert join_line(
        'SELECT',
        '  `__ORG`.`__row_id` AS `ns.__row_id`,',
        '  `__UNWIND_codes`.`codes` AS `ns.codes`,',
        '  MATCH (`__ORG`.`name`,`__ORG`.`description`) AGAINST (%(name_description)s IN BOOLEAN MODE) as `ns.__relevance`',
        'FROM',
        '  md_Model AS __ORG',
        '  LEFT JOIN md_Model_codes AS __UNWIND_codes ON `__ORG`.`__row_id` = `__UNWIND_codes`.`__org_row_id`',
        'WHERE',
        '  `__UNWIND_codes`.`codes` = %(ns_codes)s',
        '  AND `__ORG`.`__set_id` = %(__set_id)s',
        '  AND `__ORG`.`__valid_start` <= %(version)s',
        '  AND `__ORG`.`__valid_end` > %(version)s',
    ) == query

    assert ('ns.__row_id', 'ns.' + _RELEVANCE_FIELD, 'ns.codes') == fields


def test_build_query_for_core_table_for_multiple_match():
    class Model(PersistentModel):
        codes: StringArrayIndex
        name: FullTextSearchedString
        description: FullTextSearchedString
 
    query, fields = _build_query_and_fields_for_core_table('', Model, 
        [],
        (('name', 'match'), ('description', 'match',)),
        tuple(), False
    )

    assert join_line(
        'SELECT',
        '  `__ORG`.`__row_id`,',
        '  MATCH (`__ORG`.`name`) AGAINST (%(name)s IN BOOLEAN MODE) + MATCH (`__ORG`.`description`) AGAINST (%(description)s IN BOOLEAN MODE) as `__relevance`',
        'FROM',
        '  md_Model AS __ORG',
        'WHERE',
        '  `__ORG`.`__set_id` = %(__set_id)s',
        '  AND `__ORG`.`__valid_start` <= %(version)s',
        '  AND `__ORG`.`__valid_end` > %(version)s'
    ) == query

    assert ('__row_id', _RELEVANCE_FIELD) == fields


def test_extract_fields():
    fields = ('test', 'prefix.field1', 'prefix.prefix2.field', 'prefix.field2')

    assert ('test',) == _extract_fields(fields, '')
    assert ('field1', 'field2') == _extract_fields(fields, 'prefix')
    assert ('field',) == _extract_fields(fields, 'prefix.prefix2')


def test_build_join():
    class ReferencedByName(PersistentModel):
        name: StringIndex

    NameReference = Annotated[str, 
                              MetaReferenceField(target_type=ReferencedByName, 
                                                 target_field='name')]

    class ReferencedByCode(PersistentModel):
        code: StringIndex
        name: NameReference

    CodeReference = Annotated[str, 
                              MetaReferenceField(
                                  target_type=ReferencedByCode, target_field='code')]
    class ReferencedById(PersistentModel):
        id: IntIndex


    IdReference = Annotated[str, 
                            MetaReferenceField(
                                target_type=ReferencedById, target_field='id')]

    class StartModel(PersistentModel):
        code: CodeReference
        id: IdReference
        name: StringIndex

    assert [
        ('', StartModel),
        ('code', ReferencedByCode)
    ] == list(_build_namespace_types(StartModel, {}, {'code'}))

    assert [
        ('', StartModel),
        ('code', ReferencedByCode),
        ('code.name', ReferencedByName),
        ('id', ReferencedById)
    ] == list(_build_namespace_types(StartModel, {}, {'code.name', 'id'}))

    assert [
        ('', ReferencedById),
        ('start', StartModel),
        ('start.code', ReferencedByCode),
        ('start.code.name', ReferencedByName)
    ] == list(_build_namespace_types(ReferencedById, {'start': StartModel}, {'start.code.name'}))


def test_find_join_keys():
    class ReferencedByName(PersistentModel):
        name_ref: StringIndex

    NameReference = Annotated[str, 
                              MetaReferenceField(target_type=ReferencedByName, 
                                                 target_field='name_ref')]
    class ReferencedByCode(PersistentModel):
        code: StringIndex
        name: NameReference

    CodeReference = Annotated[str, 
                              MetaReferenceField(
                                  target_type=ReferencedByCode, target_field='code')]

    class ReferencedById(PersistentModel):
        id: IntIndex

    IdReference = Annotated[str, 
                            MetaReferenceField(
                                target_type=ReferencedById, target_field='id')]

    class StartModel(PersistentModel):
        code: CodeReference
        id: IdReference
        name: StringIndex

    assert {
    } == _find_join_keys((('',StartModel),))

    assert {
        (StartModel, ReferencedByCode) : ('code', 'code')
    } == _find_join_keys((('',StartModel), ('code',ReferencedByCode)))

    assert {
        (ReferencedById, StartModel) : ('id', 'id'),
        (StartModel, ReferencedByCode) : ('code', 'code'),
        (ReferencedByCode, ReferencedByName) : ('name', 'name_ref')
    } == _find_join_keys((
        ('', ReferencedById),
        ('start', StartModel), 
        ('start.code', ReferencedByCode), 
        ('start.code.name', ReferencedByName)
    ))

    with pytest.raises(RuntimeError):
        _find_join_keys((('',StartModel),('name', ReferencedByName)))


def test_get_query_and_args_for_purging():    
    class SimpleBaseModel(PersistentModel):
        id: Annotated[UuidStr, MetaIdentifyingField()]

    sqls = get_query_and_args_for_purging(
        SimpleBaseModel, {'id':('=', '@')}, 0)

    assert join_line(
        "DELETE FROM md_SimpleBaseModel",
        "WHERE",
        "  `id` = %(id)s",
        "  AND `__set_id` = %(__set_id)s",
        "RETURNING",
        "  __row_id,",
        "  __set_id,",
        "  'PURGED' as `op`,",
        "  'md_SimpleBaseModel' as `table_name`,",
        "  CONCAT_WS(',', `id`) as `model_id`,",
        "  NULL as `data_version`"
    ) == sqls[0]

    assert {'id': '@', '__set_id':0} == sqls[1]


def test_get_query_and_args_for_deleting():    
    class SimpleBaseModel(PersistentModel):
        id: Annotated[UuidStr, MetaIdentifyingField()]

    sqls = get_query_and_args_for_deleting(
        SimpleBaseModel, {'id': ('=', '@')}, 0)

    assert join_line(
        "UPDATE md_SimpleBaseModel",
        "SET",
        "  `__valid_end` = @VERSION",
        "WHERE",
        "  `id` = %(id)s",
        "  AND `__set_id` = %(__set_id)s",
        ";",
        "SELECT",
        "  __row_id,",
        "  __set_id,",
        "  'DELETED' as `op`,", 
        "  'md_SimpleBaseModel' as `table_name`,",
        "  CONCAT_WS(',', `id`) as `model_id`,",
        "  NULL as `data_version`",
        "FROM md_SimpleBaseModel",
        "WHERE",
        "  `id` = %(id)s",
        "  AND `__set_id` = %(__set_id)s",
    ) == sqls[0]
    assert {'id': '@', '__set_id':0} == sqls[1]


def test_get_query_for_adjust_seq():
    class CodedModel(PersistentModel):
        code: SequenceStr
        codes: List[SequenceStr]

    sql = '\n'.join(get_query_for_adjust_seq(CodedModel))

    assert join_line(
        "SET @MAX_VALUE = (SELECT",
        "    CAST(REPLACE(MAX(`code`), 'N', '') as INTEGER)",
        "FROM md_CodedModel);",
        "EXECUTE IMMEDIATE CONCAT('SELECT SETVAL(`sq_CodedModel_code`, ', @MAX_VALUE, ')')",
        "SET @MAX_VALUE = (SELECT",
        "    CAST(REPLACE(MAX(`codes`), 'N', '') as INTEGER)",
        "FROM md_CodedModel);",
        "EXECUTE IMMEDIATE CONCAT('SELECT SETVAL(`sq_CodedModel_codes`, ', @MAX_VALUE, ')')",
    ) == sql


def test_get_sql_for_copying_objects():
    class MyModel(PersistentModel):
        code: Annotated[SequenceStr, MetaIdentifyingField()]
        message: str

    statements = _get_sql_for_copying_objects(MyModel)

    assert join_line(
        "IF ( SELECT 1 = 1 FROM md_MyModel WHERE",
        "  `code` = %(code)s",
        "  AND `__set_id` = %(dest_id)s) THEN",
        "  REPLACE INTO md_MyModel",
        "  (",
        "    `__set_id`,",
        "    `code`,",
        "    `__json`,",
        "    `__valid_start`",
        "  )",
        "  SELECT",
        "    *",
        "  FROM",
        "  (",
        "    SELECT",
        "      `DEST`.`__set_id`,",
        "      `code`,",
        "      `SRC`.`__json`,",
        "      IF(`SRC`.`__valid_start` - IFNULL(`DEST`.`__valid_start`, 0) < 0, @VERSION, `SRC`.`__valid_start`)",
        "    FROM",
        "      (",
        "        SELECT",
        "          `__json`,",
        "          `__valid_start`,",
        "          `code`",
        "        FROM md_MyModel",
        "        WHERE",
        "          `code` = %(code)s",
        "          AND `__set_id` = %(src_id)s",
        "          AND `__valid_start` <= @VERSION",
        "          AND `__valid_end` > @VERSION",
        "      )",
        "      AS SRC",
        "      LEFT JOIN",
        "      (",
        "        SELECT",
        "          `__set_id`,",
        "          `__valid_start`,",
        "          `code`",
        "        FROM md_MyModel",
        "        WHERE",
        "          `__set_id` = %(dest_id)s",
        "          AND `__valid_start` <= @VERSION",
        "          AND `__valid_end` > @VERSION",
        "      )",
        "      AS DEST",
        "      USING (`code`)",
        "    WHERE `SRC`.`__valid_start` != IFNULL(`DEST`.`__valid_start`, 0)",
        "  )",
        "  AS _T",
        "  RETURNING",
        "    `__set_id`,",
        "    `__row_id`,",
        "    `__valid_start` as `data_version`,",
        "    'INSERTED:MERGE_SET' as `op`,",
        "    'md_MyModel' as `table_name`,",
        "    CONCAT_WS(',', `code`) as `model_id`",
        "  ;",
        "  ELSE",
        "    INSERT INTO md_MyModel",
        "    (",
        "      `__json`,",
        "      `__set_id`,",
        "      `code`,",
        "      `__valid_start`",
        "    )",
        "    SELECT",
        "      `__json`,",
        "      %(dest_id)s,",
        "      `code`,",
        "      `__valid_start`",
        "    FROM md_MyModel",
        "    WHERE",
        "      `code` = %(code)s",
        "      AND `__set_id` = %(src_id)s",
        "    RETURNING",
        "      `__set_id`,",
        "      `__row_id`,",
        "      `__valid_start` as `data_version`,",
        "      'INSERTED:MERGE_SET' as `op`,",
        "      'md_MyModel' as `table_name`,",
        "      CONCAT_WS(',', `code`) as `model_id`",
        "    ;",
        "  END IF"
    ) == statements

    class MyVersionModel(PersistentModel, VersionMixin):
        code: Annotated[SequenceStr, MetaIdentifyingField()]
        message: str

    statements = _get_sql_for_copying_objects(MyVersionModel)

    assert join_line(
        "INSERT INTO md_MyVersionModel",
        "(",
        "  __set_id,",
        "  __json,",
        "  __valid_start,",
        "  __squashed_from,",
        "  `code`",
        ")",
        "SELECT",
        "  %(dest_id)s,",
        "  `SRC`.`__json`,",
        "  IF(`SRC`.`__valid_start` - IFNULL(`DEST`.`__valid_start`, 0) < 0, @VERSION, `SRC`.`__valid_start`),",
        "  `DEST`.`__squashed_from`,",
        "  `code`",
        "FROM",
        "  (",
        "    SELECT",
        "      `__json`,",
        "      `__valid_start`,",
        "      `code`",
        "    FROM md_MyVersionModel",
        "    WHERE",
        "      `code` = %(code)s",
        "      AND `__set_id` = %(src_id)s",
        "      AND `__valid_start` <= @VERSION",
        "      AND `__valid_end` > @VERSION",
        "  )",
        "  AS SRC",
        "  LEFT JOIN",
        "  (",
        "    SELECT",
        "      `__set_id`,",
        "      `__valid_start`,",
        "      `__squashed_from`,",
        "      `code`",
        "    FROM md_MyVersionModel",
        "    WHERE",
        "      `__set_id` = %(dest_id)s",
        "      AND `__valid_start` <= @VERSION",
        "      AND `__valid_end` > @VERSION",
        "  )",
        "  AS DEST",
        "  USING (`code`)",
        "WHERE `SRC`.`__valid_start` != IFNULL(`DEST`.`__valid_start`, 0)",
        "RETURNING",
        "  `__row_id`,",
        "  `__set_id`,",
        "  CONCAT_WS(',', `code`,`__valid_start`,`__valid_end`) as `model_id`,",
        "  'INSERTED:MERGE_SET' as `op`,",
        "  'md_MyVersionModel' as `table_name`,",
        "  `__valid_start` as `data_version`",
        ";",
        "UPDATE md_MyVersionModel JOIN (",
        "  SELECT",
        "    `__row_id`,",
        "    IFNULL(LEAD(`__valid_start`) over (PARTITION BY `code` ORDER BY `__valid_start`), 9223372036854775807) as __NEW_VALID_END",
        "  FROM md_MyVersionModel",
        "  WHERE `__valid_end` = 9223372036854775807",
        "  AND `__set_id` = %(dest_id)s",
        ") as SRC USING (`__row_id`)",
        "SET `__valid_end` = __NEW_VALID_END",
        "WHERE",
        "  `code` = %(code)s",
        "  AND `__set_id` = %(dest_id)s",
        ";"
    ) == statements
     