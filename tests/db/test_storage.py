# for clearing testing database.
# mariadb -h localhost -P 33069 -p -u root -N -B -e 'select CONCAT("drop database `", schema_name, "`;") as name from information_schema.schemata where schema_name like "TEST_%";'
#
from dataclasses import asdict
from datetime import date
from typing import  List, Tuple, cast, Type
import pytest

from ormdantic.database.storage import (
    delete_objects, get_model_changes_of_version, get_version_info, query_records, squash_objects, upsert_objects, find_object, 
    find_objects, build_where, load_object
)

from ormdantic.schema import PersistentModel
from ormdantic.database.verinfo import VersionInfo
from ormdantic.schema.base import (
    DatedMixin, FullTextSearchedStringIndex, PartOfMixin, SchemaBaseModel, StringArrayIndex, VersionMixin, 
    get_identifer_of, update_forward_refs, IdentifiedModel, StrId, DateId,
    StoredFieldDefinitions
)

from .tools import (
    use_temp_database_pool_with_model, 
)

class ContainerModel(IdentifiedModel):
    name: FullTextSearchedStringIndex
    parts: List['PartModel']
    

class PartModel(PersistentModel, PartOfMixin[ContainerModel]):
    _stored_fields: StoredFieldDefinitions = {
        '_container_name': (('..', '$.name'), FullTextSearchedStringIndex)
    }

    name: FullTextSearchedStringIndex
    parts: Tuple['SubPartModel', ...]
    codes: List[str]


class SubPartModel(PersistentModel, PartOfMixin[PartModel]):
    _stored_fields: StoredFieldDefinitions = {
        '_part_codes': (('..', '$.codes'), StringArrayIndex)
    }

    name: FullTextSearchedStringIndex
    codes: StringArrayIndex

update_forward_refs(ContainerModel, locals())
update_forward_refs(PartModel, locals())


model = ContainerModel(id=StrId('@'), 
                        version='0.1.0',
                        name=FullTextSearchedStringIndex('sample'),
                        parts=[
                            PartModel(
                                name=FullTextSearchedStringIndex('part1'), 
                                parts=(
                                    SubPartModel(
                                        name=FullTextSearchedStringIndex('part1-sub1'),
                                        codes=StringArrayIndex(['part1-sub1-code1'])
                                    ),
                                ),
                                codes = ['part1-code1', 'part1-code2']
                            ),
                            PartModel(
                                name=FullTextSearchedStringIndex('part2'), 
                                parts=(
                                    SubPartModel(
                                        name=FullTextSearchedStringIndex('part2-sub1'),
                                        codes=StringArrayIndex(['part2-sub1-code1', 'part2-sub1-code2'])
                                    ),
                                    SubPartModel(
                                        name=FullTextSearchedStringIndex('part2-sub2'),
                                        codes=StringArrayIndex([])
                                    )
                                ),
                                codes = ['part2-code1', 'part2-code2']
                            )
                        ])


@pytest.fixture(scope='module')
def pool_and_model():
    with use_temp_database_pool_with_model(ContainerModel) as pool:
        upserted = upsert_objects(pool, model)

        yield pool, upserted


def test_upsert_objects(pool_and_model):
    pool, upserted = pool_and_model

    # multiple same object does not affect the database.
    upserted = upsert_objects(pool, upserted)

    where = build_where(get_identifer_of(upserted))

    found = find_object(pool, ContainerModel, where)

    assert upserted == found


def test_upsert_objects_with_exception(pool_and_model):
    pool, _ = pool_and_model

    with pytest.raises(RuntimeError, match='PartOfMixin could not be saved directly.*'):
        upsert_objects(pool, model.parts[0])


def test_delete_objects():
    with use_temp_database_pool_with_model(ContainerModel) as pool:
        upserted = upsert_objects(pool, model)

        found = find_objects(pool, PartModel, tuple())
        assert found 

        found = find_objects(pool, SubPartModel, tuple())
        assert found 

        # multiple same object does not affect the database.
        where = build_where(get_identifer_of(upserted))

        found = delete_objects(pool, ContainerModel, where)

        found = find_object(pool, ContainerModel, where)

        assert found is None

        found = list(find_objects(pool, PartModel, tuple()))
        assert not found 

        found = list(find_objects(pool, SubPartModel, tuple()))
        assert not found

        # check empty external table
        with pool.open_cursor() as cursor:
            cursor.execute('SELECT * FROM md_SubPartModel_codes')
            assert tuple() == cursor.fetchall()

        class CannotDelete(SchemaBaseModel):
            pass

        with pytest.raises(RuntimeError):
            delete_objects(pool, cast(Type[PersistentModel], CannotDelete), tuple())


def test_find_object(pool_and_model):
    pool, _ = pool_and_model

    with pytest.raises(RuntimeError, match='More than one object.*'):
        find_object(pool, PartModel, tuple())

    found = find_object(pool, PartModel, build_where([('name', 'part1')]))
    assert found == model.parts[0]


def test_find_objects(pool_and_model):
    pool, _ = pool_and_model

    found = find_objects(pool, PartModel, build_where([('_container_name', 'sample')]))

    assert next(found) == model.parts[0]
    assert next(found) == model.parts[1]
    assert next(found, None) is None

    found = find_objects(pool, SubPartModel, (('name', 'match', '+sub1 -part2'),))

    assert next(found) == model.parts[0].parts[0]
    assert next(found, None) is None


def test_find_objects_for_multiple_nested_parts(pool_and_model):
    pool, _ = pool_and_model

    found = find_objects(pool, SubPartModel, tuple())

    assert {'part1-sub1', 'part2-sub1', 'part2-sub2'} == {found.name for found in found}


def test_load_object(pool_and_model):
    pool, _ = pool_and_model

    with pytest.raises(RuntimeError, match='cannot found matched item.*'):
        load_object(pool, PartModel, (('name', '=', 'not_existed'),))


def test_query_records_with_match(pool_and_model):
    pool, _ = pool_and_model

    objects = list(
        query_records(pool, SubPartModel, (('name', 'match', 'part sub1'),), fields=('name',))
    )

    assert ['part1-sub1', 'part2-sub1'] == list(o['name'] for o in objects)
 

def test_query_records(pool_and_model):
    pool, _ = pool_and_model

    objects = list(
        query_records(pool, PartModel, tuple(), fields=('_container_name', 'name'))
    )

    assert [
        {'name':'part1', '_container_name':'sample'},
        {'name':'part2', '_container_name':'sample'},
    ] == objects


def test_upsert_objects_makes_entry_in_audit_models():
    class VersionModel(PersistentModel, VersionMixin):
        id:StrId
        message:str

    with use_temp_database_pool_with_model(VersionModel) as pool:
        first = VersionModel(id=StrId('test'), message='first')
        audit_info = VersionInfo.create('who', 'why', 'where', 'tag')

        upsert_objects(pool, first, audit_info)

        version_info = get_version_info(pool, None)

        assert 1 == version_info.version
        assert 'who' == version_info.who
        assert 'why' == version_info.why
        assert 'where' == version_info.where
        assert 'tag' == version_info.tag

        model_changes = get_model_changes_of_version(pool, 1)

        assert [
            {'version':1, 'op':'UPSERT', 'table_name':'md_VersionModel', '__row_id':1}
        ] == list(model_changes)


def test_upsert_objects_for_versioning():
    class VersionModel(PersistentModel, VersionMixin):
        id:StrId
        message:str

    with use_temp_database_pool_with_model(VersionModel) as pool:
        first = VersionModel(id=StrId('test'), message='first')
        second = VersionModel(id=StrId('test'), message='second')
        third = VersionModel(id=StrId('test'), message='third')

        upsert_objects(pool, first)
        upsert_objects(pool, second)
        upsert_objects(pool, third)

        assert first == load_object(pool, VersionModel, (('id', '=', 'test'),), version=1)
        assert second == load_object(pool, VersionModel, (('id', '=', 'test'),), version=2)
        assert third == load_object(pool, VersionModel, (('id', '=', 'test'),), version=3)


def test_upsert_objects_for_dated():
    class DatedModel(PersistentModel, DatedMixin, VersionMixin):
        id:StrId
        message:str

    with use_temp_database_pool_with_model(DatedModel) as pool:
        first = DatedModel(id=StrId('test'), applied_at=DateId(2011, 12, 1), message='first')
        second = DatedModel(id=StrId('test'), applied_at=DateId(2012, 12, 1), message='second')
        third = DatedModel(id=StrId('test'), applied_at=DateId(2013, 12, 1), message='third')

        upsert_objects(pool, first)
        upsert_objects(pool, second)
        upsert_objects(pool, third)

        assert third == load_object(pool, DatedModel, (('id', '=', 'test'),), 
                                    version=3, 
                                    ref_date=date.today())

        assert second == load_object(pool, DatedModel, (('id', '=', 'test'),), 
                                    version=3, 
                                    ref_date=date(2012, 12, 2))

        assert first == load_object(pool, DatedModel, (('id', '=', 'test'),), 
                                    version=1, 
                                    ref_date=date.today())

        # if ref_date is None, we get the all item.
        assert [1,2,3] == list(record['__row_id'] for record in query_records(pool, DatedModel, tuple()))


def test_get_version():
    class SimpleModel(PersistentModel):
        id:StrId
        message:str

    with use_temp_database_pool_with_model(SimpleModel) as pool:
        first = SimpleModel(id=StrId('test'), message='first')
        second = SimpleModel(id=StrId('test'), message='second')
        third = SimpleModel(id=StrId('test'), message='third')

        upsert_objects(pool, first)

        assert 1 == get_version_info(pool).version

        upsert_objects(pool, second)

        second_version = get_version_info(pool)

        assert second_version == get_version_info(pool, second_version.when)

        upsert_objects(pool, third)

        assert 3 == get_version_info(pool).version


def test_squash_objects():
    class VersionModel(PersistentModel, VersionMixin):
        id:StrId
        message:str

    with use_temp_database_pool_with_model(VersionModel) as pool:
        first = VersionModel(id=StrId('test'), message='first')
        second = VersionModel(id=StrId('test'), message='second')
        third = VersionModel(id=StrId('test'), message='third')
        fourth = VersionModel(id=StrId('test'), message='fourth')

        upsert_objects(pool, first)
        upsert_objects(pool, second)
        upsert_objects(pool, third)

        squash_objects(pool, VersionModel, (('id', '=', 'test'),))

        assert third == load_object(pool, VersionModel, (('id', '=', 'test'),), version=1)
        assert third == load_object(pool, VersionModel, (('id', '=', 'test'),), version=2)
        assert third == load_object(pool, VersionModel, (('id', '=', 'test'),), version=3)
        assert third == load_object(pool, VersionModel, (('id', '=', 'test'),), version=4)

        assert [{'__row_id': 1, 'op': 'UPSERT', 'table_name': 'md_VersionModel', 'version': 1}] == list(get_model_changes_of_version(pool, 1))
        assert [
            {'__row_id': 1, 'op': 'DELETE:SQUASHED', 'table_name': 'md_VersionModel', 'version': 4},
            {'__row_id': 2, 'op': 'DELETE:SQUASHED', 'table_name': 'md_VersionModel', 'version': 4},
        ] == list(get_model_changes_of_version(pool, 4))

        upsert_objects(pool, fourth)

        assert fourth == load_object(pool, VersionModel, (('id', '=', 'test'),))

        squash_objects(pool, VersionModel, (('id', '=', 'test'),))

        assert third == load_object(pool, VersionModel, (('id', '=', 'test'),), version=4)
        assert fourth == load_object(pool, VersionModel, (('id', '=', 'test'),), version=5)
        assert fourth == load_object(pool, VersionModel, (('id', '=', 'test'),), version=6)


def test_squash_objects_raises():
    class NonVersionModel(PersistentModel):
        id:StrId
        message:str

    with pytest.raises(RuntimeError, match='to squash is not supported for non version type.'):
        with use_temp_database_pool_with_model(NonVersionModel) as pool:
            first = NonVersionModel(id=StrId('test'), message='first')
            upsert_objects(pool, first)

            squash_objects(pool, NonVersionModel, (('id', '=', 'test'),))

 