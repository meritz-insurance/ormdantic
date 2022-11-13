# for clearing testing database.
#
from datetime import date
from typing import  List, Tuple, cast, Type, Any
import pytest

from ormdantic.schema import PersistentModel
from ormdantic.schema import verinfo
from ormdantic.schema.verinfo import VersionInfo
from ormdantic.schema.base import (
    DatedMixin, FullTextSearchedStringIndex, PartOfMixin, PersistentModel, 
    StringArrayIndex, VersionMixin, get_identifer_of, update_forward_refs, 
    IdentifiedModel, StrId, DateId, SequenceStrId, ArrayIndexMixin,
    StoredFieldDefinitions, SchemaBaseModel, get_identifying_field_values
)
from ormdantic.schema.shareds import (
    PersistentSharedContentModel
)

from ormdantic.database.storage import (
    delete_objects, get_current_version, purge_objects, get_model_changes_of_version, get_version_info, query_records, 
    squash_objects, upsert_objects, find_object, 
    find_objects, build_where, load_object, delete_objects,
    update_sequences
)

from ormdantic.database.queries import get_query_for_next_seq

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
        upserted = upsert_objects(pool, model, 0, False, VersionInfo())

        yield pool, upserted


def test_upsert_objects(pool_and_model):
    pool, upserted = pool_and_model

    # multiple same object does not affect the database.
    upserted = upsert_objects(pool, upserted, 0, False, VersionInfo())

    where = build_where(get_identifer_of(upserted))

    found = find_object(pool, ContainerModel, where, 0, 
                        version=get_current_version(pool))

    assert upserted == found


def test_upsert_objects_with_exception(pool_and_model):
    pool, _ = pool_and_model

    with pytest.raises(RuntimeError, match='PartOfMixin could not be saved directly.*'):
        upsert_objects(pool, model.parts[0], 0, False, VersionInfo())


def test_upsert_objects_callback(monkeypatch, pool_and_model):
    pool, upserted = pool_and_model
    called = False

    def saved(id:Tuple[Any,...], model:PersistentModel | BaseException):
        nonlocal called
        called = True

        assert id == tuple(get_identifying_field_values(upserted).values())
        assert model == upserted

    upsert_objects(pool, upserted, 0, False, VersionInfo(), saved_callback=saved)

    exception = RuntimeError()

    class RaiseInSave(IdentifiedModel):
        def _before_save(self):
            raise exception

    called = False

    model = RaiseInSave(id=StrId('@'))

    def saved_exception(id:Tuple[Any,...], model:PersistentModel | BaseException):
        nonlocal called
        called = True

        assert id == tuple(get_identifying_field_values(upserted).values())
        assert model == exception

    with pytest.raises(RuntimeError):
        upsert_objects(pool, model, 0, False, VersionInfo())
    
    upsert_objects(pool, model, 0, True, VersionInfo(), saved_exception)

    assert called


def test_purge_objects():
    with use_temp_database_pool_with_model(ContainerModel) as pool:
        upserted = upsert_objects(pool, model, 0, False, VersionInfo())

        found = find_objects(pool, PartModel, {}, 0)
        assert found 

        found = find_objects(pool, SubPartModel, {}, 0)
        assert found 

        # multiple same object does not affect the database.
        where = build_where(get_identifer_of(upserted))

        assert ['@'] == purge_objects(pool, ContainerModel, where, 0)

        found = find_object(pool, ContainerModel, where, 0)

        assert found is None

        found = list(find_objects(pool, PartModel, {}, 0))
        assert not found 

        found = list(find_objects(pool, SubPartModel, {}, 0))
        assert not found

        # check empty external table
        with pool.open_cursor() as cursor:
            cursor.execute('SELECT * FROM md_SubPartModel_codes')
            assert tuple() == cursor.fetchall()

        class CannotDelete(SchemaBaseModel):
            pass

        with pytest.raises(RuntimeError):
            purge_objects(pool, cast(Type[PersistentModel], CannotDelete), {}, 0)


def test_delete_objects():
    with use_temp_database_pool_with_model(ContainerModel) as pool:
        upserted = upsert_objects(pool, model, 0, False, VersionInfo())

        found = find_objects(pool, PartModel, {}, 0)
        assert found 

        found = find_objects(pool, SubPartModel, {}, 0)
        assert found 

        # multiple same object does not affect the database.
        where = build_where(get_identifer_of(upserted))

        version = get_current_version(pool)

        assert ['@'] == delete_objects(pool, ContainerModel, where, 0)

        found = find_object(pool, ContainerModel, where, 0, version=version)
        assert found

        found = find_object(pool, ContainerModel, where, 0)
        assert found is None

        found = list(find_objects(pool, PartModel, {}, 0, version=version))
        assert found 

        found = list(find_objects(pool, PartModel, {}, 0))
        assert not found 

        found = list(find_objects(pool, SubPartModel, {}, 0))
        assert not found

        # check empty external table
        with pool.open_cursor() as cursor:
            cursor.execute('SELECT * FROM md_SubPartModel_codes')
            assert 4 == len(cursor.fetchall())

        class CannotDelete(SchemaBaseModel):
            pass

        with pytest.raises(RuntimeError):
            delete_objects(pool, cast(Type[PersistentModel], CannotDelete), {}, 0)

        with pytest.raises(RuntimeError):
            delete_objects(pool, cast(Type[PersistentModel], PersistentSharedContentModel), {}, 0)




def test_find_object(pool_and_model):
    pool, _ = pool_and_model

    with pytest.raises(RuntimeError, match='More than one object.*'):
        find_object(pool, PartModel, {}, 0, version=get_current_version(pool))

    found = find_object(pool, PartModel, build_where([('name', 'part1')]), 0,
                        version=get_current_version(pool))
    assert found == model.parts[0]


def test_find_objects(pool_and_model):
    pool, _ = pool_and_model

    current_version = get_current_version(pool)

    found = find_objects(pool, PartModel, build_where([('_container_name', 'sample')]), 
                         0, version=current_version)

    assert next(found) == model.parts[0]
    assert next(found) == model.parts[1]
    assert next(found, None) is None

    found = find_objects(pool, SubPartModel, {'name': ('match', '+sub1 -part2')}, 
                         0, version=current_version)

    assert next(found) == model.parts[0].parts[0]
    assert next(found, None) is None


def test_find_objects_for_multiple_nested_parts(pool_and_model):
    pool, _ = pool_and_model

    found = find_objects(pool, SubPartModel, {}, 0, version=get_current_version(pool))

    assert {'part1-sub1', 'part2-sub1', 'part2-sub2'} == {found.name for found in found}


def test_load_object(pool_and_model):
    pool, _ = pool_and_model

    with pytest.raises(RuntimeError, match='cannot found matched item.*'):
        load_object(pool, PartModel, {'name': 'not_existed'}, 0)


def test_query_records_with_match(pool_and_model):
    pool, _ = pool_and_model

    objects = list(
        query_records(pool, SubPartModel, {'name': ('match', 'part sub1')}, 0, 
                      fields=('name',),
                      version=get_current_version(pool))
    )

    assert ['part1-sub1', 'part2-sub1'] == list(o['name'] for o in objects)
 

def test_query_records(pool_and_model):
    pool, _ = pool_and_model

    objects = list(
        query_records(pool, PartModel, {}, 0,
        fields=('_container_name', 'name'),
        version=get_current_version(pool))
    )

    assert [
        {'name':'part1', '_container_name':'sample'},
        {'name':'part2', '_container_name':'sample'},
    ] == objects


def test_query_records_raise(pool_and_model):
    pool, _ = pool_and_model

    with pytest.raises(RuntimeError, match='empty fields for querying'):
        list(
            query_records(pool, PartModel, {}, 0,
                          fields=tuple(),
                          version=get_current_version(pool))
        )


def test_upsert_objects_makes_entry_in_audit_models():
    class VersionModel(PersistentModel, VersionMixin):
        id:StrId
        message:str

    with use_temp_database_pool_with_model(VersionModel) as pool:
        first = VersionModel(id=StrId('test'), message='first')
        audit_info = VersionInfo.create('who', 'why', 'where', 'tag')

        upsert_objects(pool, first, 0, False, audit_info)

        version_info = get_version_info(pool, None)

        assert 1 == version_info.version
        assert 'who' == version_info.who
        assert 'why' == version_info.why
        assert 'where' == version_info.where
        assert 'tag' == version_info.tag

        model_changes = get_model_changes_of_version(pool, 1)

        assert [
            {'version':1, 'op':'UPSERTED', 'table_name':'md_VersionModel', 
            '__row_id':1, '__set_id':0, 'model_id':'test,1'}
        ] == list(model_changes)


def test_upsert_objects_for_versioning():
    class VersionModel(PersistentModel, VersionMixin):
        id:StrId
        message:str

    with use_temp_database_pool_with_model(VersionModel) as pool:
        first = VersionModel(id=StrId('test'), message='first')
        second = VersionModel(id=StrId('test'), message='second')
        third = VersionModel(id=StrId('test'), message='third')

        upsert_objects(pool, first, 0, False, VersionInfo())
        upsert_objects(pool, second, 0, False, VersionInfo())
        upsert_objects(pool, third, 0, False, VersionInfo())

        assert first == load_object(pool, VersionModel, {'id': 'test'}, 0, version=1)
        assert second == load_object(pool, VersionModel, {'id': 'test'}, 0, version=2)
        assert third == load_object(pool, VersionModel, {'id': 'test'}, 0, version=3)


def test_upsert_objects_for_dated():
    class DatedModel(PersistentModel, DatedMixin, VersionMixin):
        id:StrId
        message:str

    with use_temp_database_pool_with_model(DatedModel) as pool:
        first = DatedModel(id=StrId('test'), applied_at=DateId(2011, 12, 1), message='first')
        second = DatedModel(id=StrId('test'), applied_at=DateId(2012, 12, 1), message='second')
        third = DatedModel(id=StrId('test'), applied_at=DateId(2013, 12, 1), message='third')

        upsert_objects(pool, first, 0, False, VersionInfo())
        upsert_objects(pool, second, 0, False, VersionInfo())
        upsert_objects(pool, third, 0, False, VersionInfo())

        assert third == load_object(pool, DatedModel, {'id': 'test'}, 0,
                                    version=3, 
                                    ref_date=date.today())

        assert second == load_object(pool, DatedModel, {'id': 'test'}, 0,
                                    version=3, 
                                    ref_date=date(2012, 12, 2))

        assert first == load_object(pool, DatedModel, {'id': 'test'}, 0,
                                    version=1, 
                                    ref_date=date.today())

        # if ref_date is None, we get the all item.
        assert [1,2,3] == list(
            record['__row_id'] 
            for record in query_records(
                pool, DatedModel, {}, 0, version=get_current_version(pool)))


def test_get_version():
    class SimpleModel(PersistentModel):
        id:StrId
        message:str

    with use_temp_database_pool_with_model(SimpleModel) as pool:
        first = SimpleModel(id=StrId('test'), message='first')
        second = SimpleModel(id=StrId('test'), message='second')
        third = SimpleModel(id=StrId('test'), message='third')

        upsert_objects(pool, first, 0, False, VersionInfo())

        assert 1 == get_version_info(pool).version

        upsert_objects(pool, second, 0, False, VersionInfo())

        second_version = get_version_info(pool)

        assert second_version == get_version_info(pool, second_version.when)

        upsert_objects(pool, third, 0, False, VersionInfo())

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

        upsert_objects(pool, first, 0, False, VersionInfo())
        upsert_objects(pool, second, 0, False, VersionInfo())
        upsert_objects(pool, third, 0, False, VersionInfo())

        assert [{'id':'test'}] == squash_objects(pool, VersionModel, {'id':'test'}, 0)

        assert third == load_object(pool, VersionModel, {'id':'test'}, 0, version=1)
        assert third == load_object(pool, VersionModel, {'id':'test'}, 0, version=2)
        assert third == load_object(pool, VersionModel, {'id':'test'}, 0, version=3)
        assert third == load_object(pool, VersionModel, {'id':'test'}, 0, version=4)

        assert [
            {'__row_id': 1, 'op': 'UPSERTED', 'table_name': 'md_VersionModel', 
            'version': 1, '__set_id':0, 'model_id': 'test,1'}
        ] == list(get_model_changes_of_version(pool, 1))

        assert [
            {'__row_id': 1, 'op': 'PURGED:SQUASHED', 'table_name': 'md_VersionModel', 
            'version': 4, '__set_id':0, 'model_id': 'test,1,2'},
            {'__row_id': 2, 'op': 'PURGED:SQUASHED', 'table_name': 'md_VersionModel', 
            'version': 4, '__set_id':0, 'model_id': 'test,2,3'}
        ] == list(get_model_changes_of_version(pool, 4))

        upsert_objects(pool, fourth, 0, False, VersionInfo())

        assert fourth == load_object(pool, VersionModel, {'id':'test'}, 0, 
                                     version=get_current_version(pool))

        assert [{'id':'test'}] == squash_objects(pool, VersionModel, {'id':'test'}, 0)

        assert third == load_object(pool, VersionModel, {'id':'test'}, 0, version=4)
        assert fourth == load_object(pool, VersionModel, {'id':'test'}, 0, version=5)
        assert fourth == load_object(pool, VersionModel, {'id':'test'}, 0, version=6)


def test_squash_objects_raises():
    class NonVersionModel(PersistentModel):
        id:StrId
        message:str

    with pytest.raises(RuntimeError, match='to squash is not supported for non version type.'):
        with use_temp_database_pool_with_model(NonVersionModel) as pool:
            first = NonVersionModel(id=StrId('test'), message='first')
            upsert_objects(pool, first, 0, False, VersionInfo())

            squash_objects(pool, NonVersionModel, {'id': 'test'}, 0)

 
def test_update_sequences():
    class CodedModel(PersistentModel):
        code:SequenceStrId

    with use_temp_database_pool_with_model(CodedModel) as pool:
        first = CodedModel(code=SequenceStrId('N33'))
        upsert_objects(pool, first, 0, False, VersionInfo())
        update_sequences(pool, [CodedModel])

        second = CodedModel(code=SequenceStrId(''))
        second = upsert_objects(pool, second, 0, False, VersionInfo())

        assert 'N34' == second.code
            

