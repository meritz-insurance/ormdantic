# for clearing testing database.
#
from datetime import date
from typing import  List, Tuple, cast, Type, Any, Annotated
import pytest
import pymysql

from ormdantic.schema import PersistentModel, IdentifiedModel
from ormdantic.schema.verinfo import VersionInfo
from ormdantic.schema.base import (
    DatedMixin, FullTextSearchedStringIndex, PartOfMixin, PersistentModel, 
    StringArrayIndex, VersionMixin, get_identifer_of, update_forward_refs, 
    UuidStr, SequenceStr, 
    StoredFieldDefinitions, SchemaBaseModel, get_identifying_field_values,
    UniqueStringIndex, MetaIdentifyingField
)
from ormdantic.schema.shareds import (
    PersistentSharedContentModel
)

from ormdantic.database.storage import (
    delete_objects, get_current_version, purge_objects, get_model_changes_of_version, get_version_info, query_records, 
    squash_objects, upsert_objects, find_object, 
    find_objects, build_where, load_object, delete_objects,
    update_sequences, merge_model_set
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


model = ContainerModel(id=UuidStr('@'), 
                        version='0.1.0',
                        name='sample',
                        parts=[
                            PartModel(
                                name='part1', 
                                parts=(
                                    SubPartModel(
                                        name='part1-sub1',
                                        codes=['part1-sub1-code1']
                                    ),
                                ),
                                codes = ['part1-code1', 'part1-code2']
                            ),
                            PartModel(
                                name='part2', 
                                parts=(
                                    SubPartModel(
                                        name='part2-sub1',
                                        codes=['part2-sub1-code1', 'part2-sub1-code2']
                                    ),
                                    SubPartModel(
                                        name='part2-sub2',
                                        codes=[]
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

    model = RaiseInSave(id=UuidStr('@'))

    def saved_exception(id:Tuple[Any,...], model:PersistentModel | BaseException):
        nonlocal called
        called = True

        assert id == tuple(get_identifying_field_values(upserted).values())
        assert model == exception

    with pytest.raises(RuntimeError):
        upsert_objects(pool, model, 0, False, VersionInfo())
    
    upsert_objects(pool, model, 0, True, VersionInfo(), saved_exception)

    assert called

def test_upsert_objects_duplicate_key():
    class MyModel(IdentifiedModel):
        code: UniqueStringIndex 

    models = [
        MyModel(id=UuidStr('first'), code=UniqueStringIndex('c1')),
        MyModel(id=UuidStr('second'), code=UniqueStringIndex('c1'))
    ]

    with use_temp_database_pool_with_model(MyModel) as pool:
        with pytest.raises(pymysql.DatabaseError):
            upserted = upsert_objects(pool, models, 0, False, VersionInfo())

        upserted = upsert_objects(pool, models, 0, True, VersionInfo())

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

    with pytest.raises(RuntimeError, match='more than one object.*'):
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

    with pytest.raises(RuntimeError, match='no such item.*'):
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
        id: Annotated[UuidStr, MetaIdentifyingField()]
        message:str

    with use_temp_database_pool_with_model(VersionModel) as pool:
        first = VersionModel(id=UuidStr('test'), message='first')
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
            {'version':1, 'op':'INSERTED', 'table_name':'md_VersionModel', 
            '__row_id':1, '__set_id':0, 'model_id':'test,1', 'data_version':1}
        ] == list(model_changes)


def test_upsert_objects_for_versioning():
    class VersionModel(PersistentModel, VersionMixin):
        id: Annotated[UuidStr, MetaIdentifyingField()]
        message:str

    with use_temp_database_pool_with_model(VersionModel) as pool:
        first = VersionModel(id=UuidStr('test'), message='first')
        second = VersionModel(id=UuidStr('test'), message='second')
        third = VersionModel(id=UuidStr('test'), message='third')

        upsert_objects(pool, first, 0, False, VersionInfo())
        upsert_objects(pool, second, 0, False, VersionInfo())
        upsert_objects(pool, third, 0, False, VersionInfo())

        assert first == load_object(pool, VersionModel, {'id': 'test'}, 0, version=1)
        assert second == load_object(pool, VersionModel, {'id': 'test'}, 0, version=2)
        assert third == load_object(pool, VersionModel, {'id': 'test'}, 0, version=3)

        assert 1 == load_object(pool, VersionModel, {'id': 'test'}, 0, version=1)._valid_start 
        assert 2 == load_object(pool, VersionModel, {'id': 'test'}, 0, version=2)._row_id 
        assert 2 == load_object(pool, VersionModel, {'id': 'test'}, 0, version=2)._valid_start 
        assert 3 == load_object(pool, VersionModel, {'id': 'test'}, 0, version=3)._valid_start 
        assert 3 == load_object(pool, VersionModel, {'id': 'test'}, 0, version=3)._row_id 

def test_upsert_objects_for_dated():
    class DatedModel(PersistentModel, DatedMixin, VersionMixin):
        id: Annotated[UuidStr, MetaIdentifyingField()]
        message:str

    with use_temp_database_pool_with_model(DatedModel) as pool:
        first = DatedModel(id=UuidStr('test'), applied_at=date(2011, 12, 1), message='first')
        second = DatedModel(id=UuidStr('test'), applied_at=date(2012, 12, 1), message='second')
        third = DatedModel(id=UuidStr('test'), applied_at=date(2013, 12, 1), message='third')

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
        id: Annotated[UuidStr, MetaIdentifyingField()]
        message:str

    with use_temp_database_pool_with_model(SimpleModel) as pool:
        first = SimpleModel(id=UuidStr('test'), message='first')
        second = SimpleModel(id=UuidStr('test'), message='second')
        third = SimpleModel(id=UuidStr('test'), message='third')

        upsert_objects(pool, first, 0, False, VersionInfo())

        assert 1 == get_version_info(pool).version

        upsert_objects(pool, second, 0, False, VersionInfo())

        second_version = get_version_info(pool)

        assert second_version == get_version_info(pool, second_version.when)

        upsert_objects(pool, third, 0, False, VersionInfo())

        assert 3 == get_version_info(pool).version


def test_squash_objects():
    class VersionModel(PersistentModel, VersionMixin):
        id: Annotated[UuidStr, MetaIdentifyingField()]
        message:str

    with use_temp_database_pool_with_model(VersionModel) as pool:
        first = VersionModel(id=UuidStr('test'), message='first')
        second = VersionModel(id=UuidStr('test'), message='second')
        third = VersionModel(id=UuidStr('test'), message='third')
        fourth = VersionModel(id=UuidStr('test'), message='fourth')

        upsert_objects(pool, first, 0, False, VersionInfo())
        upsert_objects(pool, second, 0, False, VersionInfo())
        upsert_objects(pool, third, 0, False, VersionInfo())

        assert [{'id':'test'}] == squash_objects(pool, VersionModel, {'id':'test'}, 0)

        assert third == load_object(pool, VersionModel, {'id':'test'}, 0, version=1)
        assert third == load_object(pool, VersionModel, {'id':'test'}, 0, version=2)
        assert third == load_object(pool, VersionModel, {'id':'test'}, 0, version=3)
        assert third == load_object(pool, VersionModel, {'id':'test'}, 0, version=4)

        assert [
            {'__row_id': 1, 'op': 'INSERTED', 'table_name': 'md_VersionModel', 
            'version': 1, '__set_id':0, 'model_id': 'test,1', 'data_version':1}
        ] == list(get_model_changes_of_version(pool, 1))

        assert [
            {'__row_id': 1, 'op': 'PURGED:SQUASHED', 'table_name': 'md_VersionModel', 
            'version': 4, '__set_id':0, 'model_id': 'test,1,2', 'data_version':None},
            {'__row_id': 2, 'op': 'PURGED:SQUASHED', 'table_name': 'md_VersionModel', 
            'version': 4, '__set_id':0, 'model_id': 'test,2,3', 'data_version':None}
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
        id: Annotated[UuidStr, MetaIdentifyingField()]
        message:str

    with pytest.raises(RuntimeError, match='to squash is not supported for non version type.'):
        with use_temp_database_pool_with_model(NonVersionModel) as pool:
            first = NonVersionModel(id=UuidStr('test'), message='first')
            upsert_objects(pool, first, 0, False, VersionInfo())

            squash_objects(pool, NonVersionModel, {'id': 'test'}, 0)

 
def test_update_sequences():
    class CodedModel(PersistentModel):
        code:Annotated[SequenceStr, MetaIdentifyingField()]

    with use_temp_database_pool_with_model(CodedModel) as pool:
        first = CodedModel(code=SequenceStr('N33'))
        upsert_objects(pool, first, 0, False, VersionInfo())
        update_sequences(pool, [CodedModel])

        second = CodedModel(code=SequenceStr(''))
        second = upsert_objects(pool, second, 0, False, VersionInfo())

        assert 'N34' == second.code
            

def test_merge_model_set():
    class CodedModel(PersistentModel):
        code:Annotated[SequenceStr, MetaIdentifyingField()]
        message: str

    with use_temp_database_pool_with_model(CodedModel) as pool:
        first = CodedModel(code=SequenceStr('N1'), message='first')
        upsert_objects(pool, first, 0, False, VersionInfo())

        second = CodedModel(code=SequenceStr('N1'), message='second')
        upsert_objects(pool, second, 1, False, VersionInfo())

        found = find_object(pool, CodedModel, {'code':'N1'}, 1)
        assert found
        assert 2 == found._valid_start 

        assert first == find_object(pool, CodedModel, {'code':'N1'}, 0)

        merge_model_set(pool, {CodedModel:{}}, 1, 0, False, VersionInfo())

        found = find_object(pool, CodedModel, {'code':'N1'}, 0)

        assert found

        assert second == found
        assert 2 == found._valid_start 

        assert [
            {'version':3, 'data_version':2, 'op': 'INSERTED:MERGE_SET', 
            'table_name': 'md_CodedModel', '__set_id':0, '__row_id':3, 'model_id': 'N1'}
        ] == list(get_model_changes_of_version(pool, 3)) 


def test_merge_model_set_with_version():
    class CodedModel(PersistentModel, VersionMixin):
        code:Annotated[SequenceStr, MetaIdentifyingField()]
        message: str

    with use_temp_database_pool_with_model(CodedModel) as pool:
        first = CodedModel(code=SequenceStr('N1'), message='first')
        upsert_objects(pool, first, 0, False, VersionInfo())

        second = CodedModel(code=SequenceStr('N1'), message='second')
        upsert_objects(pool, second, 1, False, VersionInfo())

        found = find_object(pool, CodedModel, {'code':'N1'}, 1)
        assert found
        assert 2 == found._valid_start 

        assert first == find_object(pool, CodedModel, {'code':'N1'}, 0)

        merge_model_set(pool, {CodedModel:{}}, 1, 0, False, VersionInfo())

        found = find_object(pool, CodedModel, {'code':'N1'}, 0)

        assert found

        assert second == found
        assert 2 == found._valid_start 

        assert [
            {'version':3, 'data_version':2, 'op': 'INSERTED:MERGE_SET', 
            'table_name': 'md_CodedModel', '__set_id':0, '__row_id':3, 'model_id': 'N1,2,9223372036854775807'}
        ] == list(get_model_changes_of_version(pool, 3)) 


def test_merge_model_set_from_empty():
    class CodedModel(PersistentModel):
        code:Annotated[SequenceStr, MetaIdentifyingField()]
        message: str

    with use_temp_database_pool_with_model(CodedModel) as pool:
        first = CodedModel(code=SequenceStr('N1'), message='first')
        upsert_objects(pool, first, 1, False, VersionInfo())

        merge_model_set(pool, {CodedModel:{}}, 1, 0, False, VersionInfo())

        found = find_object(pool, CodedModel, {'code':'N1'}, 0)

        assert found

        assert first == found
        assert 1 == found._valid_start 

        assert [
            {'version':2, 'data_version':1, 'op': 'INSERTED:MERGE_SET', 
            'table_name': 'md_CodedModel', '__set_id':0, '__row_id':2, 'model_id': 'N1'}
        ] == list(get_model_changes_of_version(pool, 2)) 


def test_merge_model_set_versioned_from_empty():
    class CodedModel(PersistentModel, VersionMixin):
        code:Annotated[SequenceStr, MetaIdentifyingField()]
        message: str

    with use_temp_database_pool_with_model(CodedModel) as pool:
        first = CodedModel(code=SequenceStr('N1'), message='first')
        upsert_objects(pool, first, 1, False, VersionInfo())

        merge_model_set(pool, {CodedModel:{}}, 1, 0, False, VersionInfo())

        found = find_object(pool, CodedModel, {'code':'N1'}, 0)

        assert found

        assert first == found
        assert 1 == found._valid_start 

        assert [
            {'version':2, 'data_version':1, 'op': 'INSERTED:MERGE_SET', 
            'table_name': 'md_CodedModel', '__set_id':0, '__row_id':2, 'model_id': 'N1,1,9223372036854775807'}
        ] == list(get_model_changes_of_version(pool, 2)) 


def test_merge_model_set_forced():
    class CodedModel(PersistentModel):
        code:Annotated[SequenceStr, MetaIdentifyingField()]
        message: str

    with use_temp_database_pool_with_model(CodedModel) as pool:
        first = CodedModel(code=SequenceStr('N1'), message='first')
        upsert_objects(pool, first, 0, False, VersionInfo())

        second = CodedModel(code=SequenceStr('N1'), message='second')
        upsert_objects(pool, second, 1, False, VersionInfo())

        assert first == find_object(pool, CodedModel, {'code':'N1'}, 0)

        first.message = 'third'

        upsert_objects(pool, first, 0, False, VersionInfo())

        with pytest.raises(RuntimeError, match='.*copy object because.* new version .*forced.*'):
            merge_model_set(pool, {CodedModel:{}}, 1, 0, False, VersionInfo())

        merge_model_set(pool, {CodedModel:{}}, 1, 0, True, VersionInfo())
        found = find_object(pool, CodedModel, {'code':'N1'}, 0)

        assert found
        assert second == found

        assert 5 == found._valid_start

        assert [
            {'version':5, 'data_version':5, 'op': 'INSERTED:MERGE_SET', 
            'table_name': 'md_CodedModel', '__set_id':0, '__row_id':4, 'model_id': 'N1'}
        ] == list(get_model_changes_of_version(pool, 5)) 


def test_merge_model_set_versioned_forced():
    class CodedModel(PersistentModel, VersionMixin):
        code:Annotated[SequenceStr, MetaIdentifyingField()]
        message: str

    with use_temp_database_pool_with_model(CodedModel) as pool:
        first = CodedModel(code=SequenceStr('N1'), message='first')
        upsert_objects(pool, first, 0, False, VersionInfo())

        second = CodedModel(code=SequenceStr('N1'), message='second')
        upsert_objects(pool, second, 1, False, VersionInfo())

        assert first == find_object(pool, CodedModel, {'code':'N1'}, 0)

        first.message = 'third'

        upsert_objects(pool, first, 0, False, VersionInfo())

        with pytest.raises(RuntimeError, match='.*copy object because.* new version .*forced.*'):
            merge_model_set(pool, {CodedModel:{}}, 1, 0, False, VersionInfo())

        merge_model_set(pool, {CodedModel:{}}, 1, 0, True, VersionInfo())
        found = find_object(pool, CodedModel, {'code':'N1'}, 0)

        assert found
        assert second == found

        assert 5 == found._valid_start

        assert [
            {'version':5, 'data_version':5, 'op': 'INSERTED:MERGE_SET', 
            'table_name': 'md_CodedModel', '__set_id':0, '__row_id':5, 'model_id': 'N1,5,9223372036854775807'}
        ] == list(get_model_changes_of_version(pool, 5)) 



def test_merge_model_set_does_not_copy_already_copied():
    class CodedModel(PersistentModel):
        code:Annotated[SequenceStr, MetaIdentifyingField()]
        message: str

    with use_temp_database_pool_with_model(CodedModel) as pool:
        first = CodedModel(code=SequenceStr('N1'), message='first')
        upsert_objects(pool, first, 0, False, VersionInfo())

        second = CodedModel(code=SequenceStr('N1'), message='second')
        upsert_objects(pool, second, 1, False, VersionInfo())

        merge_model_set(pool, {CodedModel:{}}, 1, 0, False, VersionInfo())
        merge_model_set(pool, {CodedModel:{}}, 1, 0, False, VersionInfo())
        merge_model_set(pool, {CodedModel:{}}, 0, 1, False, VersionInfo())

        found = find_object(pool, CodedModel, {'code':'N1'}, 0)

        assert found

        assert second == found
        assert 2 == found._valid_start 

        assert [
            {'version':3, 'data_version':2, 'op': 'INSERTED:MERGE_SET', 
            'table_name': 'md_CodedModel', '__set_id':0, '__row_id':3, 'model_id': 'N1'}
        ] == list(get_model_changes_of_version(pool, 3)) 

        assert [
        ] == list(get_model_changes_of_version(pool, 4)) 

        assert [
        ] == list(get_model_changes_of_version(pool, 5)) 

        assert 5 == get_current_version(pool)


def test_merge_model_set_affect_parts_and_external():
    with use_temp_database_pool_with_model(ContainerModel) as pool:
        upsert_objects(pool, model, 1, False, VersionInfo())

        found = list(find_objects(pool, SubPartModel, {'codes':('like', '%code1%')}, 0, unwind='codes'))
        assert not found 

        merge_model_set(pool, {ContainerModel:{}}, 1, 0, False, VersionInfo())

        found = list(find_objects(pool, PartModel, {}, 0))
        assert found 

        found = list(find_objects(pool, SubPartModel, {'codes':('like', '%code1%')}, 0, unwind='codes'))
        assert found 

# version start, end가 제대로 update되는지 확인.
# destination의 version이 높을때, forced에 의해서 overwrite 혹은 raise가 되는지
# version type에 대한 복사와 일반 type에 대한 복사 확인
# Performance Test 1000여개 객체 move시 확인
# Data Version이 제대로 set 되는지 확인하자.
# external이나 partof가 제대로 update되는지 확인하자.
# 
