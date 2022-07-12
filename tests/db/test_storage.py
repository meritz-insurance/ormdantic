# for clearing testing database.
# mariadb -h localhost -P 33069 -p -u root -N -B -e 'select CONCAT("drop database `", schema_name, "`;") as name from information_schema.schemata where schema_name like "TEST_%";'
#
from typing import  List, Tuple
import pytest

from ormdantic.database.storage import (
    delete_objects, query_records, upsert_objects, find_object, 
    find_objects, build_where
)

from ormdantic.schema import PersistentModel
from ormdantic.schema.base import (
    FullTextSearchedStringIndex, PartOfMixin, StringArrayIndex, 
    get_identifer_of, update_part_of_forward_refs, IdentifiedModel, UuidStr,
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

update_part_of_forward_refs(ContainerModel, locals())
update_part_of_forward_refs(PartModel, locals())


model = ContainerModel(id=UuidStr('@'), 
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


@pytest.fixture
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

        # multiple same object does not affect the database.
        where = build_where(get_identifer_of(upserted))

        found = delete_objects(pool, ContainerModel, where)

        found = find_object(pool, ContainerModel, where)

        assert found is None


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
 