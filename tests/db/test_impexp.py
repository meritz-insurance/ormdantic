# for clearing testing database.
#
from datetime import date
from typing import  List, Tuple, cast, Type, Any
import pytest
import pathlib as pl
import tempfile
import pymysql.err

from ormdantic.schema import PersistentModel, IdentifiedModel
from ormdantic.schema.verinfo import VersionInfo
from ormdantic.schema.base import (
    UniqueStringIndex, FullTextSearchedStringIndex, PartOfMixin, PersistentModel, 
    StringArrayIndex, update_forward_refs, 
    StrId, 
    StoredFieldDefinitions, 
)
from ormdantic.schema.typed import (
    TypeNamedModel
)

from ormdantic.database.storage import (
    delete_objects, get_current_version, purge_objects, get_model_changes_of_version, get_version_info, query_records, 
    squash_objects, upsert_objects, find_object, 
    find_objects, build_where, load_object, delete_objects,
    update_sequences
)

from ormdantic.database.impexp import (
    export_to_file, import_from_file
)

from ormdantic.database.queries import get_query_for_next_seq

from .tools import (
    use_temp_database_pool_with_model, 
)

class ContainerModel(IdentifiedModel, TypeNamedModel):
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
                                        codes=['part1-sub1-code1']
                                    ),
                                ),
                                codes = ['part1-code1', 'part1-code2']
                            ),
                            PartModel(
                                name=FullTextSearchedStringIndex('part2'), 
                                parts=(
                                    SubPartModel(
                                        name=FullTextSearchedStringIndex('part2-sub1'),
                                        codes=['part2-sub1-code1', 'part2-sub1-code2']
                                    ),
                                    SubPartModel(
                                        name=FullTextSearchedStringIndex('part2-sub2'),
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


def test_export_to_file(pool_and_model):
    pool, _ = pool_and_model
    with tempfile.TemporaryDirectory() as directory:
        out_dir = pl.Path(directory)

        export_to_file(pool, [ContainerModel], {}, out_dir)

        assert {'ContainerModel/@.json'} == set(f'{path.parts[-2]}/{path.parts[-1]}' for path in out_dir.glob('**/*.json'))

    with pytest.raises(RuntimeError, match='is not supported'):
        export_to_file(pool, [SubPartModel], {}, out_dir)



def test_import_from_file():
    with use_temp_database_pool_with_model(ContainerModel) as pool:
        import_from_file(pool, [
            pl.Path('./tests/resources/json/import_from_file')], 'import test', 0, False)
        
        ids = set(record['id'] for record
                  in query_records(pool, ContainerModel, {}, 0, fields=('id',)))

        assert {'first', 'second'} == ids


def test_import_from_file_ignore_error():
    class MyModel(IdentifiedModel, TypeNamedModel):
        code: UniqueStringIndex 

    with use_temp_database_pool_with_model(MyModel) as pool:
        with pytest.raises(pymysql.err.IntegrityError):
            import_from_file(pool, [
                pl.Path('./tests/resources/json/import_from_file_ignore_error')
            ], 'import test', 0, False)
     
        import_from_file(pool, [
            pl.Path('./tests/resources/json/import_from_file_ignore_error')
        ], 'import test', 0, True)
     