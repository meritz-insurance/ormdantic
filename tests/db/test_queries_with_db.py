from hashlib import new
from pydoc import describe
from telnetlib import SE
from typing import List, ClassVar

from pydantic import Field

import pytest
from ormdantic.database.storage import allocate_audit_version

from ormdantic.schema import PersistentModel
from ormdantic.database.queries import (
    get_query_and_args_for_deleting, get_query_and_args_for_upserting, get_query_and_args_for_reading,
    get_query_and_args_for_purging, 
)
from ormdantic.schema.base import (
    PersistentModel, SequenceStrId, StringArrayIndex, FullTextSearchedStringIndex, 
    PartOfMixin, StringReference, 
    StringIndex, UseBaseClassTableMixin, 
    update_forward_refs, IdentifiedModel, StrId, 
    StoredFieldDefinitions
)
from ormdantic.schema.verinfo import VersionInfo

from .tools import (
    use_temp_database_cursor_with_model, 
)

class SimpleBaseModel(IdentifiedModel):
    pass

def test_get_query_and_args_for_reading():
    model = SimpleBaseModel(id=StrId('@'))

    with use_temp_database_cursor_with_model(model, model_created=False) as cursor:
        new_version = allocate_audit_version(cursor, VersionInfo())

        model.id = StrId("0")
        query_and_args = get_query_and_args_for_upserting(model, set_id=0)

        cursor.execute(*query_and_args)

        model.id = StrId("1")
        query_and_args = get_query_and_args_for_upserting(model, set_id=0)

        cursor.execute(*query_and_args)

        query_and_args = get_query_and_args_for_reading(
            SimpleBaseModel, '*', tuple(), set_id=0, version=new_version)

        cursor.execute(*query_and_args)

        assert [
            {'__row_id': 1, '__set_id': 0, 'id': '0', '__json': '{"id":"0","version":"0.1.0"}', '__valid_start':1, '__valid_end': 9223372036854775807,},
            {'__row_id': 2, '__set_id': 0, 'id': '1', '__json': '{"id":"1","version":"0.1.0"}', '__valid_start':1, '__valid_end': 9223372036854775807,}
        ] == cursor.fetchall()

        query_and_args = get_query_and_args_for_reading(
            SimpleBaseModel, ('__row_id',), (('__row_id', '=', 2),), 0,
            version=new_version
        )

        cursor.execute(*query_and_args)
        assert [{'__row_id': 2}] == cursor.fetchall()


def test_get_query_and_args_for_purging():
    model = SimpleBaseModel(id=StrId('@'), version='0.1.0')

    with use_temp_database_cursor_with_model(model) as cursor:
        query_and_args = get_query_and_args_for_purging(
            SimpleBaseModel, tuple(), 0)

        cursor.execute(*query_and_args)

        assert [{
            '__row_id':1, '__set_id': 0, 'op':'PURGED', 
            'table_name':'md_SimpleBaseModel', 'model_id': '@'}
        ] == cursor.fetchall()

        query_and_args = get_query_and_args_for_reading(
            SimpleBaseModel, ('id',), tuple(), 0, version=2)

        cursor.execute(*query_and_args)

        assert tuple() == cursor.fetchall()

def test_get_query_and_args_for_deleting():
    model = SimpleBaseModel(id=StrId('@'), version='0.1.0')

    with use_temp_database_cursor_with_model(model) as cursor:
        allocate_audit_version(cursor, VersionInfo())
        query_and_args = get_query_and_args_for_deleting(
            SimpleBaseModel, tuple(), 0)

        cursor.execute(*query_and_args)

        cursor.nextset()

        assert [{
            '__row_id':1, '__set_id': 0, 'op':'DELETED', 
            'table_name':'md_SimpleBaseModel', 'model_id': '@'}
        ] == cursor.fetchall()

        query_and_args = get_query_and_args_for_reading(
            SimpleBaseModel, ('id',), tuple(), 0, version=2)

        cursor.execute(*query_and_args)

        assert tuple() == cursor.fetchall()

        query_and_args = get_query_and_args_for_reading(
            SimpleBaseModel, ('id',), tuple(), 0, version=1)

        cursor.execute(*query_and_args)

        assert [{'id':'@'}] == cursor.fetchall()



def test_get_query_and_args_for_reading_for_parts():
    class ContainerModel(IdentifiedModel):
        name: FullTextSearchedStringIndex
        parts: List['PartModel']
     
    class PartModel(PersistentModel, PartOfMixin[ContainerModel]):
        name: FullTextSearchedStringIndex

    update_forward_refs(ContainerModel, locals())

    model = ContainerModel(id=StrId('@'), 
                           version='0.1.0',
                           name=FullTextSearchedStringIndex('sample'),
                           parts=[
                               PartModel(name=FullTextSearchedStringIndex('part1')),
                               PartModel(name=FullTextSearchedStringIndex('part2')),
                           ])

    with use_temp_database_cursor_with_model(model, 
                                             keep_database_when_except=False) as cursor:

        new_version = allocate_audit_version(cursor, VersionInfo())

        model.id = StrId("@")
        query_and_args = get_query_and_args_for_reading(
            ContainerModel, ('id', 'name'), (('name', 'match', 'sample'),), 0,
            version=new_version)

        cursor.execute(*query_and_args)

        assert [{'id':'@', 'name':'sample'}] == cursor.fetchall()

        query_and_args = get_query_and_args_for_reading(
            PartModel, ('__row_id', '__json', 'name', '__valid_start'), tuple(), 0, 
            version=new_version)

        cursor.execute(*query_and_args)

        assert [
            {'__row_id':1, '__json':'{"name": "part1"}', 'name':'part1', '__valid_start':1}, 
            {'__row_id':2, '__json':'{"name": "part2"}', 'name':'part2', '__valid_start':1}
        ] == cursor.fetchall()


def test_get_query_and_args_for_reading_for_multiple_parts():
    class ContainerModel(IdentifiedModel):
        name: FullTextSearchedStringIndex
        parts: List['PartModel']
        part: 'PartModel'
        my_parts: List['PartModel']
     
    class PartModel(PersistentModel, PartOfMixin[ContainerModel]):
        name: FullTextSearchedStringIndex

    update_forward_refs(ContainerModel, locals())

    model = ContainerModel(id=StrId('@'), 
                           version='0.1.0',
                           name=FullTextSearchedStringIndex('sample'),
                           parts=[
                               PartModel(name=FullTextSearchedStringIndex('part1')),
                               PartModel(name=FullTextSearchedStringIndex('part2')),
                           ],
                           part=PartModel(name=FullTextSearchedStringIndex('part3')),
                           my_parts=[
                               PartModel(name=FullTextSearchedStringIndex('part4')),
                           ])

    with use_temp_database_cursor_with_model(model, 
                                             keep_database_when_except=False) as cursor:
        
        query_and_args = get_query_and_args_for_reading(
            PartModel, ('__row_id', '__json', 'name'), tuple(), 0, version=2)

        cursor.execute(*query_and_args)

        assert [
            {'__row_id':1, '__json':'{"name": "part1"}', 'name':'part1'}, 
            {'__row_id':2, '__json':'{"name": "part2"}', 'name':'part2'},
            # I don't know why __row_id:3 is missed.
            {'__row_id':4, '__json':'{"name": "part3"}', 'name':'part3'},
            {'__row_id':5, '__json':'{"name": "part4"}', 'name':'part4'},
        ] == cursor.fetchall()


def test_get_query_and_args_for_reading_for_nested_parts():
    class ContainerModel(IdentifiedModel):
        name: FullTextSearchedStringIndex
        part: 'PartModel'
     
    class PartModel(PersistentModel, PartOfMixin[ContainerModel]):
        name: FullTextSearchedStringIndex
        members: List['MemberModel']

    class MemberModel(PersistentModel, PartOfMixin[PartModel]):
        descriptions: StringArrayIndex

    update_forward_refs(ContainerModel, locals())
    update_forward_refs(PartModel, locals())

    model = ContainerModel(id=StrId('@'), 
                           version='0.1.0',
                           name=FullTextSearchedStringIndex('sample'),
                           part=PartModel(
                                name=FullTextSearchedStringIndex('part1'),
                                members=[
                                    MemberModel(descriptions=StringArrayIndex(['desc1'])),
                                    MemberModel(descriptions=StringArrayIndex(['desc1', 'desc2']))
                                ]
                           ))

    with use_temp_database_cursor_with_model(model, 
                                             keep_database_when_except=False) as cursor:
        model.id = StrId("@")
        query_and_args = get_query_and_args_for_reading(
            ContainerModel, ('id', 'name'), (('name', '=', 'sample'),), 0, version=2)

        cursor.execute(*query_and_args)

        assert [{'id':'@', 'name':'sample'}] == cursor.fetchall()

        query_and_args = get_query_and_args_for_reading(
            PartModel, ('__row_id', 'name'), tuple(), 0, version=2)

        cursor.execute(*query_and_args)

        assert [
            {'__row_id':1, 'name':'part1'}, 
        ] == cursor.fetchall()

        query_and_args = get_query_and_args_for_reading(
            MemberModel, ('__row_id', '__json', 'descriptions'), tuple(), 0, version=2)

        cursor.execute(*query_and_args)

        assert [
            {'__row_id':1, '__json':'{"descriptions": ["desc1"]}', 'descriptions':'["desc1"]'}, 
            {'__row_id':2, '__json':'{"descriptions": ["desc1", "desc2"]}', 'descriptions':'["desc1","desc2"]'}, 
        ] == cursor.fetchall()


def test_get_query_and_args_for_reading_for_external_index():
    class PartModel(PersistentModel):
        name: FullTextSearchedStringIndex
        codes: StringArrayIndex

    model = PartModel(
        name=FullTextSearchedStringIndex('part1'),
        codes= StringArrayIndex(['code1', 'code2'])
    )


    emptry_codes_model = PartModel(
        name=FullTextSearchedStringIndex('empty code'),
        codes= StringArrayIndex([])
    )
    with use_temp_database_cursor_with_model(model, emptry_codes_model,
                                             keep_database_when_except=False) as cursor:
        query_and_args = get_query_and_args_for_reading(
            PartModel, ('__row_id', 'name', 'codes'), (('codes', '=', 'code1'),), 
            0, unwind='codes', version=2)


        cursor.execute(*query_and_args)

        assert [
            {'__row_id':1, 'name':'part1', 'codes': 'code1'}
        ] == cursor.fetchall()

        query_and_args = get_query_and_args_for_reading(
            PartModel, ('__row_id', 'name', 'codes'), (('name', '=', 'part1'),), 0, version=2)

        cursor.execute(*query_and_args)

        assert [
            {'__row_id':1, 'name':'part1', 'codes': '["code1", "code2"]'}
        ] == cursor.fetchall()

        query_and_args = get_query_and_args_for_reading(
            PartModel, ('__row_id', 'name', 'codes'), (('name', '=', 'empty code'),), 
            0, unwind='codes', version=2)

        cursor.execute(*query_and_args)

        assert [
            {'__row_id':2, 'name':'empty code', 'codes': None}
        ] == cursor.fetchall()


def test_get_query_and_args_for_reading_for_stored_fields():
    class PartModel(PersistentModel):
        name: FullTextSearchedStringIndex
        members: List['MemberModel']

    class MemberModel(PersistentModel, PartOfMixin[PartModel]):
        _stored_fields: ClassVar[StoredFieldDefinitions] = {
            '_part_name': (('..', '$.name'), FullTextSearchedStringIndex)
        }
        descriptions: StringArrayIndex

    update_forward_refs(PartModel, locals())

    model = PartModel(
        name=FullTextSearchedStringIndex('part1'),
        members=[
            MemberModel(
                descriptions=StringArrayIndex(['desc1'])),
            MemberModel(descriptions=StringArrayIndex(
                ['desc1', 'desc2']))
        ]
    )

    with use_temp_database_cursor_with_model(model, 
                                             keep_database_when_except=False) as cursor:
        query_and_args = get_query_and_args_for_reading(
            MemberModel, ('__row_id', '__json', 'descriptions'), (('_part_name', '=', 'part1'),), 0,
            version=2)

        cursor.execute(*query_and_args)

        assert [
            {'__row_id':1, '__json':'{"descriptions": ["desc1"]}', 'descriptions':'["desc1"]'}, 
            {'__row_id':2, '__json':'{"descriptions": ["desc1", "desc2"]}', 'descriptions':'["desc1","desc2"]'}, 
        ] == cursor.fetchall()


def test_get_query_and_args_for_reading_for_explicit_external_index():
    class MemberModel(PersistentModel):
        name: str

    class PartModel(PersistentModel, PartOfMixin['ContainerModel']):
        _stored_fields: ClassVar[StoredFieldDefinitions] = {
            '_container_codes' : (('..', '$.codes'), StringArrayIndex),
            '_members_names' : (('$.members[*].name',), StringArrayIndex)
        }
        name: FullTextSearchedStringIndex
        members: List[MemberModel]

    class ContainerModel(PersistentModel):
        codes: StringArrayIndex
        parts: List[PartModel]

    update_forward_refs(PartModel, locals())
     
    model = ContainerModel(
        codes=StringArrayIndex(['code1', 'code2']),
        parts=[
           PartModel(
             name=FullTextSearchedStringIndex('part1'), 
             members=[MemberModel(name='part1-member1'), MemberModel(name='part1-member2')]
           )   
        ]
    )

    with use_temp_database_cursor_with_model(model, 
                                             keep_database_when_except=False) as cursor:
        query_and_args = get_query_and_args_for_reading(
            PartModel, 
            ('__row_id', 'name', '_container_codes', '_members_names'), 
            tuple(), 0,
            version=2,
            )

        cursor.execute(*query_and_args)

        assert [
            {'__row_id':1, 'name':'part1', '_container_codes': '["code1", "code2"]', '_members_names': '["part1-member1","part1-member2"]'}
        ] == cursor.fetchall()
 
        query_and_args = get_query_and_args_for_reading(
            PartModel, 
            ('__row_id', 'name', '_container_codes', '_members_names'), 
            tuple(), 0,
            unwind='_container_codes',
            version=2,
            )

        cursor.execute(*query_and_args)

        assert [
            {'__row_id':1, 'name':'part1', '_container_codes': "code1", '_members_names': '["part1-member1","part1-member2"]'},
            {'__row_id':1, 'name':'part1', '_container_codes': "code2", '_members_names': '["part1-member1","part1-member2"]'}
        ] == cursor.fetchall()
 
        query_and_args = get_query_and_args_for_reading(
            PartModel, 
            ('__row_id', 'name', '_container_codes', '_members_names'), 
            tuple(), 0,
            unwind=('_container_codes', '_members_names'),
            order_by=('_container_codes', '_members_names'),
            version=2,
        )

        cursor.execute(*query_and_args)

        assert [
            {'__row_id':1, 'name':'part1', '_container_codes': "code1", '_members_names': "part1-member1"},
            {'__row_id':1, 'name':'part1', '_container_codes': "code1", '_members_names': "part1-member2"},
            {'__row_id':1, 'name':'part1', '_container_codes': "code2", '_members_names': "part1-member1"},
            {'__row_id':1, 'name':'part1', '_container_codes': "code2", '_members_names': "part1-member2"},
        ] == cursor.fetchall()
 
        query_and_args = get_query_and_args_for_reading(
            PartModel, 
            ('__row_id', 'name', '_container_codes', '_members_names'), 
            (('_container_codes', '=', 'code1'), ('_members_names', '=', 'part1-member2')), 0,
            unwind=('_container_codes', '_members_names'),
            version=2,
        )

        cursor.execute(*query_and_args)

        assert [
            {'__row_id':1, 'name':'part1', '_container_codes': "code1", '_members_names': "part1-member2"},
        ] == cursor.fetchall()


def test_get_query_and_args_for_reading_for_reference():
    class PartModel(PersistentModel):
        name: StringIndex
        description: StringIndex

    class PartReference(StringReference[PartModel]):
        _target_field: ClassVar[str] = 'name'

    class PartInfoModel(PersistentModel):
        name: StringIndex
        part: PartReference
        codes: StringArrayIndex
        
    class ContainerModel(PersistentModel):
        name: StringIndex
        part: PartReference

    update_forward_refs(PartModel, locals())
     
    model = ContainerModel(
        name=StringIndex('container'),
        part=PartReference('part1')
    )

    part_info = PartInfoModel(
        name=StringIndex('part info'),
        part=PartReference('part1'),
        codes=StringArrayIndex(['code-1', 'code-2'])
    )

    part = PartModel(
        name=StringIndex('part1'),
        description=StringIndex('part 1')
    )

    with use_temp_database_cursor_with_model(model, part_info, part,
                                             keep_database_when_except=False) as cursor:
        # simple one
        query_and_args = get_query_and_args_for_reading(
            ContainerModel, 
            ('name', 'part'),
            tuple(), 0,
            version=2,
            )

        cursor.execute(*query_and_args)

        assert [
            {'name': 'container', 'part':'part1'}
        ] == cursor.fetchall()

        # join part model
        query_and_args = get_query_and_args_for_reading(
            ContainerModel, 
            ('name', 'part.name', 'part.description'),
            tuple(), 0,
            version=2,
            )

        cursor.execute(*query_and_args)

        assert [
            {'name': 'container', 'part.name':'part1', 'part.description':'part 1'}
        ] == cursor.fetchall()

        # explicit join
        query_and_args = get_query_and_args_for_reading(
            ContainerModel, 
            ('name', 'part.name', 'part.part_info.name'),
            tuple(), 0, 
            ns_types={'part.part_info':PartInfoModel},
            version=2,
            )

        cursor.execute(*query_and_args)

        assert [
            {'name': 'container', 'part.name':'part1', 'part.part_info.name':'part info'}
        ] == cursor.fetchall()

        query_and_args = get_query_and_args_for_reading(
            ContainerModel, 
            ('name', 'part.name', 'part.part_info.name'),
            (('part.part_info.codes', '=', 'code-1'), ), 0,
            ns_types={'part.part_info':PartInfoModel},
            unwind="part.part_info.codes",
            version=2,
        )
            
        cursor.execute(*query_and_args)

        assert [
            {'name': 'container', 'part.name':'part1', 'part.part_info.name':'part info'}
        ] == cursor.fetchall()


def test_get_query_and_args_for_reading_for_reference_with_base_type():
    class PartModel(PersistentModel):
        name: StringIndex
        description: str

    class PartReference(StringReference[PartModel]):
        _target_field: ClassVar[str] = 'name'

    class PartInfoModel(PersistentModel):
        name: StringIndex
        part: PartReference
        
    class PartAttrModel(PersistentModel):
        name: StringIndex
        part: PartReference

    update_forward_refs(PartModel, locals())
     
    models = [
        PartModel(name=StringIndex('part-1'), description='part 1 description'),
        PartModel(name=StringIndex('part-3'), description='part 4 description'),
        PartInfoModel(name=StringIndex('part-1 info'), part=PartReference('part-1')),
        PartInfoModel(name=StringIndex('part-2 info'), part=PartReference('part-2')),
        PartAttrModel(name=StringIndex('part-1 attr'), part=PartReference('part-1')),
        PartAttrModel(name=StringIndex('part-3 attr'), part=PartReference('part-3')),
    ]

    with use_temp_database_cursor_with_model(*models,
                                             keep_database_when_except=False) as cursor:
        # simple one
        query_and_args = get_query_and_args_for_reading(
            PartModel, ('name'), tuple(), 0,
            version=2)

        cursor.execute(*query_and_args)

        assert [
            {'name': 'part-1'},
            {'name': 'part-3'},
        ] == cursor.fetchall()

        query_and_args = get_query_and_args_for_reading(
            PartModel, ('name', 'info.name', 'attr.name'), tuple(),  0,
            ns_types={'info':PartInfoModel, 'attr':PartAttrModel},
            version=2)

        cursor.execute(*query_and_args)

        assert [
            {'name': 'part-1', 'info.name': 'part-1 info', 'attr.name': 'part-1 attr'},
        ] == cursor.fetchall()

        query_and_args = get_query_and_args_for_reading(
            PartModel, ('name', 'attr.name'), tuple(), 0,
            ns_types={'attr':PartInfoModel}, 
            base_type=PartInfoModel,
            version=2)

        cursor.execute(*query_and_args)

        assert [
            {'name': 'part-1', 'attr.name': 'part-1 info'},
            {'name': None, 'attr.name': 'part-2 info'},
        ] == cursor.fetchall()

        query_and_args = get_query_and_args_for_reading(
            PartModel, ('name', 'attr.name'), (('name', 'is null', None),),  0,
            ns_types={'attr':PartInfoModel}, 
            base_type=PartInfoModel,
            version=2)

        cursor.execute(*query_and_args)

        assert [
            {'name': None, 'attr.name': 'part-2 info'},
        ] == cursor.fetchall()
        

        query_and_args = get_query_and_args_for_reading(
            PartModel, ('name', 'attr.name'), tuple(), 0, 
            ns_types={'attr': PartInfoModel},
            base_type=PartModel,
            version=2)

        cursor.execute(*query_and_args)

        assert [
            {'name': 'part-1', 'attr.name': 'part-1 info'},
        ] == cursor.fetchall()


def test_get_query_and_args_for_reading_for_where_is_null():
    class PartModel(PersistentModel):
        name: StringIndex
        description: str

    class PartReference(StringReference[PartModel]):
        _target_field: ClassVar[str] = 'name'

    class PartInfoModel(PersistentModel):
        name: StringIndex
        part: PartReference
        
    class PartAttrModel(PersistentModel):
        name: StringIndex
        part: PartReference

    update_forward_refs(PartModel, locals())
     
    models = [
        PartModel(name=StringIndex('part-1'), description='part 1 description'),
        PartModel(name=StringIndex('part-3'), description='part 4 description'),
        PartInfoModel(name=StringIndex('part-1 info'), part=PartReference('part-1')),
        PartInfoModel(name=StringIndex('part-2 info'), part=PartReference('part-2')),
        PartAttrModel(name=StringIndex('part-1 attr'), part=PartReference('part-1')),
        PartAttrModel(name=StringIndex('part-3 attr'), part=PartReference('part-3')),
    ]

    with use_temp_database_cursor_with_model(*models,
                                             keep_database_when_except=False) as cursor:
        # simple one
        query_and_args = get_query_and_args_for_reading(
            PartModel, ('name', 'attr.name'), (('name', 'is null', None),), 0,
            ns_types={'attr':PartInfoModel}, 
            base_type=PartInfoModel, version=2)

        cursor.execute(*query_and_args)

        assert [
            {'name': None, 'attr.name': 'part-2 info'},
        ] == cursor.fetchall()
        

def test_get_query_and_args_for_counting():
    class ContainerModel(IdentifiedModel):
        name: FullTextSearchedStringIndex
        parts: List['PartModel']
        part: 'PartModel'
        my_parts: List['PartModel']
     
    class PartModel(PersistentModel, PartOfMixin[ContainerModel]):
        name: FullTextSearchedStringIndex

    update_forward_refs(ContainerModel, locals())

    model = ContainerModel(id=StrId('@'), 
                           version='0.1.0',
                           name=FullTextSearchedStringIndex('sample'),
                           parts=[
                               PartModel(name=FullTextSearchedStringIndex('part1')),
                               PartModel(name=FullTextSearchedStringIndex('part2')),
                           ],
                           part=PartModel(name=FullTextSearchedStringIndex('part3')),
                           my_parts=[
                               PartModel(name=FullTextSearchedStringIndex('part4')),
                           ])

    with use_temp_database_cursor_with_model(model,
                                             keep_database_when_except=False) as cursor:

        query_and_args = get_query_and_args_for_reading(
            PartModel, ('*',), tuple(), 0,
            base_type=PartModel, for_count=True,
            version=2)

        cursor.execute(*query_and_args)

        assert [
            {'COUNT':4}
        ] == cursor.fetchall()


def test_get_query_and_args_for_reading_with_limit():
    class EntryModel(PersistentModel):
        name: StringIndex
        description: str

    models = [
        EntryModel(name=StringIndex('part-1'), description='part 1 description'),
        EntryModel(name=StringIndex('part-2'), description='part 2 description'),
        EntryModel(name=StringIndex('part-3'), description='part 3 description'),
        EntryModel(name=StringIndex('part-4'), description='part 4 description'),
        EntryModel(name=StringIndex('part-5'), description='part 5 description'),
        EntryModel(name=StringIndex('part-6'), description='part 6 description'),
        EntryModel(name=StringIndex('part-7'), description='part 7 description'),
        EntryModel(name=StringIndex('part-8'), description='part 8 description'),
    ]

    with use_temp_database_cursor_with_model(*models,
                                             keep_database_when_except=False) as cursor:
        # simple one
        query_and_args = get_query_and_args_for_reading(
            EntryModel, ('name',), tuple(), 0,
            offset=2, limit=4, order_by='name',
            version=2)

        cursor.execute(*query_and_args)

        assert [
            {'name': 'part-3'},
            {'name': 'part-4'},
            {'name': 'part-5'},
            {'name': 'part-6'},
        ] == cursor.fetchall()
        

def test_seq_id():
    class RiskIdStr(SequenceStrId):
        prefix = 'Q'

    class Model(PersistentModel):
        seq_1: SequenceStrId = Field(default=SequenceStrId(''))
        seq_2: RiskIdStr = Field(default=RiskIdStr(''))
        name: StringIndex


    models = [
        Model(name=StringIndex('first'), seq_2=RiskIdStr('QQ1')),
        Model(name=StringIndex('second')),
        Model(name=StringIndex('third'))
    ]

    with use_temp_database_cursor_with_model(*models,
                                             keep_database_when_except=False) as cursor:

        # simple one
        query_and_args = get_query_and_args_for_reading(
            Model, ('name', 'seq_1', 'seq_2'), tuple(), 0,
            version=2)

        cursor.execute(*query_and_args)

        assert [
            {'name': 'first', 'seq_1': 'C1', 'seq_2': 'QQ1'},
            {'name': 'second', 'seq_1': 'C2', 'seq_2': 'Q1'},
            {'name': 'third', 'seq_1': 'C3', 'seq_2': 'Q2'},
        ] == cursor.fetchall()
        

def test_use_base_class_table_mixin():
    class BaseModel(PersistentModel):
        name: StringIndex

    class DerivedModel(BaseModel, UseBaseClassTableMixin):
        description: StringIndex

    models = [
        DerivedModel(description=StringIndex('hello'), name=StringIndex('first')),
        BaseModel(name=StringIndex('second'))
    ]

    with use_temp_database_cursor_with_model(*models,
                                             keep_database_when_except=False) as cursor:
        # simple one
        query_and_args = get_query_and_args_for_reading(
            BaseModel, ('name','__json'), tuple(), 0, version=2)

        cursor.execute(*query_and_args)

        assert [
            {'name': 'first', '__json':'{"name":"first","description":"hello"}'},
            {'name': 'second', '__json':'{"name":"second"}'}
        ] == cursor.fetchall()

        with pytest.raises(RuntimeError, match='cannot make query for UseBaseClassTableMixin'):
            get_query_and_args_for_reading(DerivedModel, ('*',), tuple(), 0)