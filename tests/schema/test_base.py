from typing import Any, cast, List, Tuple, Type
import uuid

import pytest
from pydantic import ConstrainedStr, parse_raw_as

from ormdantic.schema.base import (
    IdentifiedModel, IdentifyingMixin, PersistentModel, PartOfMixin, UseBaseClassTableMixin, 
    assign_identifying_fields_if_empty, get_container_type, 
    get_field_name_and_type, get_identifer_of, get_field_names_for, StrId, DateId, get_type_for_table,
    update_forward_refs, is_field_list_or_tuple_of, get_field_type,
    get_root_container_type
)

def test_identified_model():
    model = IdentifiedModel(id=StrId(uuid.UUID(int=0).hex), version='0.0.0')

    data = model.json()

    assert model == parse_raw_as(type(model), data.encode())


def test_get_container_type():
    class Container(PersistentModel):
        pass

    class Part(PersistentModel, PartOfMixin[Container]):
        pass

    assert Container is get_container_type(Part)
    assert None is get_container_type(Container)


def test_get_root_container_type():
    class Container(PersistentModel):
        pass

    class Part(PersistentModel, PartOfMixin[Container]):
        pass

    class PartOfPart(PersistentModel, PartOfMixin[Part]):
        pass

    assert Container is get_container_type(Part)
    assert Container is get_root_container_type(Part)
    assert Container is get_root_container_type(PartOfPart)

    assert None is get_container_type(Container)
    

   

def test_get_field_type():
    class Container(PersistentModel):
        name:str

    assert str == get_field_type(Container, 'name')

    with pytest.raises(RuntimeError, match='.*Invalid*'):
        get_field_type(cast(Type[Container], str), 'name')


def test_get_part_field_name():
    class Part(PersistentModel, PartOfMixin['Container']):
        pass

    class NotPart(PersistentModel, PartOfMixin['Container']):
        pass

    class Container(PersistentModel):
        part:Part

    assert ('part',) == get_field_names_for(Container, Part)
    assert tuple() == get_field_names_for(Container, NotPart)

    class MultipleFieldsContainer(PersistentModel):
        part1:Part
        part2:List[Part]

    assert ('part1', 'part2') == get_field_names_for(MultipleFieldsContainer, Part)


def test_get_field_name_and_type():
    class Part(PersistentModel, PartOfMixin['Container']):
        pass

    class Container(PersistentModel):
        parts: List[Part]
        part: Part
        number: int

    assert [('parts', List[Part]), ('part', Part), ('number', int)] == list(get_field_name_and_type(Container))
    assert [('parts', List[Part]), ('part', Part)] == list(get_field_name_and_type(Container, PartOfMixin))

    class WrongType():
        pass

    with pytest.raises(RuntimeError):
        list(get_field_name_and_type(cast(Any, WrongType)))

def test_new_if_empty():
    date = DateId(2020, 1, 1)

    assert date == date.new_if_empty() 

def test_new_if_empty_raise_exception():
    class NotImplementedStr(ConstrainedStr, IdentifyingMixin):
        pass

    with pytest.raises(NotImplementedError):
        NotImplementedStr().new_if_empty()


def test_get_identifier_of():
    class SimpleModel(PersistentModel):
        id: StrId = StrId(uuid.UUID(int=0).hex)

    model = SimpleModel()

    assert {'id':'00000000000000000000000000000000'} == dict(get_identifer_of(model))


def test_is_fields_collection_type():
    class SimpleModel(PersistentModel):
        list_id: List[str]
        tuple_id: Tuple[str,...]
    
    assert is_field_list_or_tuple_of(SimpleModel, 'list_id', str)
    assert not is_field_list_or_tuple_of(SimpleModel, 'list_id', int)
    assert is_field_list_or_tuple_of(SimpleModel, 'tuple_id', str)


def test_assign_identified_if_empty():
    class SimpleModel(IdentifiedModel):
        pass

    model = SimpleModel(id=StrId(''), version='')

    replaced = assign_identifying_fields_if_empty(model)

    assert replaced is not model
    assert model.id == ''
    assert replaced.id

    replaced = assign_identifying_fields_if_empty(model, True)

    assert replaced is model
    assert model.id != ''
    assert replaced.id
  

def test_assign_identified_if_empty_for_vector():
    class SimpleModel(PersistentModel):
        list_ids : List[StrId] 
        tuple_ids : Tuple[StrId,...]
        empty_ids : List[StrId]

    model = SimpleModel(
        list_ids=[StrId('')], 
        tuple_ids=(StrId(''), ),
        empty_ids=[]
    )

    replaced = assign_identifying_fields_if_empty(model)

    assert replaced.list_ids != model.list_ids
    assert replaced.tuple_ids != model.tuple_ids
    assert replaced.empty_ids is model.empty_ids


def test_assign_identified_if_empty_for_parts():
    class ContainerModel(IdentifiedModel):
        parts : List['SimpleModel']
        part : 'SimpleModel'

    class SimpleModel(IdentifiedModel, PartOfMixin[ContainerModel]):
        pass

    update_forward_refs(ContainerModel, locals())

    model = ContainerModel(
        id = StrId('Container'),
        version = '0.0.0',
        parts = [
            SimpleModel(id=StrId('1'), version=''),
            SimpleModel(id=StrId('1'), version='')
        ],
        part = SimpleModel(id=StrId('1'), version='')
    )

    replaced = assign_identifying_fields_if_empty(model)

    assert replaced is model
    
    model = ContainerModel(
        id = StrId('Container'),
        version = '0.0.0',
        parts = [
            SimpleModel(id=StrId('1'), version=''),
            SimpleModel(id=StrId('1'), version='')
        ],
        part = SimpleModel(id=StrId(''), version='')
    )

    replaced = assign_identifying_fields_if_empty(model)

    assert replaced is not model
    assert replaced.parts is model.parts
    assert replaced.part != model.part
    
    model = ContainerModel(
        id = StrId('Container'),
        version = '0.0.0',
        parts = [
            SimpleModel(id=StrId('1'), version=''),
            SimpleModel(id=StrId(''), version='')
        ],
        part = SimpleModel(id=StrId('1'), version='')
    )

    replaced = assign_identifying_fields_if_empty(model)

    assert replaced is not model
    assert replaced.parts is not model.parts
    assert replaced.parts[0] is model.parts[0]
    assert replaced.parts[1] != model.parts[1]
    assert replaced.part is model.part

    replaced = assign_identifying_fields_if_empty(model, True)

    assert replaced is model

    replaced = assign_identifying_fields_if_empty(model)

    assert replaced is model


def test_get_type_for_table():
    class TableModel(PersistentModel):
        pass

    class DerivedModel(TableModel, UseBaseClassTableMixin):
        pass

    class WrongModel(UseBaseClassTableMixin):
        pass

    assert TableModel == get_type_for_table(TableModel)
    assert TableModel == get_type_for_table(DerivedModel)

    with pytest.raises(RuntimeError):
        get_type_for_table(WrongModel)