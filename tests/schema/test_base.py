from typing import Any, cast, List, Tuple, Type, Annotated
import uuid

import pytest
from pydantic import ConstrainedStr, parse_raw_as

from ormdantic.schema import (
    IdentifiedModel
)
from ormdantic.schema.base import (
    is_derived_from,
    AutoAllocatedMixin, PersistentModel, PartOfMixin, 
    allocate_fields_if_empty, get_container_type, 
    get_field_name_and_type, get_identifer_of, get_field_names_for, UuidStr,
    is_field_list_or_tuple_of, get_field_type,
    get_root_container_type, get_field_name_and_type_for_annotated,
    MetaStoredField, MetaIndexField, MetaIdentifyingField,
    get_stored_fields_for
)
from ormdantic.schema.typed import (BaseClassTableModel, get_type_for_table)

def test_identified_model():
    model = IdentifiedModel(id=UuidStr(uuid.UUID(int=0).hex), version='0.0.0')

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
    

def test_get_stored_fields_for():
    class Container(PersistentModel):
        stored:Annotated[str, MetaStoredField()]
        index:Annotated[str, MetaIndexField()]
        identifying:Annotated[str, MetaIdentifyingField()]

    assert {
        'stored':(('$.stored',), Annotated[str, MetaStoredField()]),
        'index':(('$.index',), Annotated[str, MetaIndexField()]),
        'identifying':(('$.identifying',), Annotated[str, MetaIdentifyingField()]),
        } == get_stored_fields_for(Container, MetaStoredField)

    with pytest.raises(RuntimeError, match='.*invalid*'):
        get_field_type(cast(Type[Container], str), 'name')

  

def test_get_field_type():
    class Container(PersistentModel):
        name:str

    assert str == get_field_type(Container, 'name')

    with pytest.raises(RuntimeError, match='.*invalid*'):
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


def test_get_field_name_and_type_for_annotated():
    class Part(PersistentModel, PartOfMixin['Container']):
        pass

    class Container(PersistentModel):
        parts: Annotated[List[Part], MetaStoredField()]
        part: Part
        number: Annotated[int, MetaIndexField()]

    assert [
        ('parts', Annotated[List[Part], MetaStoredField()]), 
        ('number', Annotated[int, MetaIndexField()])
    ] == list(get_field_name_and_type_for_annotated(Container, MetaStoredField))

    class WrongType():
        pass

    with pytest.raises(RuntimeError):
        list(get_field_name_and_type_for_annotated(cast(Any, WrongType)))


def test_new_if_empty_raise_exception():
    class NotImplementedStr(ConstrainedStr, AutoAllocatedMixin):
        pass

    with pytest.raises(NotImplementedError):
        NotImplementedStr().new_if_empty()


def test_get_identifier_of():
    class SimpleModel(PersistentModel):
        id: Annotated[UuidStr, MetaIdentifyingField()] = UuidStr(uuid.UUID(int=0).hex)

    model = SimpleModel()

    assert {'id':'00000000000000000000000000000000'} == dict(get_identifer_of(model))


def test_is_fields_collection_type():
    class SimpleModel(PersistentModel):
        list_id: List[str]
        tuple_id: Tuple[str,...]
    
    assert is_field_list_or_tuple_of(SimpleModel, 'list_id', str)
    assert not is_field_list_or_tuple_of(SimpleModel, 'list_id', int)
    assert is_field_list_or_tuple_of(SimpleModel, 'tuple_id', str)


def test_allocate_fields_if_empty():
    class SimpleModel(IdentifiedModel):
        pass

    model = SimpleModel(id=UuidStr(''), version='')

    replaced = allocate_fields_if_empty(model)

    assert replaced is not model
    assert model.id == ''
    assert replaced.id

    replaced = allocate_fields_if_empty(model, True)

    assert replaced is model
    assert model.id != ''
    assert replaced.id
  

def test_allocate_fields_if_empty_for_vector():
    class SimpleModel(PersistentModel):
        list_ids : List[UuidStr] 
        tuple_ids : Tuple[UuidStr,...]
        empty_ids : List[UuidStr]

    model = SimpleModel(
        list_ids=[UuidStr('')], 
        tuple_ids=(UuidStr(''), ),
        empty_ids=[]
    )

    replaced = allocate_fields_if_empty(model)

    assert replaced.list_ids != model.list_ids
    assert replaced.tuple_ids != model.tuple_ids
    assert replaced.empty_ids is model.empty_ids


def test_get_type_for_table():
    class TableModel(BaseClassTableModel, PersistentModel):
        pass

    class DerivedTableModel(TableModel):
        pass



    assert TableModel == get_type_for_table(TableModel)
    assert TableModel == get_type_for_table(DerivedTableModel)

    assert PersistentModel == get_type_for_table(PersistentModel)