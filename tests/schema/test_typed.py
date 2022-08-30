from typing import List, Tuple, Union

from ormdantic import PersistentSharedContentModel, ContentReferenceModel
import pytest
from ormdantic.schema.typed import TypeNamedModel, get_type_named_model_type, parse_object_for_model

class MySubModel(TypeNamedModel):
    name: str

class MyDerivedModel(MySubModel):
    pass

class MyTypeNamedModel(TypeNamedModel):
    items : List[MySubModel]
    objs : Tuple[MySubModel, ...]

class Flag(PersistentSharedContentModel, TypeNamedModel):
	color: str

class FlagReferenceModel(ContentReferenceModel[Flag]):
    pass

class UseFlag(TypeNamedModel):
    flags: List[FlagReferenceModel]


class UnionList(TypeNamedModel):
    items: Union[List[FlagReferenceModel], str]

class MyTuple(TypeNamedModel):
    items: Tuple[FlagReferenceModel, str, Union[str, int], None]


def test_get_type_named_model_type():
    assert MyTypeNamedModel == get_type_named_model_type('MyTypeNamedModel')


def test_duplicate_type_named_model():
    with pytest.raises(RuntimeError):
        class MyTypeNamedModel(TypeNamedModel):
            pass


@pytest.mark.parametrize('message, expected, object', [
    (
        'tuple and list',
        MyTypeNamedModel(
            items=[
                MySubModel(name='my sub model'),
                MyDerivedModel(name='my derived model')
            ],
            objs=(
                MySubModel(name='my sub model'),
                MyDerivedModel(name='my derived model')
            )
        ),
        {
            'type_name': 'MyTypeNamedModel',
            'items':[
                {
                    'type_name': 'MySubModel',
                    'name': 'my sub model'
                },
                {
                    'type_name': 'MyDerivedModel',
                    'name': 'my derived model'
                }
            ],
            'objs':(
                {
                    'type_name': 'MySubModel',
                    'name': 'my sub model'
                },
                {
                    'type_name': 'MyDerivedModel',
                    'name': 'my derived model'
                }
            )
        }
    ),
    (
        'shared content',
        UseFlag(
            flags=[
                FlagReferenceModel(content=
                    Flag(color='red')
                ),
                FlagReferenceModel(content=
                    Flag(color='blue')
                )
            ]
        ),
        {
            'type_name': 'UseFlag',
            'flags': [
                { 
                    'content': {
                        'color': 'red'
                    } 
                },
                { 
                    'content': {
                        'color': 'blue'
                    } 
                },
            ]
        }
    ),
    (
        'union has list',
        UnionList(
            items=[FlagReferenceModel(content=Flag(color='blue'))]
        ),
        {
            'type_name': 'UnionList',
            'items': [
                { 
                    'content': {
                        'color': 'blue'
                    } 
                }
            ]
        }
    ),
    (
        'union has list',
        UnionList(
            items='Hello'
        ),
        {
            'type_name': 'UnionList',
            'items': 'Hello'
        }
    ),
    (
        'tuple with various type',
        MyTuple(
            items=(
                FlagReferenceModel(content=Flag(color='blue')),
                'hello',
                2,
                None
            )
        ),
        {
            'type_name': 'MyTuple',
            'items': [
                {
                    'content': {
                        'color': 'blue'
                    }
                },
                'hello',
                2,
                None
            ]
        }
    )
])
def test_parse_obj_for_model(message, expected, object):
    assert expected == parse_object_for_model(object)
