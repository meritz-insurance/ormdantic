from typing import List, Tuple

import pytest
from ormdantic.schema.typed import TypeNamedModel, get_type_named_model_type, parse_obj_for_model

class MySubModel(TypeNamedModel):
    name: str

class MyDerivedModel(MySubModel):
    pass

class MyTypeNamedModel(TypeNamedModel):
    items : List[MySubModel]
    objs : Tuple[MySubModel, ...]


def test_get_type_named_model_type():
    assert MyTypeNamedModel == get_type_named_model_type('MyTypeNamedModel')


def test_duplicate_type_named_model():
    with pytest.raises(RuntimeError):
        class MyTypeNamedModel(TypeNamedModel):
            pass


def test_parse_obj_for_model():
    model = parse_obj_for_model({
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
    })

    assert len(model.items) == 2
    assert model.items[0] == MySubModel(name='my sub model')
    assert model.items[1] == MyDerivedModel(name='my derived model')

    assert model.objs[0] == MySubModel(name='my sub model')
    assert model.objs[1] == MyDerivedModel(name='my derived model')

