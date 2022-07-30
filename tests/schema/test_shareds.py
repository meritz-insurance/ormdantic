from typing import List
import pytest
from pydantic import Field

from ormdantic.schema.base import (
    PersistentModel, SchemaBaseModel
)
from ormdantic.schema.shareds import (
    SharedContentMixin, ContentReferenceModel, 
    collect_shared_model_type_and_ids, concat_shared_models, 
    extract_shared_models, extract_shared_models_for, get_shared_content_types
)
from ormdantic.util.tools import digest

class MyContent(SharedContentMixin):
    name:str

class MyDerivedContent(MyContent):
    pass

class MyAnotherContent(SharedContentMixin):
    name:str

class MySharedModel(ContentReferenceModel[MyContent]):
    code:str = Field(default='')

class MyDerivedSharedModel(MySharedModel):
    name:str

class MySharedAnotherModel(ContentReferenceModel[MyAnotherContent]):
    code:str

class Container(SchemaBaseModel):
    items: List[MySharedModel]

class ComplexContainer(SchemaBaseModel):
    items: List[MySharedModel]
    other_items: List[MySharedAnotherModel]

class MyPersistentModel(PersistentModel):
    items: List[MySharedModel]


def test_refresh_id(monkeypatch:pytest.MonkeyPatch):
    content = MyContent(name='name')

    assert content.id == digest('{"name":"name"}')

    normalized_called = False

    def new_normalized(self):
        nonlocal normalized_called
        normalized_called = True

    monkeypatch.setattr(MyContent, 'normalize', new_normalized)

    assert content.refresh_id() == digest('{"name":"name"}')
    assert normalized_called


def test_get_content_id():
    model = MySharedModel(content='1', code='1')

    assert '1' == model.content
    assert '1' == model.get_content_id()

    model.content = MyContent(name='name')
    assert model.content.id == model.get_content_id()


def test_get_content():
    content = MyContent(name='name')
    model = MySharedModel(content=content, code='1')

    assert content == model.get_content()

    extract_shared_models(model, True)

    with pytest.raises(RuntimeError, match='.*cannot get the content.*'):
        model.get_content()


def test_wrong_using_content_reference_model():
    with pytest.raises(TypeError, match='.*requires a parameter class.*'):
        class MissingParameter(ContentReferenceModel):
            pass


def test_parameter_type_will_be_created_for_parsing():
    model = MySharedModel.parse_obj(
        {
            'content': {'name':'name'}
        }
    )

    assert model.get_content_id() == digest('{"name":"name"}', 'sha1')
    model.content = MyContent.parse_obj({'name':'name'})

def test_extract_shared_models():
    content1 = MyContent(name='name1')
    content2 = MyContent(name='name2')

    shared_content1 = MySharedModel.parse_obj({
        'version': '0',
        'code': 'code1',
        'content': {'name':'name1'}
    })

    shared_content2 = MySharedModel.parse_obj({
        'version': '0',
        'code': 'code2',
        'content': {'name': 'name2'}
    })

    container = Container(items=[
        shared_content1, shared_content2
    ])

    assert {
        content1.id:{MyContent:content1},
        content2.id:{MyContent:content2}
    } == extract_shared_models(container)

    assert container.items[0].content == content1

    extract_shared_models(container, True)

    assert container.items[0].content == content1.id
    assert container.items[1].content == content2.id

    assert {} == extract_shared_models(content1)


def test_extract_shared_models_for():
    content1 = MyContent(name='name1')
    content2 = MyDerivedContent(name='name2')

    other_content1 = MyAnotherContent(name='name1')

    container = ComplexContainer(
        items=[
            MySharedModel.parse_obj({
                'version': '0',
                'code': 'code1',
                'content': content1
            }),
            MySharedModel.parse_obj({
                'version': '0',
                'code': 'code2',
                'content': content2
            }),
            MyDerivedSharedModel.parse_obj({
                'version': '0',
                'code': 'code2',
                'name': 'derived',
                'content': content2
            })
        ],
        other_items=[
            MySharedAnotherModel.parse_obj({
                'code': 'code2',
                'name': 'derived',
                'content': other_content1
            })
        ]
    )

    assert {
        content1.id:content1, 
        content2.id:content2
    } == extract_shared_models_for(container, MyContent)

    assert {
        other_content1.id:other_content1
    } == extract_shared_models_for(container, MyAnotherContent, True)

    assert {
    } == extract_shared_models_for(container, MyAnotherContent)


def test_extract_shared_models_for_with_multiple_item_with_same_id():
    content1 = MyContent(name='name1')
    content2 = MyDerivedContent(name='name1')

    container = ComplexContainer(
        items=[
            MySharedModel.parse_obj({
                'version': '0',
                'code': 'code1',
                'content': content1
            }),
            MySharedModel.parse_obj({
                'version': '0',
                'code': 'code2',
                'content': content2
            }),
        ],
        other_items=[]
    )

    with pytest.raises(RuntimeError, match='.*different type.*'):
        extract_shared_models_for(container, MyContent)

    extract_shared_models_for(container, MyDerivedContent)
     

def test_concat_shared_models():
    content1 = MyContent(name='name1')
    content2 = MyContent(name='name2')

    shared_content1 = MySharedModel.parse_obj({
        'version': '0',
        'code': 'code1',
        'content': content1.id
    })
 

    shared_content2 = MySharedModel.parse_obj({
        'version': '0',
        'code': 'code2',
        'content': content2.id
    })

    container = Container(items=[
        shared_content1, shared_content2
    ])

    assert {
    } == extract_shared_models(container)

    contents = {
        content1.id:{MyContent:content1},
        content2.id:{MyContent:content2}
    } 

    concat_shared_models(container, contents)

    assert contents == extract_shared_models(container)

    extract_shared_models(container, True)

    concat_shared_models(container, {
        content1.id:content1,
        content2.id:content2
    })

    assert contents == extract_shared_models(container)

def test_collect_shared_model_ids():
    content1 = MyContent(name='name1')
    content2 = MyContent(name='name2')

    other_content1 = MyAnotherContent(name='name1')

    container = ComplexContainer(
        items=[
            MySharedModel.parse_obj({
                'version': '0',
                'code': 'code1',
                'id': content1.id
            }),
            MySharedModel.parse_obj({
                'version': '0',
                'code': 'code2',
                'id': content2.id
            }),
            MyDerivedSharedModel.parse_obj({
                'version': '0',
                'code': 'code2',
                'name': 'derived',
                'id': content2.id
            })
        ],
        other_items=[
            MySharedAnotherModel.parse_obj({
                'code': 'code2',
                'name': 'derived',
                'id': other_content1.id
            })
        ]
    )

    assert {
        MyContent: {content1.id, content2.id},
        MyAnotherContent: {other_content1.id}
    } == collect_shared_model_type_and_ids(container)


def test_collect_shared_model_ids():
    content1 = MyContent(name='name1')
    content2 = MyContent(name='name2')

    other_content1 = MyAnotherContent(name='name1')

    container = ComplexContainer(
        items=[
            MySharedModel.parse_obj({
                'version': '0',
                'code': 'code1',
                'content': content1.id
            }),
            MySharedModel.parse_obj({
                'version': '0',
                'code': 'code2',
                'content': content2.id
            }),
            MyDerivedSharedModel.parse_obj({
                'version': '0',
                'code': 'code2',
                'name': 'derived',
                'content': content2.id
            })
        ],
        other_items=[
            MySharedAnotherModel.parse_obj({
                'code': 'code2',
                'name': 'derived',
                'content': other_content1.id
            })
        ]
    )

    assert {
        MyContent: {content1.id, content2.id},
        MyAnotherContent: {other_content1.id}
    } == collect_shared_model_type_and_ids(container)


def test_get_shared_content_types():
    assert (MyContent, MyAnotherContent) == get_shared_content_types(ComplexContainer)
 