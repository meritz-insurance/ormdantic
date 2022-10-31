from operator import ne
from typing import List, ClassVar, Dict, Any
import pytest
from pydantic import Field

from ormdantic.schema.base import (
    PersistentModel, PersistentModel
)
from ormdantic.schema.shareds import (
    SharedContentMixin, ContentReferenceModel, 
    collect_shared_model_field_type_and_ids, populate_shared_models, 
    extract_shared_models, extract_shared_models_for, get_shared_content_types
)
from ormdantic.schema.modelcache import ModelCache
from ormdantic.util.tools import digest

class MyContent(SharedContentMixin, PersistentModel):
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


class Container(PersistentModel):
    items: List[MySharedModel]


class ComplexContainer(PersistentModel):
    items: List[MySharedModel]
    other_items: List[MySharedAnotherModel]


class MyPersistentModel(PersistentModel):
    items: List[MySharedModel]


class MyNestedModel(SharedContentMixin, PersistentModel):
    shared_model: MySharedModel


class MyNestedReferenceModel(ContentReferenceModel[MyNestedModel]):
    pass


class MyNestedNestedModel(SharedContentMixin, PersistentModel):
    nested_ref_model: MyNestedReferenceModel


class MyCode(SharedContentMixin):
    _id_attr: ClassVar[str] = 'code'
    code: str

class MyProduct(PersistentModel):
    code: str


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


def test_refresh_id_with_id_attr(monkeypatch:pytest.MonkeyPatch):
    content = MyCode(code='code')

    assert content.id == 'code'

    content.code = 'Q12'

    assert content.refresh_id() == 'Q12'


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
    assert MyContent == model.get_content_type()

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
        content1.id:{MySharedModel:content1},
        content2.id:{MySharedModel:content2}
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
     

def test_populate_shared_models():
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

    with pytest.raises(RuntimeError, match='cannot populate shared model.'):
        populate_shared_models(container, ModelCache())

    contents = {
        content1.id: {MySharedModel:content1},
        content2.id: {MySharedModel:content2},
    }

    populated = populate_shared_models(container, ModelCache(entries={
        (content1.id,):{MySharedModel:content1},
        (content2.id,):{MySharedModel:content2}
    }))

    assert {} == extract_shared_models(container)
    assert contents == extract_shared_models(populated)

    extract_shared_models(container, True)

    populated = populate_shared_models(container, ModelCache(entries={
        (content1.id,):{MySharedModel:content1},
        (content2.id,):{MySharedModel:content2}
    }))

    assert contents == extract_shared_models(populated)


def test_populate_shared_model_return_self_if_no_reference():
    content = MyContent(name='test')
    nested_content = MyNestedModel(shared_model=MySharedModel(content=content.id))

    model_cache = ModelCache(entries={
        (content.id,):{MyContent: content},
        (nested_content.id,): {MyNestedModel: nested_content}
    })

    assert content is populate_shared_models(content, model_cache)

    assert nested_content is not populate_shared_models(nested_content, model_cache)
    assert content is populate_shared_models(nested_content, model_cache).shared_model.content

    nested_nested_content = MyNestedNestedModel(
        nested_ref_model=MyNestedReferenceModel(content=nested_content.id))

    assert nested_nested_content is not populate_shared_models(nested_nested_content, model_cache)
    nested_content_populated = populate_shared_models(nested_nested_content, model_cache).nested_ref_model.content

    assert nested_content is not nested_content_populated
    assert isinstance(nested_content_populated, MyNestedModel)
    shared_model_populated = nested_content_populated.shared_model.content

    assert isinstance(shared_model_populated, MyContent)
    assert content is shared_model_populated


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
    } == collect_shared_model_field_type_and_ids(container)


def test_get_shared_content_types():
    assert (MyContent, MyAnotherContent) == get_shared_content_types(ComplexContainer)

def test_set_loader():
    content1 = MyContent(name='name1')

    shared_model = MySharedModel.parse_obj({
                'version': '0',
                'code': 'code1',
                'content': content1.id
    })

    shared_model.set_loader(lambda x: content1)

    assert content1 == shared_model.get_content()
