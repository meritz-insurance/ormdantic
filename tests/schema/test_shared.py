from typing import List
import pytest

from ormdantic.schema.base import (
    SchemaBaseModel, IdStr
)

from ormdantic.schema.shareds import (
    SharedModel, collect_shared_model_ids, concat_shared_models, extract_shared_models
)
from ormdantic.util.tools import digest

class MyContent(SchemaBaseModel):
    name:str


class MySharedContent(SharedModel[MyContent]):
    code:str

class Container(SchemaBaseModel):
    items: List[MySharedContent]


def test_setattr():
    model = MySharedContent(id=IdStr('1'), version='0', code='1', content=None)
    assert '1' == model.id

    content = MyContent(name='name')
    model.content = content

    assert digest(content.json(), 'sha1') == model.id

    with pytest.raises(RuntimeError, match='cannot set id.*'):
        model.id = IdStr('1')

    model.content = None
    assert digest(content.json(), 'sha1') == model.id

    model.id = IdStr('1')
    assert '1' == model.id


def test_setup_id():
    model = MySharedContent.parse_obj(
        {
            'version': '0',
            'code': 'code',
            'content': {'name':'name'}
        }
    )

    assert model.id == digest('{"name":"name"}', 'sha1')


def test_extract_shared_models():
    content1 = MyContent(name='name1')
    content2 = MyContent(name='name2')

    container = Container(items=[
        MySharedContent.parse_obj({
            'version': '0',
            'code': 'code1',
            'content': {'name':'name1'}
        }),
        MySharedContent.parse_obj({
            'version': '0',
            'code': 'code2',
            'content': {'name':'name2'}
        })
    ])

    assert {
        digest(content1):content1,
        digest(content2):content2
    } == extract_shared_models(container)

    assert container.items[0].content == content1

    extract_shared_models(container, True)

    assert container.items[0].content is None
    assert container.items[1].content is None

    assert {} == extract_shared_models(content1)


def test_concat_shared_models():
    content1 = MyContent(name='name1')
    content2 = MyContent(name='name2')

    container = Container(items=[
        MySharedContent.parse_obj({
            'version': '0',
            'code': 'code1',
            'id': digest(content1)
        }),
        MySharedContent.parse_obj({
            'version': '0',
            'code': 'code2',
            'id': digest(content2)
        })
    ])

    assert {
    } == extract_shared_models(container)

    contents = {
        digest(content1):content1,
        digest(content2):content2
    } 

    concat_shared_models(container, contents)

    assert contents == extract_shared_models(container)

def test_collect_shared_model_ids():
    content1 = MyContent(name='name1')
    content2 = MyContent(name='name2')

    container = Container(items=[
        MySharedContent.parse_obj({
            'version': '0',
            'code': 'code1',
            'id': digest(content1)
        }),
        MySharedContent.parse_obj({
            'version': '0',
            'code': 'code2',
            'id': digest(content2)
        })
    ])

    assert (digest(content1), digest(content2)) == tuple(collect_shared_model_ids(container))

