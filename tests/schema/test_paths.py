from typing import Any, cast, List, Tuple, cast
import uuid

import pytest
from pydantic import ConstrainedStr

from ormdantic.schema.base import (
    SchemaBaseModel, StringIndex, 
    update_forward_refs, 
)

from ormdantic.schema.paths import (
    extract,
    extract_as,
    get_path_and_type,
    get_paths_for_type
)

class StartModel(SchemaBaseModel):
    name:StringIndex
    description:str

    parts: List['PartModel']
    part : 'PartModel'

class PartModel(SchemaBaseModel):
    name:StringIndex
    description:str

    sub_parts: List['SubPartModel']

class SubPartModel(SchemaBaseModel):
    name:StringIndex
    description:str

update_forward_refs(StartModel, locals())
update_forward_refs(PartModel, locals())

model = StartModel(name=StringIndex('start'), description='start model',
                    parts=[
                        PartModel(name=StringIndex('part1'), description='part1',
                                sub_parts=[
                            SubPartModel(
                                name=StringIndex('part1-sub1'),
                                description='part1-sub1'
                            ),
                            SubPartModel(
                                name=StringIndex('part1-sub2'),
                                description='part1-sub1'
                            )
                        ]),
                        PartModel(name=StringIndex('part2'), description='part1',
                                sub_parts=[
                            SubPartModel(
                                name=StringIndex('part2-sub1'),
                                description='part2-sub1'
                            ),
                            SubPartModel(
                                name=StringIndex('part2-sub2'),
                                description='part2-sub1'
                            )
                        ]),
                    ],
                    part=PartModel(name=StringIndex('part3'),
                        description='part3 model', sub_parts=[])
)


def test_extract():
    assert 'start' == extract(model, '$.name')
    assert ('part1', 'part2') == extract(model, '$.parts[*].name')
    assert ('part1', 'part2') == extract(model, '$.parts.name')
    assert ('part1', 'part2') == extract(extract(model, '$.parts'), '$.name')

    assert ('part1-sub1', 'part1-sub2', 'part2-sub1', 'part2-sub2') == extract(model, '$.parts.sub_parts.name')
    assert ('part1-sub1', 'part1-sub2', 'part2-sub1', 'part2-sub2') == extract(model, '$.parts[*].sub_parts[*].name')


def test_extract_as():
    assert 'start' == extract_as(model, '$.name', str)
    assert None is extract_as(model, '$.not_existed.not_existed', str)

    assert ('part1-sub1', 'part1-sub2', 'part2-sub1', 'part2-sub2') == extract_as(model, '$.parts.sub_parts.name', str)

    with pytest.raises(RuntimeError, match='invalid type for casting'):
        extract_as(model, '$.name', int)


def test_get_path_and_type():
    assert [
        ('$.name', StringIndex), 
        ('$.parts.name', StringIndex), 
        ('$.parts.sub_parts.name', StringIndex), 
        ('$.part.name', StringIndex), 
        ('$.part.sub_parts.name', StringIndex), 
    ] == list(get_path_and_type(StartModel, StringIndex))


def test_get_path_and_type_throw_exception():
    with pytest.raises(RuntimeError, match='invalid type .*'):
        list(get_path_and_type(cast(SchemaBaseModel, dict), StringIndex))


def test_get_paths_for_type():
    assert (
        '$.name',
        '$.parts.name',
        '$.parts.sub_parts.name',
        '$.part.name',
        '$.part.sub_parts.name',
    ) == get_paths_for_type(StartModel, StringIndex)