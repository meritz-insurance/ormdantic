from pydantic import BaseModel

from ormdantic.util import (
    convert_tuple, convert_as_collection, unique, digest
)

def test_convert_tuple():
    assert convert_tuple(42) == (42,)
    assert convert_tuple((32, 42)) == (32, 42)


def test_convert_as_collection():
    assert convert_as_collection(42) == (42,)
    assert convert_as_collection((32, 42)) == (32, 42)
    assert convert_as_collection([32, 42]) == [32, 42]


def test_unique():
    assert list(unique([1,2,3,2,4,3])) == [1,2,3,4]
    assert list(unique((1,2))) == [1,2]


def test_digest():
    class MyModel(BaseModel):
        name:str

    assert digest('hello') == 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d'
    assert digest(MyModel(name='name')) == '7687e5ebae02f5340426c1a1a0607681c482354d'
    