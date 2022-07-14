from configparser import InterpolationSyntaxError
from typing import (
    Tuple, TypeVar, Iterable, Iterator, List, Collection
)
from pydantic import BaseModel

import hashlib

T = TypeVar('T')

def convert_tuple(items:Tuple[T]|T) -> Tuple[T]:
    return items if isinstance(items, tuple) else (items,)


def convert_collection(items:List[T]|Tuple[T]|T) -> Collection[T]:
    if isinstance(items, (list, tuple)):
        return items
    else:
        return (items,)


def unique(items:Iterable[T]) -> Iterator[T]:
    seen = set()

    for item in items:
        if item not in seen:
            seen.add(item)
            yield item


def digest(item:str|BaseModel, algorithm:str = 'sha1') -> str:
    if isinstance(item, BaseModel):
        return digest_str(item.json())
    else:
        return digest_str(item)


def digest_str(item:str, algorithm:str = 'sha1') -> str:
    h = hashlib.new(algorithm)

    h.update(item.encode('utf-8'))

    return h.hexdigest()
