from typing import (
    Tuple, TypeVar, Iterable, Iterator, List, Collection,
    Any
)
from pydantic import BaseModel

import hashlib
import orjson

T = TypeVar('T')

def convert_tuple(items:Tuple[T]|T) -> Tuple[T]:
    return items if isinstance(items, tuple) else (items,)


def convert_list(items:List[T]|T) -> List[T]:
    return items if isinstance(items, list) else [items]


def convert_as_collection(items:List[T]|Tuple[T]|T) -> Collection[T]:
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


def load_json(item:str | bytes) -> Any:
    return orjson.loads(item)