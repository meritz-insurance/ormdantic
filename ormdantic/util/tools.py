from typing import (
    Tuple, TypeVar, Iterable, Iterator
)

T = TypeVar('T')

def convert_tuple(items:Tuple[T]|T) -> Tuple[T]:
    return items if isinstance(items, tuple) else (items,)

def unique(items:Iterable[T]) -> Iterator[T]:
    seen = set()

    for item in items:
        if item not in seen:
            seen.add(item)
            yield item