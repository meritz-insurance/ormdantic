from typing import (
    Tuple, TypeVar
)

T = TypeVar('T')

def convert_tuple(items:Tuple[T]|T) -> Tuple[T]:
    return items if isinstance(items, tuple) else (items,)
