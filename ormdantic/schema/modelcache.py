from typing import (Generic, DefaultDict, Dict, Type, Callable, TypeVar, Any)
from collections import defaultdict

from ..util import get_logger


_logger = get_logger(__name__)

_T = TypeVar('_T')

class ModelCache(Generic[_T]):
    def __init__(self, threshold:int = 100_000, entries:Dict[str, Dict[Type, _T]] = {}):
        self._cached : DefaultDict[str, Dict[Type, Any]] = defaultdict(dict)
        self._threshold = threshold

        if entries:
            self._cached.update(entries.items())

    def register(self, type_:Type, id:str, object:Any):
        self._cached[id][type_] = object

        item_count = len(self._cached)

        if item_count >= self._threshold and (item_count - 1) % 100 == 0:
            _logger.warning(f'cache size is over {self._threshold=} {item_count=}')

        return object

    def fetch(self, type_:Type, id:str) -> _T | None:
        if id in self._cached:
            type_and_model = self._cached[id]

            if type_ in type_and_model:
                return type_and_model[type_]

            for item in type_and_model.values():
                if isinstance(item, type_):
                    return item
        
        return None

    def clear(self):
        return self._cached.clear()

    def cached_get(self, type_:Type, id:str, 
                   func: Callable[[Type, str], _T | None]) -> _T | None:
        fetch = self.fetch(type_, id)

        if fetch:
            return fetch
        else:
            result = func(type_, id)

            if result:
                return self.register(type_, id,  result)

        return None

