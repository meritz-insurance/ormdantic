from typing import (DefaultDict, Dict, Type, Callable, Tuple, Any, cast)
from collections import defaultdict

from ormdantic.schema.base import PersistentModel, PersistentModelT, get_identifying_field_values

from ..util import get_logger


_logger = get_logger(__name__)

class ModelCache():
    def __init__(self, threshold:int = 100_000, 
                 entries: Dict[Tuple[Any, ...], Dict[Type, PersistentModel]] = {}):
        self._cached : DefaultDict[Tuple[Any,...], Dict[Type, PersistentModel]] = defaultdict(dict)
        self._threshold = threshold

        if entries:
            self._cached.update(entries.items())

    def register(self, type_:Type, model:PersistentModel) -> PersistentModel:
        id_fields = tuple(get_identifying_field_values(model).values())

        self._cached[id_fields][type_] = model

        item_count = len(self._cached)

        if item_count >= self._threshold and (item_count - 1) % 100 == 0:
            _logger.warning(f'cache size is over {self._threshold=} {item_count=}')

        return model

    def find(self, type_:Type[PersistentModelT], *id_values:Any) -> PersistentModelT | None:
        if id_values in self._cached:
            type_and_model = self._cached[id_values]

            if type_ in type_and_model:
                return cast(PersistentModelT, type_and_model[type_])

            for item in type_and_model.values():
                if isinstance(item, type_):
                    return item
        
        return None

    def load(self, type_:Type, *id_values:Any) -> PersistentModel:
        found = self.find(type_, *id_values)

        if found is None:
            raise RuntimeError('no such item in model cache')

        return found

    def clear(self):
        return self._cached.clear()

    def delete(self, type_:Type, *id_values:Any):
        targets = self._cached[id_values]

        if type_ in targets:
            targets.pop(type_)
        else:
            for key, value in targets.items():
                if isinstance(value, type_):
                    targets.pop(key)
                    break

    def cached_get(self, type_:Type[PersistentModel], 
                   func: Callable[..., PersistentModel | None], *id_values:Any) -> PersistentModel | None:
        fetch = self.find(type_, *id_values)

        if fetch:
            return fetch
        else:
            result = func(type_, *id_values)

            if result:
                return self.register(type_, result)

        return None

    def has_entry(self, type_:Type, *key:Any) -> bool:
        if key in self._cached:
            if type_ in self._cached[key]:
                return True
            
            type_and_model = self._cached[key]

            for model in type_and_model.values():
                if isinstance(model, type_):
                    return True

        return False

