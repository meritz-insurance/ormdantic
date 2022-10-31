import itertools
from typing import (
    Any, Tuple, Type, TypeVar, Iterator, Callable, 
    List, cast
)
import inspect

from ormdantic.util.hints import get_args_of_list_or_tuple
from ormdantic.util.tools import convert_tuple

from .base import ( ModelT, PersistentModel)
from ..util import (
    get_logger, is_derived_from, is_list_or_tuple_of, convert_as_collection,
    is_derived_or_collection_of_derived
)


T = TypeVar('T')

_logger = get_logger(__name__)


def get_paths_for(type_:Type, types:Type | Tuple[Type, ...]) -> Tuple[str, ...]:
    types = convert_tuple(types)

    return tuple(path for path, _ in get_path_and_types_for(type_, 
        lambda t: any(
            is_derived_or_collection_of_derived(t, target_type) for target_type in types
        )
    ))


def get_path_and_types_for(type_:Type[ModelT], 
                      predicate: Type | Callable[[Type], bool] | None = None,
                      ) -> Iterator[Tuple[str, Type]]:
    
    if not is_derived_from(type_, PersistentModel):
        _logger.fatal(f'{type_=} should be subclass of SchemaBaseModel. '
                      f'check the mro {inspect.getmro(type_)=}')
        raise RuntimeError(f'invalid type {type_} for get path and type')

    yield from (('$.' + ('.'.join(paths)), type_) for paths, type_ in _get_path_and_type(type_, predicate))
    

def _get_path_and_type(type_:Type[ModelT], 
                      predicate: Type | Callable[[Type], bool] | None = None,
                      ) -> Iterator[Tuple[List[str], Type]]:
    # TODO:
    # checking recursively refere4nce should be implemented
    assert is_derived_from(type_, PersistentModel)

    for field_name, model_field in type_.__fields__.items():
        field_type = model_field.outer_type_ 

        if not predicate or (
            predicate(field_type)
            if inspect.isfunction(predicate) else 
            is_derived_or_collection_of_derived(field_type, predicate)
        ):
            json_path = []

            json_path.append(field_name)

            if is_list_or_tuple_of(field_type):
                generic_param = get_args_of_list_or_tuple(field_type)

                # path에서 tuple[int, str] 같은 형태는 지원하지 않는다.
                assert not isinstance(generic_param, tuple), "not support heterogeneous type for collection"

                field_type = generic_param

            yield (json_path, field_type)
        elif is_derived_from(field_type, PersistentModel):
            yield from (([field_name] + paths, type_) 
                for paths, type_ in 
                _get_path_and_type(field_type, predicate)
            )
        elif is_list_or_tuple_of(field_type, PersistentModel):
            generic_param = get_args_of_list_or_tuple(field_type)

            assert not isinstance(generic_param, tuple), "not support heterogeneous type for collection"

            if generic_param: 
                yield from (([field_name] + paths, type_) 
                    for paths, type_ in 
                    _get_path_and_type(generic_param, predicate)
                )


def extract(model:PersistentModel, path:str) -> Any:
    current = model

    for field in path.split('.'):
        if field == '$':
            current = model
        else:
            if field.endswith('[*]'):
                field_name = field[:-3]
            else:
                field_name = field

            if is_list_or_tuple_of(type(current)):
                current = tuple(itertools.chain(*(
                    convert_as_collection(getattr(item, field_name, tuple()))
                    for item in cast(list, current)
                )))
            elif current is not None:
                current = getattr(current, field_name, None)
            else:
                return None

    return current
            

def extract_as(model:PersistentModel, path:str, target_type_:Type[T]) -> T | Tuple[T] | None:
    data = extract(model, path)

    if data is None:
        return None

    if isinstance(data, target_type_) or is_list_or_tuple_of(type(data)):
        return data

    _logger.fatal(f'{data=} cannot be converted type {target_type_=}')
    raise RuntimeError('invalid type for casting in extract_as')

