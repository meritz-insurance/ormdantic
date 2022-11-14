from types import UnionType
from typing import (
    TypeGuard, get_args, Type, get_origin, Tuple, Any, Generic, Protocol,
    ForwardRef, Dict, Generic, Union, TypeVar, List, Tuple, overload,
    Annotated
)
from typing_extensions import _collect_type_vars # type: ignore
from inspect import getmro
import sys
import copy
import functools
import inspect


from .log import get_logger
from .tools import convert_tuple


_logger = get_logger(__name__)

#@functools.cache
def get_base_generic_alias_of(type_:Type, *generic_types:Type) -> Type | None:
    generic_types = convert_tuple(generic_types)

    if get_origin(type_) == Annotated:
        type_ = type_.__origin__

    for base_type in get_mro_with_generic(type_):
        if any(base_type is t or get_origin(base_type) is t for t in generic_types):
            return base_type

    return None

def get_args_of_base_generic_alias(type_:Type, *generic_types:Type) -> Tuple[Any,...]:
    generic = get_base_generic_alias_of(type_, *generic_types)

    if generic:
        return get_args(generic)

    raise RuntimeError('no generic. check type.')


_T = TypeVar('_T')


def get_type_args(type_:Type) -> Tuple[Any,...]:
    return get_args(type_)


# from https://github.com/python/typing/issues/777
def _generic_mro(result, tp):
    origin = get_origin(tp)
    origin = origin or tp

    result[origin] = tp
    if hasattr(origin, "__orig_bases__"):
        parameters = _collect_type_vars(origin.__orig_bases__)
        substitution = dict(zip(parameters, get_args(tp)))

        for base in origin.__orig_bases__:
            if get_origin(base) in result:
                continue
            base_parameters = getattr(base, "__parameters__", ())
            if base_parameters:
                base = base[tuple(substitution.get(p, p) for p in base_parameters)]
            _generic_mro(result, base)


@functools.cache
def get_mro_with_generic(tp:Type):
    origin = get_origin(tp)

    if origin is None and not hasattr(tp, "__orig_bases__"):
        if not isinstance(tp, type):
            raise TypeError(f"{tp!r} is not a type or a generic alias")
        return tp.__mro__
    # sentinel value to avoid to subscript Generic and Protocol

    result = {Generic: Generic, Protocol: Protocol}
    _generic_mro(result, tp)
    cls = origin if origin is not None else tp
    mro = getattr(cls, '__mro__', (cls,))
    return tuple(result.get(sub_cls, sub_cls) for sub_cls in mro)


def update_forward_refs_in_generic_base(type_:Type, localns:Dict[str, Any]):
    # convert ForwardRef as resolved class in __orig_bases__ in base classes
    # if class is derived from generic, it has __orig_bases__ attribute

    if hasattr(type_, '__orig_bases__'):
        type_.__orig_bases__ = tuple(
            resolve_forward_ref_in_args(base, localns) for base in type_.__orig_bases__)

                
def resolve_forward_ref_in_args(base:Type, localns:Dict[str, Any]) -> Type:
    if hasattr(base, '__args__') and any(type(arg) is ForwardRef for arg in base.__args__):
        # List[ForwardRef("Container")] will be same object 
        # though they are declared in different scope.
        # so, we copy current type and change ForwardRef as evaluated.
        new_type = copy.deepcopy(base)
        new_type.__args__ = tuple(resolve_forward_ref(arg, localns) for arg in base.__args__)

        return new_type

    return base


def resolve_forward_ref(type_:Type, localns:Dict[str, Any]) -> Type:
    if type_.__class__ is ForwardRef:
        globalns = (
            sys.modules[type_.__module__].__dict__.copy() 
            if type_.__module__ in sys.modules else {}
        )

        real_type = type_._evaluate(globalns, localns, set())
    
        return real_type

    return type_


@overload
def is_derived_from(type_:Type, base_type:Type[_T]) -> TypeGuard[Type[_T]]:
    ...

@overload
def is_derived_from(type_:Type, base_type:Tuple[Type,...]) -> bool:
    ...

def is_derived_from(type_:Type, base_type:Type[_T] | Tuple[Type,...]) -> TypeGuard[Type[_T]] | bool:
    # if first argument is not class, the issubclass throw the exception.
    # but usually, we don't need the exception. 
    # we just want to know whether the type is derived or not.

    # if type_ is generic, we will use __origin__
    #
    # https://stackoverflow.com/questions/49171189/whats-the-correct-way-to-check-if-an-object-is-a-typing-generic

    base_types = convert_tuple(base_type)

    if type_ in base_types:
        return True

    if hasattr(type_, "__origin__"):
        origin_type = get_origin(type_)

        if origin_type == Annotated:
            return is_derived_from(type_.__origin__, base_type)

        if origin_type in base_types:
            return True

    if hasattr(type_, '__mro__'):
        # Union does not have __mro__ attribute
        return any(t in base_types for t in getmro(type_))

    return False


def has_metadata(type_:Type, meta_type:Type) -> bool:

    if hasattr(type_, '__metadata__'):
        if __debug__:
            _validate_metadata(type_)
        return any(isinstance(o, meta_type) for o in type_.__metadata__)
    
    return False


def get_metadata_for(type_:Type, meta_type:Type[_T]) -> _T | None:
    if hasattr(type_, '__metadata__'):
        if __debug__:
            _validate_metadata(type_)

        for o in type_.__metadata__:
            if isinstance(o, meta_type):
                return o
    
    return None


def _validate_metadata(type_:Type):
    if any(inspect.isclass(o)  for o in type_.__metadata__):
        _logger.fatal(f'{type_.__metadata__=} contains class.')
        raise RuntimeError('metadata should be instance not class.')



def get_union_type_arguments(type_:Type, target_type:Type | None = None) -> Tuple[Type,...] | None:
    union_generic = get_base_generic_alias_of(type_, Union, UnionType)

    if union_generic:
        args = get_args(union_generic)

        if not target_type:
            return args

        return tuple(a for a in args if is_derived_from(a, target_type))

    return None


def is_list_or_tuple_of(type_:Type, *parameters:Type) -> bool:
    args = get_args_of_list_or_tuple(type_)

    if args is None:
        return False

    if not parameters:
        return True

    if isinstance(args, tuple):
        if len(args) == len(parameters):
            return all(is_derived_from(t, b) for t, b in zip(args, parameters))

        return False
    else:
        if len(parameters) != 1:
            return False

        return is_derived_from(args, parameters[0])


@functools.cache
def is_derived_or_collection_of_derived(type_:Type, param_type_:Type):
    return is_derived_from(type_, param_type_) or is_list_or_tuple_of(type_, param_type_) 


@functools.cache
def get_args_of_list_or_tuple(type_:Type) -> Type | Tuple[Type,...] | None:
    ''' return args from type.
        if list, return Type or empty tuple
        if tuple, return Tuple[Type].  if tuple with ellispsi, handle it as list

        if type_ is not list and tuple, return None
    '''
    generic = get_base_generic_alias_of(type_, list)

    if generic:
        args = get_args(generic) 
        if len(args) == 1:
            return args[0]

        return args

    generic = get_base_generic_alias_of(type_, tuple)

    if generic:
        args = get_args(generic) 

        # handle Tuple[str, ...] = List[str]
        if len(args) == 2 and args[-1] is Ellipsis:
            return args[0]

        return args

    return None

