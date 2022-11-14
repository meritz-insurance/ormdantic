from types import UnionType
from typing import Type, Any, Dict, get_origin, Union, get_args, TypeVar
import copy 
import inspect

from pydantic import parse_obj_as
from pydantic.fields import Field 
from .base import (
    PersistentModel, register_class_postprocessor, SchemaBaseModel, IdentifiedMixin
)
from ormdantic.util.hints import get_args_of_list_or_tuple, is_derived_from
from ..util import get_base_generic_alias_of, get_logger

_TYPE_NAME_FIELD = 'type_name'

_logger = get_logger(__name__)

_all_type_named_models : Dict[str, Type] = {}


class TypeNamedModel(PersistentModel):
    # this field will be update by Metaclass. so, 
    type_name: str = Field(default='TypeNamedModel')


class IdentifiedModel(PersistentModel, IdentifiedMixin):
    ''' identified by uuid '''
    version:str = Field(default='0.1.0')

    class Config:
        title = 'base object which can be saved or retreived by id'


IdentifiedModelT = TypeVar('IdentifiedModelT', bound=IdentifiedModel)

class TypedIdentifiedModel(IdentifiedModel, TypeNamedModel):
    class Config:
        title = 'base object which can be saved or retreived by id and type'



def _fill_type_name_field(class_type:Type):
    name = class_type.__name__
        

    model_field = copy.copy(class_type.__fields__[_TYPE_NAME_FIELD])
    model_field.default = name

    class_type.__fields__[_TYPE_NAME_FIELD] = model_field

    if name in _all_type_named_models:
        previous = _all_type_named_models[name]
        if (previous is not class_type
            and inspect.getsourcefile(previous) != inspect.getsourcefile(class_type)
            and inspect.getsourcelines(previous) != inspect.getsourcelines(class_type)
        ):
            _logger.fatal(f'duplicated type name class {name=}. '
                f'previous {inspect.getsourcefile(previous)=}, {inspect.getsourcelines(previous)=}'
            )
            raise RuntimeError(f'duplicated name {name}.')
    else:
        _all_type_named_models[name] = class_type

 
register_class_postprocessor(TypeNamedModel, _fill_type_name_field)

def parse_object_for_model(obj:Dict[str, Any], model_type:Type|None = None) -> Any:
    model_type = model_type or get_type_named_model_type(obj[_TYPE_NAME_FIELD])

    return _parse_obj(obj, model_type)


def get_type_named_model_type(type_name:str) -> Type:
    return _all_type_named_models[type_name]


def _parse_obj(obj:Any, target_type:Type) -> Any:
    if get_base_generic_alias_of(target_type, Union, UnionType):
        generic_args = get_args(target_type)

        for arg in generic_args:
            try:
                if is_derived_from(arg, SchemaBaseModel) and isinstance(obj, dict):
                    return _parse_obj(obj, arg)
                if is_derived_from(arg, (list, tuple)) and isinstance(obj, (list, tuple)):
                    return _parse_obj(obj, arg)
            except RuntimeError as e:
                continue
        else:
            return parse_obj_as(target_type, obj)

    elif get_base_generic_alias_of(target_type, list, tuple):
        collection = list if is_derived_from(target_type, list) else tuple
        params = get_args_of_list_or_tuple(target_type)       

        # type unknown. List, Tuple
        if not params:
            return parse_obj_as(target_type, obj)

        if isinstance(params, tuple):
            if len(obj) != len(params):
                _logger.fatal(f'{target_type=} requires {params=} but {obj=}')
                raise RuntimeError('mismatched size of array item.')

            return tuple(
                _parse_obj(item, item_type) for item, item_type 
                in zip(obj, params)
            )
        else:
            return collection(_parse_obj(item, params) for item in obj)
    else:
        # target_type can be generic alias because we will lookup outer_type_
        # for looking __fields__ we should check orginal type not generic alias.
        type_ = get_origin(target_type) or target_type


        if is_derived_from(target_type, SchemaBaseModel):
            obj = dict(obj)

            if _TYPE_NAME_FIELD in obj:
                type_ = get_type_named_model_type(obj[_TYPE_NAME_FIELD])

            for field_name, model_field in type_.__fields__.items():
                if is_derived_from(model_field.type_, (SchemaBaseModel, Union, UnionType)):
                    type = model_field.outer_type_

                    obj[field_name] = _parse_obj(obj[field_name], type)
                elif is_derived_from(model_field.outer_type_, (list, tuple)):
                    obj[field_name] = _parse_obj(obj[field_name], model_field.outer_type_)

        return parse_obj_as(target_type, obj)

