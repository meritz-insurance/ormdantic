from typing import Type, Any, Dict, cast, get_origin
import itertools
import copy 

from pydantic import BaseModel, parse_obj_as
from pydantic.fields import Field, FieldInfo
from pydantic.main import ModelMetaclass, __dataclass_transform__
from .base import SchemaBaseModel, register_class_postprocessor
from ormdantic.util.hints import get_type_parameter_of_list_or_tuple, is_derived_from
from ..util import get_base_generic_alias_of, get_logger

_TYPE_NAME_FIELD = 'type_name'

_logger = get_logger(__name__)

_all_type_named_models : Dict[str, Type] = {}

# @__dataclass_transform__(kw_only_default=True, field_descriptors=(Field, FieldInfo))
# class TypeNamedModelMetaclass(ModelMetaclass):
#     def __new__(cls, name, bases, namespace, **kwds):
#         new_one = cast(BaseModel, super().__new__(cls, name, bases, namespace, **kwds))

#         return new_one

def _fill_type_name_field(class_type:Type):
    name = class_type.__name__
    model_field = copy.copy(class_type.__fields__[_TYPE_NAME_FIELD])
    model_field.default = name

    class_type.__fields__[_TYPE_NAME_FIELD] = model_field

    if name in _all_type_named_models:
        _logger.fatal(f'duplicated type name class {name=}. {_all_type_named_models=}')
        raise RuntimeError(f'duplicated name {name}')

    _all_type_named_models[name] = class_type

 
class TypeNamedModel(SchemaBaseModel):
    # this field will be update by Metaclass. so, 
    type_name: str = Field(default='TypeNamedModel')

register_class_postprocessor(TypeNamedModel, _fill_type_name_field)

def parse_obj_for_model(obj:Dict[str, Any], model_type:Type|None = None) -> Any:
    model_type = model_type or get_type_named_model_type(obj[_TYPE_NAME_FIELD])

    return _parse_obj(obj, model_type)


def get_type_named_model_type(type_name:str) -> Type:
    return _all_type_named_models[type_name]


def _parse_obj(obj:Any, target_type:Type) -> Any:
    if alias := get_base_generic_alias_of(target_type, list, tuple):
        params = get_type_parameter_of_list_or_tuple(target_type)       

        assert params

        if get_origin(alias) is list:
            return [_parse_obj(item, params[0]) for item in obj]
        else:
            return tuple(
                _parse_obj(item, item_type) for item, item_type 
                in itertools.zip_longest(obj, params, fillvalue=params[-1])
            )
    else:
        obj = dict(obj)

        if _TYPE_NAME_FIELD in obj:
            target_type = get_type_named_model_type(obj[_TYPE_NAME_FIELD])

        if issubclass(target_type, SchemaBaseModel):
            for field_name, model_field in target_type.__fields__.items():
                if is_derived_from(model_field.type_, SchemaBaseModel):
                    obj[field_name] = _parse_obj(obj[field_name], model_field.outer_type_)

        return parse_obj_as(target_type, obj)
