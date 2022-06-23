from typing import (
    Any, ForwardRef, Tuple, Dict, Type, Generic, TypeVar, Iterator, Callable, Optional,
    List
)
import datetime
import inspect
from uuid import uuid4

import orjson

from pydantic import (
    BaseModel, ConstrainedDecimal, ConstrainedInt, Field, ConstrainedStr 
)
from ormdantic.util import (
    get_logger,
    get_base_generic_type_of, get_type_args, update_forward_refs_in_generic_base,
    is_derived_from, resolve_forward_ref, is_collection_type_of
)

JsonPathAndType = Tuple[Tuple[str,...], Type[Any]]
MaterializedFieldDefinitions = Dict[str, JsonPathAndType]
 
T = TypeVar('T')
ModelT = TypeVar('ModelT', bound='SchemaBaseModel')
PersistentModelT = TypeVar('PersistentModelT', bound='PersistentModel')


_logger = get_logger(__name__)

class MaterializedMixin():
    ''' the json value will be saved as table fields. '''
    pass


class IdentifyingMixin(MaterializedMixin):
    ''' the json value will be used identify the object. 
    The value of this type will be update through the sql param 
    so, this field of database will not use stored feature. '''
    
    def new_if_empty(self:T) -> T:
        raise NotImplementedError('fill if exist should be implemented.')


class IndexMixin(MaterializedMixin):
    ''' the json value will be indexed as table fields. '''
    pass


class UniqueIndexMixin(IndexMixin):
    ''' the json value will be indexed as unique. '''
    pass


class FullTextSearchedMixin(MaterializedMixin):
    ''' the json value will be used by full text searching.'''
    pass


class ArrayIndexMixin(IndexMixin, List[T]):
    ''' the json value will be indexed as table fields. '''
    pass 


class PartOfMixin(Generic[ModelT]):
    '''part of some json. ModelT will be container. '''
    pass

class StringIndex(ConstrainedStr, IndexMixin):
    ''' string index '''
    pass


class DecimalIndex(ConstrainedDecimal, IndexMixin):
    ''' decimal index'''
    pass


class IntIndex(ConstrainedInt, IndexMixin):
    ''' decimal index'''
    pass


class DateIndex(datetime.date, IndexMixin):
    ''' date index '''
    pass


class DateTimeIndex(datetime.datetime, IndexMixin):
    ''' date time index '''
    pass


class UniqueStringIndex(ConstrainedStr, UniqueIndexMixin):
    ''' Unique Index string '''
    pass


class UuidStr(ConstrainedStr, IdentifyingMixin):
    max_length = 36
    ''' UUID string'''
    pass

    def new_if_empty(self) -> 'UuidStr':
        if self == '':
            return UuidStr(uuid4().hex)

        return self



class FullTextSearchedStr(ConstrainedStr, FullTextSearchedMixin):
    ''' String for full text searching '''
    pass


class FullTextSearchedStringIndex(FullTextSearchedStr, IndexMixin):
    ''' string for full text searching and indexing'''
    pass


class ArrayStringIndex(ArrayIndexMixin[str]):
    ''' Array of string '''
    pass


def _orjson_dumps(v, *, default):
    # orjson.dumps returns bytes, to match standard json.dumps we need to decode
    return orjson.dumps(v, default=default).decode()


class SchemaBaseModel(BaseModel):
    class Config:
        title = 'model which can generate json schema.'

        json_dumps = _orjson_dumps
        json_loads = orjson.loads


class PersistentModel(SchemaBaseModel):
    _materialized_fields: MaterializedFieldDefinitions = {
    }

    class Config:
        title = 'model which can be saved in database'


class IdentifiedModel(PersistentModel):
    ''' identified by uuid '''
    version:str = Field('0.1.0')
    id: UuidStr = Field('', title='identifier for retreiving')

    class Config:
        title = 'unit object which can be saved or retreived by id'


IdentifiedModelT = TypeVar('IdentifiedModelT', bound=IdentifiedModel)

def get_container_type(type_:Type[ModelT]) -> Optional[Type[ModelT]]:
    ''' get the type of container '''
    part_type = get_base_generic_type_of(type_, PartOfMixin)

    if part_type:
        return get_type_args(part_type)[0]

    return None


def get_part_field_names(type_:Type[ModelT], part_types:Type | Tuple[Type, ...]
                        ) -> Tuple[str,...]:
    part_types = part_types if isinstance(part_types, tuple) else (part_types,)

    return tuple(field_name for field_name, t in get_field_name_and_type(type_, 
        lambda t: any(t is part_type for part_type in part_types)))

def get_field_name_and_type(type_:Type[ModelT], 
                            predicate: Callable[[Type], bool] | None = None
                            ) -> Iterator[Tuple[str, Type]]:
    if not issubclass(type_, BaseModel):
        _logger.fatal(f'type_ {type_=}should be subclass of BaseModel. '
                      f'check the mro {inspect.getmro(type_)}')
        raise RuntimeError(f'invalid type {type_}')

    for field_name, model_field in type_.__fields__.items():
        if not predicate or predicate(model_field.type_):
            yield (field_name, model_field.outer_type_)


def update_part_of_forward_refs(type_:Type[ModelT], localns:Dict[str, Any]):
    update_forward_refs_in_generic_base(type_, localns)

    type_.update_forward_refs(**localns)

    # resolve outer type also,
    for model_field in type_.__fields__.values(): 
        if isinstance(model_field.outer_type_, ForwardRef):
            model_field.outer_type_ = resolve_forward_ref(model_field.outer_type_, localns)


def is_field_collection_type(type_:Type[ModelT], field_name:str, parameters:Tuple[Type,...] | Type = tuple()) -> bool:
    model_field = type_.__fields__[field_name]

    return is_collection_type_of(model_field.outer_type_, parameters)


def get_part_types(type_:Type[ModelT]) -> Tuple[Type]:
    return tuple(
        model_field.type_
        for model_field in type_.__fields__.values()
        if is_derived_from(model_field.type_, PartOfMixin)
    )
        

def get_identifer_of(model:SchemaBaseModel) -> Iterator[Tuple[str, Any]]:
    for field_name, model_field in type(model).__fields__.items(): 
        if is_derived_from(model_field.outer_type_, IdentifyingMixin):
            yield (field_name, getattr(model, field_name))


def assign_identifying_fields_if_empty(model:ModelT, inplace:bool=False) -> ModelT:
    to_be_updated  = None

    for field_name in type(model).__fields__.keys(): 
        field_value = getattr(model, field_name)

        updated_value = (
            _replace_scalar_value_if_empty_value(field_value, inplace) 
            or _replace_vector_if_empty_value(field_value, inplace)
        )

        if updated_value is not None:
            if to_be_updated is None:
                to_be_updated = model if inplace else model.copy()

            setattr(to_be_updated, field_name, updated_value)

    return to_be_updated or model


def _replace_scalar_value_if_empty_value(obj:Any, inplace:bool) -> Any:
    if isinstance(obj, IdentifyingMixin):
        new_value = obj.new_if_empty()

        if new_value is not obj:
            return new_value
    elif isinstance(obj, PartOfMixin) and isinstance(obj, SchemaBaseModel):
        replaced = assign_identifying_fields_if_empty(obj, inplace)

        if replaced is not obj:
            return replaced

    return None


def _replace_vector_if_empty_value(obj:Any, inplace:bool) -> Any:    
    if isinstance(obj, (list, tuple)):
        if not obj:
            return None

        to_be_updated = None

        for index, item in enumerate(obj):
            replaced = _replace_scalar_value_if_empty_value(item, inplace)

            if not to_be_updated and replaced is not None:
                to_be_updated = list(obj[:index])

            if to_be_updated is not None:
                to_be_updated.append(replaced)

        if isinstance(obj, tuple) and to_be_updated:
            to_be_updated = tuple(to_be_updated)

        return to_be_updated

    return None

