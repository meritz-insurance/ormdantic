from typing import (
    Dict, Generic, Iterator, Iterator, get_args, Union, Type, Set, DefaultDict, overload
)
from pydantic import Field, root_validator
from pydantic.main import ModelMetaclass
from collections import defaultdict

from ..util import digest, convert_as_collection, get_logger, get_base_generic_type_of, is_derived_from

from .base import ( IdentifiedMixin, ModelT, IdStr, SchemaBaseModel, get_field_type)
from .paths import ( extract_as, get_path_and_type, get_paths_for_type)

_logger = get_logger(__name__)


# SharedModel could not be referenced in _SharedMixinMetaclass.__new__ 
# we make mixin which can be referenced in the function. 
class SharedMixin(Generic[ModelT]):
    pass


class _SharedMixinMetaclass(ModelMetaclass):
    def __new__(cls, name, bases, namespace, **kwargs):
        orgs = namespace.get('__orig_bases__', tuple())

        for org in orgs:
            shared_mixin = get_base_generic_type_of(org, SharedMixin)
            if  shared_mixin:
                args = get_args(shared_mixin)

                if args:
                    annotations = namespace.get('__annotations__', {})
                    annotations['content'] = Union[args[0], None]

        return super().__new__(cls, name, bases, namespace, **kwargs)


class SharedModel(IdentifiedMixin, SharedMixin[ModelT], metaclass=_SharedMixinMetaclass):
    ''' identified by content '''
    content : ModelT | None = Field(None, title='content')

    def __setattr__(self, name, value):
        if name == 'content':
            if value is not None and self.content != value:
                regenerated = digest(value.json(), 'sha1')
                self.__dict__['id'] = IdStr(regenerated)

        if name == 'id' and value != self.id and self.content != None:
            _logger.fatal(f'try to change id though content was set. {id=}')
            raise RuntimeError('cannot set id if content is not None.')

        return super().__setattr__(name, value)

    @root_validator
    def _setup_id(cls, values):
        if values.get('content', None) is not None:
            regenerated = digest(values['content'].json(), 'sha1')
            values['id'] = regenerated

        return values

    class Config:
        title = 'base object which can be saved or retreived by content'


def extract_shared_models(model: SchemaBaseModel,
                          replace_none: bool = False) -> DefaultDict[str, DefaultDict[Type, SchemaBaseModel]]:
    contents : DefaultDict[str, DefaultDict[Type, SharedModel]] = defaultdict(defaultdict)

    for shared_model in iterate_shared_models(model):
        if shared_model.content:
            content_type = type(shared_model.content)
            contents[shared_model.id][content_type] = shared_model.content

            if replace_none:
                shared_model.content = None

    return contents


def extract_shared_models_for(model: SchemaBaseModel, target_type: Type[ModelT],
                            replace_none: bool = False) -> Dict[str, ModelT]:
    contents : Dict[str, ModelT] = dict()

    for shared_model in iterate_shared_models(model):
        if shared_model.content:
            content = shared_model.content
            if is_derived_from(type(content), target_type):
                contents[shared_model.id] = content

                if replace_none:
                    shared_model.content = None

    return contents


def has_shared_models(model:SchemaBaseModel) -> bool:
    return any(get_paths_for_type(type(model), SharedModel))


def concat_shared_models(model:SchemaBaseModel, contents:Dict[str, Dict[Type, SchemaBaseModel]] | Dict[str, SchemaBaseModel]):
    for shared_model in iterate_shared_models(model):
        target_type = get_field_type(type(shared_model), 'content')

        if not shared_model.content:
            id_models = contents[shared_model.id]

            if isinstance(id_models, dict):
                for base_type in target_type.mro():
                    if base_type in id_models:
                        shared_model.content = id_models[base_type]
                        break
            else:
                shared_model.content = id_models


def collect_shared_model_type_and_ids(model:SchemaBaseModel) -> DefaultDict[Type[SchemaBaseModel], Set[str]]:
    type_and_ids : DefaultDict[Type[SchemaBaseModel], Set[str]]= defaultdict(set)

    for path, field_type in get_path_and_type(type(model), SharedModel) :
        shared_type = get_base_generic_type_of(field_type, SharedModel)
        ref_type = get_args(shared_type)[0]

        for sharedModel in convert_as_collection(extract_as(model, path, SharedModel) or []):
            type_and_ids[ref_type].add(sharedModel.id)

    return type_and_ids
 

def iterate_shared_models(model:SchemaBaseModel) -> Iterator[SharedModel]:
    for path in get_paths_for_type(type(model), SharedModel) :
        for shared_model in convert_as_collection(extract_as(model, path, SharedModel) or []):
            yield shared_model
     