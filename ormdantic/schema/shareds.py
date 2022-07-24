from email.headerregistry import ContentDispositionHeader
from typing import (
    Dict, Generic, Iterator, Iterator, TypeVar, get_args, Union, Type, Set, DefaultDict,
    Any, cast, Tuple
)
import orjson
from pydantic import Field
from pydantic.fields import FieldInfo
from pydantic.main import ModelMetaclass, __dataclass_transform__
from collections import defaultdict

from ormdantic.util.hints import get_union_type_arguments

from ..util import digest, convert_as_collection, get_logger, get_base_generic_type_of, is_derived_from

from .base import (IdentifiedMixin, IdStr, PersistentModel,  SchemaBaseModel, get_field_type)
from .paths import (extract_as, get_path_and_type, get_paths_for_type)


_logger = get_logger(__name__)

class SharedContentMixin(IdentifiedMixin):
    def __init__(self, *args, **kwds):
        super().__init__(*args, **kwds)

        if not self.id:
            self.refresh_id()

    def refresh_id(self):
        self.normalize()

        self.id = IdStr(_get_content_id(self.dict()))
        return self.id

    def normalize(self):
        pass


SharedContentModelT = TypeVar('SharedContentModelT', bound=SharedContentMixin)

# SharedModel could not be referenced in _SharedMixinMetaclass.__new__ 
# we make mixin which can be referenced in the function. 
class _ReferenceMarker(Generic[SharedContentModelT]):
    pass


@__dataclass_transform__(kw_only_default=True, field_descriptors=(Field, FieldInfo))
class ContentReferencedMetaclass(ModelMetaclass):
    def __new__(cls, name, bases, namespace, **kwargs):
        orgs = namespace.get('__orig_bases__', tuple())

        if _is_inherit_from_content_reference_model(bases):
            if not orgs:
                _logger.fatal(
                    f'{bases=} is derived from "ContentReferenceModel". {orgs=}'
                    f'but {name=} class should have parameter class for content'
                )
                raise TypeError(
                    'ContentReferenceModel requires a parameter class for '
                    f'holding the content. check the {name} class is derived '
                    'from the ContentReferenceModel[T]')

        for org in orgs:
            shared_mixin = get_base_generic_type_of(org, _ReferenceMarker)

            if shared_mixin:
                args = get_args(shared_mixin)

                if args:
                    annotations = namespace.get('__annotations__', {})
                    content_type = cast(Any, args[0])
                    annotations['content'] = Union[content_type, str]

        return super().__new__(cls, name, bases, namespace, **kwargs)


def _is_inherit_from_content_reference_model(bases:Tuple[Type,...]) -> bool:
    for base in bases:
        if base.__module__ == __name__ and base.__qualname__ == 'ContentReferenceModel':
            return True

    return False


class ContentReferenceModel(SchemaBaseModel, 
                            _ReferenceMarker[SharedContentModelT], 
                            metaclass=ContentReferencedMetaclass):
    ''' identified by content '''
    content : SharedContentMixin | str = Field(default='', title='content')

    def get_content_id(self)->str:
        return self.content if isinstance(self.content, str) else self.content.id

    def get_content(self) -> SharedContentMixin:
        if isinstance(self.content, str):
            raise RuntimeError('cannot get the content. call concat_shared_model before call this function.')

        return self.content

    class Config:
        title = 'base object which can be saved or retreived by content'


class PersistentSharedContentModel(SharedContentMixin, PersistentModel):
    pass


def _get_content_id(content:Dict[str, Any]) -> str:
    content.pop('id', None)

    return digest(orjson.dumps(content).decode(), 'sha1')


def extract_shared_models(model: SchemaBaseModel,
                          replace_with_id: bool = False
                          ) -> DefaultDict[str, DefaultDict[Type, SharedContentMixin]]:
    contents : DefaultDict[str, DefaultDict[Type, SharedContentMixin]] = defaultdict(defaultdict)

    for shared_model in _iterate_shared_models(model):
        content = shared_model.content

        if isinstance(content, SharedContentMixin):
            content.refresh_id()

            content_type = type(content)
            contents[content.id][content_type] = content

            if replace_with_id:
                shared_model.content = content.id

    return contents


def extract_shared_models_for(model: SchemaBaseModel, 
                              target_type: Type[SharedContentModelT],
                              replace_with_id: bool = False
                              ) -> Dict[str, SharedContentModelT]:
    contents : Dict[str, SharedContentModelT] = dict()

    for shared_model in _iterate_shared_models(model):
        content = cast(SharedContentMixin, shared_model.content)

        if is_derived_from(type(content), target_type):
            content.refresh_id()

            if content.id in contents:
                if content != contents[content.id]:
                    raise RuntimeError('cannot support same id of multiple contents of different type.')

            contents[content.id] = cast(SharedContentModelT, content)

            if replace_with_id:
                shared_model.content = content.id

    return contents


def has_shared_models(model:SchemaBaseModel) -> bool:
    return any(get_paths_for_type(type(model), ContentReferenceModel))


def concat_shared_models(model:SchemaBaseModel, 
                         contents: Dict[str, Dict[Type, SharedContentMixin]] | Dict[str, SharedContentMixin]):
    for shared_model in _iterate_shared_models(model):
        target_type = get_field_type(type(shared_model), 'content')
        content = shared_model.content

        if isinstance(content, str):
            id_models = contents[content]

            if isinstance(id_models, dict):
                for target_type in get_union_type_arguments(target_type) or tuple():
                    for base_type in target_type.mro():
                        if base_type in id_models:
                            shared_model.content = id_models[base_type]
                            break
            else:
                shared_model.content = id_models


def collect_shared_model_type_and_ids(model:SchemaBaseModel) -> DefaultDict[Type[SchemaBaseModel], Set[str]]:
    type_and_ids : DefaultDict[Type[SchemaBaseModel], Set[str]]= defaultdict(set)

    for path, field_type in get_path_and_type(type(model), ContentReferenceModel) :
        shared_type = get_base_generic_type_of(field_type, ContentReferenceModel)
        ref_type = get_args(shared_type)[0]

        for sharedModel in convert_as_collection(extract_as(model, path, ContentReferenceModel) or []):
            type_and_ids[ref_type].add(sharedModel.get_content_id())

    return type_and_ids
 

def _iterate_shared_models(model:SchemaBaseModel) -> Iterator[ContentReferenceModel]:
    model_type = type(model)

    if is_derived_from(model_type, ContentReferenceModel):
        yield cast(ContentReferenceModel, model)

    for path in get_paths_for_type(model_type, ContentReferenceModel) :
        for shared_model in convert_as_collection(extract_as(model, path, ContentReferenceModel) or []):
            yield shared_model
