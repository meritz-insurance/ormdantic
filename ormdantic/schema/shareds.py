import inspect
from typing import (
    ClassVar, Dict, Generic, Iterator, Iterator, TypeVar, get_args, Union, Type, 
    Set, DefaultDict, Any, cast, Tuple, List, Set, Callable, Annotated
)
import orjson
from pydantic import Field, PrivateAttr
from collections import defaultdict
from ormdantic.schema.modelcache import ModelCache

from ormdantic.util.hints import (
    get_args_of_base_generic_alias, get_union_type_arguments
)

from ..util import (
    digest, convert_as_list_or_tuple, get_logger, get_base_generic_alias_of, 
    is_derived_from, unique,
)

from .base import (
    IdentifiedMixin, UuidStr, PersistentModel,  PersistentModelT, 
    register_class_preprocessor, orjson_dumps, SchemaBaseModel,
    MetaIdentifyingField
)
from .paths import (extract_as, get_path_and_types_for, get_paths_for)


_logger = get_logger(__name__)

class SharedContentMixin(IdentifiedMixin):
    _id_attr : ClassVar[str] = ''

    def __init__(self, *args, **kwds):
        super().__init__(*args, **kwds)

        if not self.id:
            self.refresh_id()

    def refresh_id(self):
        self.normalize()

        if self._id_attr:
            id = UuidStr(getattr(self, self._id_attr))
        else:
            id = UuidStr(_get_content_id(self.dict()))

        self.id = id
        return id

    def normalize(self):
        pass


def _get_content_id(content:Dict[str, Any]) -> str:
    content.pop('id', None)

    return digest(orjson_dumps(content), 'sha1')


SharedContentModelT = TypeVar('SharedContentModelT', bound=SharedContentMixin)

# SharedModel could not be referenced in _SharedMixinMetaclass.__new__ 
# we make mixin which can be referenced in the function. 
class _ReferenceMarker(Generic[SharedContentModelT]):
    pass

def _update_annotation_for_shared_content(name:str, 
                                          bases: Tuple[Type, ...], 
                                          namespace: Dict[str, Any]) -> None:
    orgs = namespace.get('__orig_bases__', tuple())

    if _is_inherit_from_content_reference_model(bases):
        if not orgs:
            _logger.fatal(
                f'{bases=} is derived from "ContentReferenceModel". {orgs=}'
                f'but {name=} class should have parameter class for content'
            )
            raise TypeError(
                'ContentReferenceModel requires a parameter class for '
                f'holding the content. check the "{name}" class is derived '
                'from the ContentReferenceModel[T]')

    for org in orgs:
        shared_mixin = get_base_generic_alias_of(org, _ReferenceMarker)

        if shared_mixin:
            args = get_args(shared_mixin)

            if args:
                # it is possible that __annntations__ does not exist.
                # __annotations__ will keep the field which is existed only 
                # current class.
                # but we should forced set the __annotations__ for
                # override.
                annotations = namespace.get('__annotations__', {})
                content_type = cast(Any, args[0])
                annotations['content'] = Union[content_type, str]

                namespace['__annotations__'] = annotations


register_class_preprocessor(_ReferenceMarker, _update_annotation_for_shared_content)


def _is_inherit_from_content_reference_model(bases:Tuple[Type,...]) -> bool:
    for base in bases:
        # we could not use ContentReferenceModel here becuase it does not setted.
        # so check the name and module.
        if base.__module__ == __name__ and base.__qualname__ == 'ContentReferenceModel':
            return True

    return False


LazyLoader = Callable[[str], 'SharedContentMixin']  

class ContentReferenceModel(SchemaBaseModel, 
                            _ReferenceMarker[SharedContentModelT]):
    ''' identified by content '''
    content : SharedContentModelT | str = Field(default='', title='content')
    _lazy_loader : LazyLoader | None = PrivateAttr(default=None)

    def get_content_type(self) -> Type:
        arguments = get_union_type_arguments(self.__fields__['content'].outer_type_)
        assert arguments

        content_type = arguments[0]

        if not inspect.isclass(content_type):
            _logger.fatal(f'{content_type=} is not general class. if it is TypeVar, '
                'you should declare class which derived from ContentReferenceModel. '
                'do not use ContentReferenceModel directly. we could not know what '
                'is the real class for content')
            raise RuntimeError('do not use ContentReferenceModel directly.')

        return content_type

    def get_content_id(self) -> str:
        return self.content if isinstance(self.content, str) else self.content.id

    def get_content(self) -> SharedContentModelT:
        if isinstance(self.content, str):
            if self._lazy_loader is None:
                _logger.fatal('cannot lazy loading shared content _loader is None')
                raise RuntimeError('cannot get the content. set lazy loader')
            
            self.content = cast(SharedContentModelT, self._lazy_loader(self.content))

        return self.content

    def set_loader(self, loader:LazyLoader | None):
        self._lazy_loader = loader

    class Config:
        title = 'base object which can be saved or retreived by content'


class SharedContentModel(SharedContentMixin):
    pass


class PersistentSharedContentModel(SharedContentMixin, PersistentModel):
    pass


def get_shared_content_types(model_type:Type) -> Tuple[Type]:
    return tuple(
        unique(
            get_args_of_base_generic_alias(field_type, ContentReferenceModel)[0]
            for _, field_type in get_path_and_types_for(model_type, ContentReferenceModel) 
        )
    )


def extract_shared_models(model: PersistentModel,
                          replace_with_id: bool = False
                          ) -> DefaultDict[str, Dict[Type, SharedContentMixin]]:

    shared_models : DefaultDict[str, Dict[Type, SharedContentMixin]] = defaultdict(dict)

    for shared_model, field_type in _iterate_content_reference_models_and_type(model):
        content = shared_model.content

        if isinstance(content, SharedContentMixin):
            content.refresh_id()

            shared_models[content.id][field_type] = content

            if replace_with_id:
                shared_model.content = content.id

    return shared_models


def extract_shared_models_for(model: PersistentModel, 
                              target_type: Type[SharedContentModelT],
                              replace_with_id: bool = False
                              ) -> Dict[str, SharedContentModelT]:
    shared_models : Dict[str, SharedContentModelT] = dict()

    for shared_model in _iterate_content_reference_models(model):
        content = cast(SharedContentMixin, shared_model.content)

        if is_derived_from(type(content), target_type):
            content.refresh_id()

            if content.id in shared_models:
                if type(shared_models[content.id]) is not type(content):
                    _logger.fatal(f'{content=} has different type but has same {content.id=}')
                    raise RuntimeError('cannot support the contents of different type which have same id.')

            shared_models[content.id] = cast(SharedContentModelT, content)

            if replace_with_id:
                shared_model.content = content.id

    return shared_models


def has_shared_models(model:PersistentModel) -> bool:
    return any(get_paths_for(type(model), ContentReferenceModel))


def populate_shared_models(model:PersistentModelT, 
                           *model_sets: ModelCache) -> PersistentModelT:
    # contents should be dictionary of dictionary. 
    # contents {'1a2221adef12':{field_type:model_object}}
    # we should iterate all item which has same id for checking derived type.

    if _has_to_be_populated(model):
        model = model.copy(deep=True)

    for reference_model in _iterate_content_reference_models(model):
        content = reference_model.content

        if isinstance(content, str):
            for model_set in model_sets:
                matched_models = model_set.find(reference_model.get_content_type(), content)

                if matched_models:
                    # if shared model has a nested shared model we will populate it.
                    matched_models = populate_shared_models(matched_models, model_set)
                    reference_model.content = matched_models
                    break
            else:
                _logger.fatal(f'cannot load shared model from cache {content=}, {model_sets=}')
                raise RuntimeError('cannot populate shared model.')

    return model


def iterate_isolated_models(model:PersistentModel
                                    ) -> Iterator[PersistentModel]:
    has_shared = has_shared_models(model)

    if has_shared:
        # model will be changed after extract_shared_models_for,
        # so deepcopy should be called here.
        model = model.copy(deep=True)

        for content_model in extract_shared_models_for(
                model, PersistentSharedContentModel, True).values():
            yield from iterate_isolated_models(content_model)

    yield model


def _has_to_be_populated(model:PersistentModel):
    return any(
        isinstance(reference_model.content, str) 
        for reference_model 
        in _iterate_content_reference_models(model)
    )


def collect_shared_model_field_type_and_ids(
        model: PersistentModel) -> DefaultDict[Type[PersistentModel], Set[str]]:
    type_and_ids : DefaultDict[Type[PersistentModel], Set[str]]= defaultdict(set)

    # ignore that model is ContentReferenceModel 

    for path, field_type in get_path_and_types_for(type(model), ContentReferenceModel):
        ref_type = get_args_of_base_generic_alias(field_type, ContentReferenceModel)[0]

        for sharedModel in convert_as_list_or_tuple(
                extract_as(model, path, ContentReferenceModel)
                or cast(List[ContentReferenceModel], [])):
            type_and_ids[ref_type].add(sharedModel.get_content_id())

    return type_and_ids
 

def _iterate_content_reference_models(
        model: PersistentModel) -> Iterator[ContentReferenceModel]:
    model_type = type(model)

    # if is_derived_from(model_type, ContentReferenceModel):
    #     yield cast(ContentReferenceModel, model)

    for path in get_paths_for(model_type, ContentReferenceModel) :
        for shared_model in convert_as_list_or_tuple(
                extract_as(model, path, ContentReferenceModel)
                or cast(List[ContentReferenceModel], [])):
            yield shared_model


def _iterate_content_reference_models_and_type(
        model: PersistentModel) -> Iterator[Tuple[ContentReferenceModel, Type]]:
    model_type = type(model)

    # if is_derived_from(model_type, ContentReferenceModel):
    #     yield cast(ContentReferenceModel, model), model_type

    for path, type_ in get_path_and_types_for(model_type, ContentReferenceModel) :
        for shared_model in convert_as_list_or_tuple(
                extract_as(model, path, ContentReferenceModel)
                or cast(List[ContentReferenceModel], [])):
            yield shared_model, type_
