from typing import (
    Dict, Generic, Iterator, cast, Iterable, Iterator, get_origin, get_args, Union, Type
)
from pydantic import Field, root_validator
from pydantic.main import ModelMetaclass
import orjson

from ..util import is_list_or_tuple_of, digest, convert_collection, get_logger

from .base import (
    IdentifiedModel, ModelT, IdStr, SchemaBaseModel, get_field_name_and_type
)
from .paths import (
    extract_as,
    get_paths_for_type
)

_logger = get_logger(__name__)

class _SharedModelMetaclass(ModelMetaclass):
    def __new__(cls, name, bases, namespace, **kwargs):
        orgs = namespace.get('__orig_bases__', tuple())

        for org in orgs:
            if  org.__name__ == 'SharedModel' and org.__module__ == 'ormdantic.schema.shareds':
                args = get_args(org)

                if args:
                    annotations = namespace.get('__annotations__', {})
                    annotations['content'] = Union[args[0], None]

        return super().__new__(cls, name, bases, namespace, **kwargs)


class SharedModel(IdentifiedModel, Generic[ModelT], metaclass=_SharedModelMetaclass):
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


def extract_shared_models(model:SchemaBaseModel, set_none:bool = False) -> Dict[str, SchemaBaseModel]:
    contents : Dict[str, SchemaBaseModel] = {}

    for sharedModel in _iterate_shared_models(model):
        if sharedModel.content:
            contents[sharedModel.id] = sharedModel.content

            if set_none:
                sharedModel.content = None

    return contents


def concat_shared_models(model:SchemaBaseModel, contents:Dict[str, SchemaBaseModel]):
    for sharedModel in _iterate_shared_models(model):
        if not sharedModel.content:
            sharedModel.content = contents[sharedModel.id]


def collect_shared_model_ids(model:SchemaBaseModel) -> Iterator[str]:
    for sharedModel in _iterate_shared_models(model):
        yield sharedModel.id


def _iterate_shared_models(model:SchemaBaseModel) -> Iterator[SharedModel]:
    for path in get_paths_for_type(type(model), SharedModel) :
        for sharedModel in convert_collection(extract_as(model, path, SharedModel) or []):
            yield sharedModel
     