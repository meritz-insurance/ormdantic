from typing import (
    Type, Iterator, List, Tuple, Any, Iterable
)
import orjson
import itertools
import pathlib as pl
import getpass as gp
import os

from ..util import get_logger, convert_tuple, is_derived_from
from ..schema.base import ( PartOfMixin, PersistentModel, get_identifying_fields )
from ..schema.verinfo import VersionInfo
from ..schema.typed import parse_object_for_model
from ..schema.source import QueryConditionType

from .connections import DatabaseConnectionPool
from .queries import _JSON_FIELD
from .storage import (
    query_records, get_current_version, upsert_objects, update_sequences
)


_logger = get_logger(__name__)

def export_to_file(pool:DatabaseConnectionPool,
                   types: List[Type], 
                   query_condition: QueryConditionType,
                   out_dir:pl.Path,
                   version:int | None = None,
                   set_id:int = 0):

    current_version = get_current_version(pool)

    if version is None:
        version = current_version
    
    _logger.debug(f'try to export objects to file {types=} {version=} {set_id=} to {out_dir=}')

    counter = 0

    for type_name, concatted, content in _export_objects(
        pool, types, query_condition, version, set_id):
        type_out_dir = out_dir / type_name

        type_out_dir.mkdir(exist_ok=True, parents=True)

        out_path = type_out_dir / (concatted + '.json')
        out_path.write_text(content, encoding='utf-8')
        counter += 1
        _logger.info(f'{counter=} {out_path=} was saved.')

    _logger.info(f'done. {counter} items saved.')


def import_from_file(pool:DatabaseConnectionPool, 
                     input_dirs: List[pl.Path],
                     why: str = '',
                     set_id: int = 0,
                     ignore_error: bool = False):

    _logger.debug(f'listing items in {input_dirs=}')

    json_paths = list(itertools.chain(
        *(input_dir.glob('**/*.json') for input_dir in input_dirs)
    ))

    why = why or f'imported from {input_dirs} for {set_id}. {len(json_paths)} json files'
    who = gp.getuser()
    where = os.uname().nodename

    _logger.debug(f'start to import {len(json_paths)=} from {input_dirs}')

    contents = [f.read_text(encoding='utf-8') for f in json_paths]

    import_objects(pool, contents, len(json_paths), set_id, ignore_error,
                   VersionInfo(who=who, why=why, where=where))

    _logger.info(f'done')


def _export_objects(pool:DatabaseConnectionPool, 
                    types: List[Type],
                    query_condition: QueryConditionType,
                    version: int,
                    set_id: int = 0) -> Iterator[Tuple[str, str, str]]:

    for type_ in types:
        if is_derived_from(type_, PartOfMixin):
            _logger.fatal(f'{type_=} is derived from PartOfMixin. but PartOFMixin cannot be exported. it should be saved when Container saved')
            raise RuntimeError('PartOfMixin is not supported.')

        id_fields = get_identifying_fields(type_)

        for record in query_records(pool, type_, query_condition, set_id, None,
                                    fields=(_JSON_FIELD, *id_fields),
                                    version=version):
            concatted = '.'.join(record[f] for f in id_fields)

            yield type_.__name__, concatted, record[_JSON_FIELD]


def import_objects(pool: DatabaseConnectionPool,
                   items: Iterable[str], total: int,
                   set_id: int = 0,
                   ignore_error: bool = False,
                   version_info: VersionInfo = VersionInfo()):
    count = 0

    def _progress(id_values:Tuple[Any,...], model:PersistentModel | BaseException):
        nonlocal count
        count += 1

        if isinstance(model, BaseException):
            _logger.info(f'{count=}/{total=} To save {id_values=} was failed. ignore it')
        else:
            _logger.info(f'{count=}/{total=} {id_values=} was saved.')

    try:
        results = upsert_objects(pool, 
                                (parse_object_for_model(orjson.loads(json))
                                for json in items),
                                set_id, ignore_error, version_info, _progress)
        _logger.info(f'models was saved.')
    except BaseException as e:
        _logger.fatal(f'models would not saved due to {e=}')
        raise

    if isinstance(results, dict):
        models = [model for model in results.values() if isinstance(model, PersistentModel)]
    else:
        models = [model for model in convert_tuple(results)]

    types = set(type(m) for m in models)

    update_sequences(pool, types)
    _logger.info(f'sequence was updated as max of SequenceStrId')

    if isinstance(results, dict):
        for id_values, model_or_exception in results.items():
            if isinstance(model_or_exception, BaseException):
                _logger.fatal(f'To save {id_values=} was failed. check the exception {model_or_exception}.')

