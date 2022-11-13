from collections import defaultdict
from typing import (Type, Dict, Any, Tuple, Iterator, TypeVar, cast, 
                    Iterable, List, overload, DefaultDict, FrozenSet)
from datetime import date

import copy
import orjson

from ormdantic.util.tools import convert_as_list_or_tuple

from ..util import get_logger, is_derived_from

from .typed import parse_object_for_model
from .base import (
    PersistentModelT, PersistentModel, assign_identifying_fields_if_empty, 
    get_identifying_fields, get_stored_fields, ScalarType
)
from .shareds import (
    PersistentSharedContentModel, collect_shared_model_field_type_and_ids, 
    populate_shared_models, iterate_isolated_models
)
from .paths import extract
from .modelcache import ModelCache
from .verinfo import VersionInfo

_logger = get_logger(__name__)

_SourceT = TypeVar('_SourceT', bound='ModelSource')

NormalizedQueryConditionType = Dict[str, Tuple[str, Any]]
QueryConditionType = Dict[str, Tuple[str, Any] | ScalarType]

class SharedModelSource:
    ''' Usually, shared model is not different by version or ref_date. So we
        don't need version and ref_date. 

        unchanged item is existed for each id.
    '''
    def __init__(self, cache: ModelCache | None = None):
        self._cache = cache or ModelCache()

    def find(self, type_:Type[PersistentModelT], id:str | int) -> PersistentModelT | None: 
        ''' check internal cache and load item if not exist. '''
        if self._cache.has_entry(type_, id):
            return self._cache.find(type_, id)

        record = next(self.find_records_by_ids(type_, id), None)

        if record:
            return cast(PersistentModelT, self._cache.register(type_, _parse_model(type_, *record)))

        return None

    def find_multiple(self, type_:Type[PersistentModelT], *ids:str | int) -> Iterator[PersistentModelT]:
        ''' check internal cache and load items if not exist. '''
        to_find = [id for id in ids if not self._cache.has_entry(type_, id)]

        for record in self.find_records_by_ids(type_, *to_find):
            self._cache.register(type_, _parse_model(type_, *record))

        for id in ids:
            found = self._cache.find(type_, id)
            if found:
                yield found

    def load(self, type_:Type, id:str | int):
        found = self.find(type_, id)

        if found is None:
            _logger.fatal(f'cannot load {type_=}:{id=} from {self=}')
            raise RuntimeError('no such SharedModel')

        return found

    def find_records_by_ids(self, type_:Type, *ids:str | int) -> Iterator[Tuple[str, int]]:
        raise NotImplementedError('_find_records should be implemented')

    def _build_shared_model_set(self, model:PersistentModel):
        type_and_ids = collect_shared_model_field_type_and_ids(model)

        for shared_type, shared_ids in type_and_ids.items():
            to_be_retreived = tuple(id for id in shared_ids 
                                    if not self._cache.has_entry(shared_type, id))

            if not to_be_retreived:
                continue

            for shared_model in self.find_multiple(shared_type, *shared_ids):
                self._build_shared_model_set(shared_model)

    def populate_shared_models(self, model:PersistentModelT) -> PersistentModelT:
        self._build_shared_model_set(model)

        return populate_shared_models(model, self._cache)


class ModelSource:
    def __init__(self, 
                 shared_source: SharedModelSource, ref_date: date,
                 version: int | None, name: str = '', *, 
                 cache: ModelCache | None = None):
        self._shared_source = shared_source
        self._ref_date = ref_date
        self._version = version or self.get_latest_version()
        self._name = name

        self._cache = cache or ModelCache()

    def query_records(self, type_: Type, query_condition: QueryConditionType,
                     *,
                     fetch_size: int | None = None,
                     fields: Tuple[str, ...] = tuple(),
                     order_by: Tuple[str, ...] | str = tuple(),
                     limit: int | None = None,
                     offset: int | None = None,
                     unwind: Tuple[str, ...] | str = tuple(),
                     joined: Dict[str, Type[PersistentModel]] | None = None
                     ) -> Iterator[Dict[str, Any]]:
        raise NotImplementedError('query should be implemented')
     
    def find_record(self, type_:Type, 
                    query_condition: QueryConditionType, 
                    unwind: Tuple[str, ...] | str = tuple()) -> Tuple[str, int] | None:
        raise NotImplementedError('find_record should be implemented')

    def get_latest_version(self) -> int:
        raise NotImplementedError('get current version should be implemented')

    def update_version(self, version:int | None = None) -> int:
        current = self.get_latest_version() if version is None else min(version, self.get_latest_version())

        if current != self._version:
            self._version = current
            self._cache = ModelCache()

        return current

    def find(self, type_:Type[PersistentModelT], query_condition:QueryConditionType, 
             populated: bool = False, 
             unwind: Tuple[str, ...] | str = tuple()) -> PersistentModelT | None:
        id_values = extract_id_values(type_, query_condition)

        if id_values:
            if is_derived_from(type_, PersistentSharedContentModel):
                model = cast(PersistentModelT, self._shared_source.find(type_, id_values[0]))
            else:
                model = cast(Any, self._cache.get(type_, id_values, self._find_model))

            if model and populated:
                return self._shared_source.populate_shared_models(model)

            return model
        else:     
            items = self.query(type_, query_condition, populated=populated, unwind=unwind)
            first = next(items, None)

            if not first:
                return None

            second = next(items, None)

            if second:
                _logger.fatal(f'multiple items for find. {query_condition=} for {type_=} in {self=}')
                raise RuntimeError(f'multiple items for find')

            return first
    
    def load(self, type_:Type[PersistentModelT], 
             query_condition: QueryConditionType,
             populated: bool = False, 
             unwind: Tuple[str, ...] | str = tuple()) -> PersistentModelT:

        fetch = self.find(type_, query_condition, 
                          populated=populated, unwind=unwind)

        if fetch is None:
            _logger.fatal(f'cannot load {type_=}:{query_condition=} from {self=}')
            raise RuntimeError(f'no such {type_.__name__}')

        return fetch

    def query(self, type_:Type[PersistentModelT], 
              query_condition: QueryConditionType,
              populated: bool = False,
              unwind: Tuple[str, ...] | str = tuple()) -> Iterator[PersistentModelT]:

        for identified in self._iterate_identified_result_record(type_, query_condition, unwind):
            found = self.find(type_, identified, populated=populated)

            if found:
                yield found

    def clone_with(self:_SourceT, ref_date:date | None = None, 
                   version: int | None = None) -> _SourceT:
        copied = copy.copy(self)
        changed = False

        if ref_date and copied._ref_date != ref_date:
            copied._ref_date = ref_date
            changed = True

        if version is None:
            version = self.get_latest_version()
        else:
            version = min(version, self.get_latest_version())

        if copied._version != version:
            copied._version = version
            changed = True

        if changed:
            copied._cache = ModelCache() 
        else:
            copied._cache = self._cache

        return copied

    def _find_model(self, type_:Type, *id_values:Any):
        fields = get_identifying_fields(type_)

        record = self.find_record(type_, dict(zip(fields, id_values)))

        if record:
            return _parse_model(type_, *record)

        return None

    def _iterate_identified_result_record(self,
                                          type_: Type, 
                                          query_condition: QueryConditionType,
                                          unwind: Tuple[str, ...] | str = tuple()
                                          ) -> Iterator[Dict[str, Any]]:
        fields = get_identifying_fields(type_)

        if fields and len(fields) == len(query_condition.keys()):
            if all(f in query_condition and _is_op_equals(query_condition[f]) for f in fields):
                yield query_condition
                return

        for record in self.query_records(type_, query_condition, fields=fields, unwind=unwind):
            yield record

def _is_op_equals(item:Tuple[str, Any] | ScalarType) -> bool:
    return not isinstance(item, tuple) or item[0] == '='

class ModelStorage(ModelSource):
    @overload
    def store(self, objs:PersistentModel, version_info:VersionInfo) -> PersistentModel:
        ...

    @overload
    def store(self, 
              objs: Iterable[PersistentModel], version_info: VersionInfo
              ) -> Iterable[PersistentModel]:
        ...

    def store(self, objs:Iterable[PersistentModel] | PersistentModel, 
              version_info: VersionInfo) -> Iterable[PersistentModel] | PersistentModel:
        raise NotImplementedError('store should be implemented')

    def squash(self, type_:Type, query_condition:QueryConditionType,
               version_info: VersionInfo, unwind:Tuple[str,...]|str = tuple()) -> List[Dict[str, Any]]:

        return self._squash_models(type_,
                           self._iterate_identified_result_record(
                               type_, query_condition, unwind),
                           version_info)
                                    
    
    def _squash_models(self, type_:Type, identifieds:Iterator[Dict[str, Any]],
               version_info: VersionInfo) -> List[Dict[str, Any]]:
        raise NotImplementedError('_squash_model should be implemented')

    def delete(self, type_:Type, query_condition:QueryConditionType,
               version_info: VersionInfo, unwind:Tuple[str,...]|str = tuple()) -> List[Dict[str, Any]]:

        return self._delete_models(type_,
                           self._iterate_identified_result_record(
                               type_, query_condition, unwind),
                           version_info)

    def _delete_models(self, type_:Type, identifieds:Iterator[Dict[str, Any]],
               version_info: VersionInfo) -> List[Dict[str, Any]]:
        raise NotImplementedError('_squash_model should be implemented')

    def purge(self, type_:Type, query_condition:QueryConditionType,
               version_info: VersionInfo, unwind:Tuple[str,...]|str = tuple()) -> List[Dict[str, Any]]:
        return self._purge_models(type_,
                           self._iterate_identified_result_record(
                               type_, query_condition, unwind),
                           version_info)

    def _purge_models(self, type_:Type, identifieds:Iterator[Dict[str, Any]],
               version_info: VersionInfo) -> List[Dict[str, Any]]:
        raise NotImplementedError('_squash_model should be implemented')

def _parse_model(type_:Type[PersistentModelT], json:str, row_id:int) -> PersistentModelT:
    model = parse_object_for_model(orjson.loads(json), type_)
    model._row_id = row_id

    model._after_load()

    return model


class ChainedSharedModelSource(SharedModelSource):
    def __init__(self, *sources:SharedModelSource):
        self._sources = sources

    def __reduce__(self):
        return (ChainedSharedModelSource, self._sources)

    def find(self, type_:Type[PersistentModel], id:str | int) -> PersistentModel | None: 
        for s in self._sources:
            found = s.find(type_, id)
            if found:
                return found

        return None

    def find_multiple(self, type_: Type, *ids: str | int) -> Iterator[PersistentModel]:
        for id in ids:
            found = self.find(type_, id)

            if found:
                yield found

    def find_records_by_ids(self, type_: Type, *ids: str | int) -> Iterator[Tuple[str, int]]:
        for id in ids:
            for s in self._sources:
                found = list(s.find_records_by_ids(type_, id))

                if found:
                    yield from found
                    break

    def populate_shared_models(self, model:PersistentModelT) -> PersistentModelT:
        for s in self._sources:
            s._build_shared_model_set(model)

        return populate_shared_models(model, *(s._cache for s in self._sources))


class ChainedModelSource(ModelSource):
    def __init__(self, *sources:ModelSource):
        self._sources = sources
     
    def __reduce__(self):
        return (ChainedModelSource, self._sources)

    def query_records(self, type_:Type, where:QueryConditionType, 
              *,
              fetch_size: int | None = None,
              fields: Tuple[str, ...] = tuple(),
              order_by: Tuple[str, ...] | str = tuple(),
              limit: int | None = None,
              offset: int | None = None,
              unwind: Tuple[str, ...] | str = tuple(),
              joined: Dict[str, Type[PersistentModel]] | None = None) -> Iterator[Dict[str, Any]]:
        raise NotImplementedError('query should be implemented')
     
    def find_record(self, type_:Type, query_condition:QueryConditionType, 
                    unwind: Tuple[str, ...] | str = tuple()) -> Tuple[str, int] | None:
        for source in self._sources:
            found = source.find_record(type_, query_condition, unwind)
            
            if found:
                return found

        return None

    def update_version(self, version:int | None = None) -> int:
        return max(source.update_version(version) for source in self._sources)

    def get_latest_version(self) -> int:
        return max(source.get_latest_version() for source in self._sources)

    def find(self, type_:Type[PersistentModel], *id_values:Any,
             populated: bool = False) -> PersistentModel | None:
        for source in self._sources:
            found = source.find(type_, *id_values, populated=populated)

            if found:
                return found

        return None

    def query(self, type_:Type[PersistentModel], query_condition:QueryConditionType, *, 
            populated:bool = False) -> Iterator[PersistentModel]:

        fields = get_identifying_fields(type_)
        id_values_set = set()

        for source in self._sources:
            for record in source.query_records(type_, query_condition, fields=fields):
                id_values = tuple(record[f] for f in fields)
                id_values_set.add(id_values)

        for id_values in id_values_set:
            for s in self._sources:
                found = s.find(type_, dict(zip(fields, id_values)), populated=populated)

                if found:
                    yield found
                    break

    def clone_with(self, ref_date:date | None = None, version: int | None = None):
        if version is None:
            version = self.get_latest_version()

        return ChainedModelSource(*tuple(s.clone_with(ref_date, version) for s in self._sources))


class MemorySharedModelSource(SharedModelSource):
    ''' dummy model source for testing or temporary source '''
    def __init__(self, shared_models:Iterable[PersistentSharedContentModel]):
        cache = ModelCache()

        for model in shared_models:
            cache.register(type(model), model)

        super().__init__(cache)

    def __reduce__(self):
        return (MemorySharedModelSource, (list(self._cache.iterate_all()),))

    def store(self, model:PersistentSharedContentModel):
        self._cache.register(type(model), model)

    def delete(self, type, id:str):
        self._cache.delete(type, id)

    def find_records_by_ids(self, type_:Type, *ids:str | int) -> Iterator[Tuple[str, int]]:
        # we keep all model in cache. 
        return
        yield


class MemoryModelStorage(ModelStorage):
    def __init__(self, models:Iterable[PersistentModel], shared_source:SharedModelSource | None = None, name:str=''):
        model_sets = [[], []]

        for model in models:
            model_sets[isinstance(model, PersistentSharedContentModel)].append(model)

        shared_source = shared_source or MemorySharedModelSource(model_sets[1])

        super().__init__(shared_source, date.today(), 0, name=name)

        for model in model_sets[0]:
            self._cache.register(type(model), model)

        self._model_maps = _build_model_maps(model_sets[0])

    def __reduce__(self):
        return (MemoryModelStorage, 
                (list(self._cache.iterate_all()), self._shared_source, self._name))

    def query_records(self, type_: Type, query_condition: QueryConditionType,
                      *,
                      fetch_size: int | None = None,
                      fields: Tuple[str, ...] = tuple() ,
                      order_by: Tuple[str, ...] | str = tuple(),
                      limit: int | None = None,
                      offset: int | None = None,
                      unwind: Tuple[str, ...] | str = tuple(),
                      joined: Dict[str, Type[PersistentModel]] | None = None
                      ) -> Iterator[Dict[str, Any]]:
        if joined or unwind or offset or order_by or limit:
            raise NotImplementedError('query_record should be implemented for specific case')

        key_and_values = []

        for field, op_value in query_condition.items():
            if isinstance(op_value, tuple):
                if op_value[0] != '=':
                    raise NotImplementedError('query_record should be implemented for specific op')
                sub_key = (field, op_value[1])
            else:
                sub_key = (field, op_value)

            key_and_values.append(sub_key)

        targets = self._model_maps[type_]

        keys = _find_matched_keys(targets, frozenset(key_and_values))

        for key in keys:
            yield {f:v for f, v in targets[key].items() if not fields or f in fields}

    def find_record(self, type_:Type, query_cond:QueryConditionType, unwind:Tuple[str,...]|str = tuple()) -> Tuple[str, int] | None:
        return None

    def get_latest_version(self) -> int:
        # to return 0 makes cache kept.
        return 0

    def store(self, models:Iterable[PersistentModel] | PersistentModel, 
              version_info: VersionInfo):
        models = [models] if isinstance(models, PersistentModel) else models

        saved = []

        for model in models:
            model = assign_identifying_fields_if_empty(model)

            model._before_save()

            for sub_model in iterate_isolated_models(model):
                if isinstance(sub_model, PersistentSharedContentModel):
                    shared_source = self._shared_source
                    if isinstance(shared_source, MemorySharedModelSource):
                        shared_source.store(sub_model)
                else:
                    self._cache.register(type(model), model)
                    model_dict = model.dict()

                    self._model_maps[type(model)][_build_model_map_key(type(model), model_dict)] = model_dict

            saved.append(model)

        return saved

    def squash(self, type_:Type, query_condition:QueryConditionType, version_info:VersionInfo):
        return []

    def _delete_models(self, type_:Type, identifieds:Iterator[Dict[str, Any]], version_info: VersionInfo):
        if is_derived_from(type_, PersistentSharedContentModel):
            shared_source = self._shared_source
            for identified in identifieds:
                if isinstance(shared_source, MemorySharedModelSource):
                    shared_source.delete(type_, next(iter(identified.values())))
        else:
            for identified in identifieds:
                targets = self._model_maps[type_]

                self._cache.delete(type_, *identified.values())

                key_and_values = frozenset(identified.items())
                keys = _find_matched_keys(targets, key_and_values)

                for key in keys:
                    targets.pop(key)

    def purge(self, type_:Type, query_condition:QueryConditionType, version_info:VersionInfo):
        self.delete(type_, query_condition, version_info)


def extract_id_values(type_:Type, where_condition:QueryConditionType) -> Tuple[Any,...] | None:
    id_fields = get_identifying_fields(type_)
    id_values = []

    if not id_fields:
        _logger.fatal(f'identified field should be existed for {type_=}')
        raise RuntimeError('no identified fields')

    for field in id_fields:
        if field not in where_condition:
            return None

        value = where_condition[field]

        if isinstance(value, tuple):
            if value[0] != '=':
                return None
            
            id_values.append(value[1])
        else:
            id_values.append(value)

    return tuple(id_values)


def to_normalize_query_condition(query_condition:QueryConditionType) -> NormalizedQueryConditionType:
    return {
        field:(value if isinstance(value, tuple) else ('=', value))
        for field, value in query_condition.items()
    }


def _build_model_maps(models:List[PersistentModel]):
    model_maps: DefaultDict[Type, Dict[FrozenSet[Tuple[str, Any]], Dict[str, Any]]] = defaultdict(dict)

    for model in models:
        model_type = type(model)
        model_dict = model.dict()

        model_maps[model_type][_build_model_map_key(model_type, model_dict)] = model_dict

    return model_maps


def _find_matched_keys(targets:Dict[FrozenSet[Tuple[str, Any]], Any], 
                       key_and_values: FrozenSet[Tuple[str, Any]]) -> List[FrozenSet]:
    return [key for key in targets.keys() if key_and_values <= key]


def _build_model_map_key(model_type:Type, model_dict:Dict[str, Any]) -> FrozenSet[Tuple[str, Any]]:
    key = []

    for field, (paths, _) in get_stored_fields(model_type).items():
        values = cast(Any, model_dict)

        for path in paths:
            values = extract(values, path)

        items = convert_as_list_or_tuple(values)

        if len(items) == 1:
            field_value = items[0]
        else:
            field_value = tuple(items) if isinstance(items, list) else items

        key.append((field, field_value))

    return frozenset(key)

