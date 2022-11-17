from typing import Iterator, Dict, Any, Optional, Type
from uuid import uuid4
from pymysql import Connection, connect
from pymysql.cursors import DictCursor
from contextlib import contextmanager

from ormdantic.database.storage import (
    DatabaseConnectionPool, create_table, upsert_objects
)
from ormdantic.database.connections import _DATABASE, _PASSWORD

from ormdantic.schema.base import PersistentModel, get_part_types
from ormdantic.schema.verinfo import VersionInfo
from ormdantic.util import is_derived_from, get_logger

_config = {
    'user':'orm',
    _PASSWORD:'iamroot',
    _DATABASE:'json_storage',
    'host':'localhost',
    'port':33069
}

_logger = get_logger(__name__)

def get_database_config(database_name:Optional[str] = None) -> Dict[str, Any]:
    if database_name:
        return dict(_config) | {_DATABASE:database_name}
    else:
        return _config


@contextmanager
def use_random_database_cursor(keep_database:bool = False) -> Iterator[DictCursor]:
    with use_random_database_pool(keep_database) as pool:
        with pool.open_cursor(True) as cursor:
            yield cursor
 

@contextmanager
def use_temp_database(database_name:str) -> Iterator[Connection]:
    with _create_database(database_name) as connection:
        try:
            yield connection
        finally:
            _drop_database(connection, database_name)
 

@contextmanager
def use_random_database_pool(keep_database_when_except:bool = False) -> Iterator[DatabaseConnectionPool]:
    database_name = f'TEST_{uuid4().hex}'

    with _create_database(database_name) as connection:
        config = _config | {_DATABASE: database_name}

        pool = DatabaseConnectionPool(config)

        try:
            yield pool
        except:
            pool.close_all()
            if not keep_database_when_except:
                _drop_database(connection, database_name)
            else:
                _logger.info(f'{database_name} is remained. please remove it after investigation.')

            raise
        else:
            pool.close_all()
            _drop_database(connection, database_name)


@contextmanager
def _create_database(database_name:str) -> Iterator[Connection]:
    connection = connect(**(_config | {_DATABASE:None}))
    cursor = connection.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(database_name))

    try:
        yield connection
    finally:
        cursor.close()
        connection.close()


def _drop_database(connection : Connection, database_name:str) -> None:
    cursor = connection.cursor()
    cursor.execute("DROP DATABASE IF EXISTS {}".format(database_name))
    cursor.close()


@contextmanager
def use_temp_database_pool_with_model(*args:Type) -> Iterator[DatabaseConnectionPool]:
    with use_random_database_pool() as pool:
        create_table(pool, *args)

        yield pool


@contextmanager
def use_temp_database_cursor_with_model(*models:PersistentModel, 
                                        model_created: bool = True,
                                        keep_database_when_except: bool = False
                                        ) -> Iterator[DictCursor]:
    with use_random_database_pool(keep_database_when_except) as pool:
        types = set()

        for model_obj in models:
            type_ = type(model_obj)

            if is_derived_from(type_, PersistentModel):
                types.add(type_)
                types.update(get_part_types(type_))
            else:
                raise TypeError(f'{type_} is not derived from PersistentModel')

        create_table(pool, *types)

        if model_created:
            upsert_objects(pool, models, 0, False, VersionInfo())

        with pool.open_cursor(True) as cursor:
            yield cursor
