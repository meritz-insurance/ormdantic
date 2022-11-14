from typing import Dict, Any, Deque, Iterator, Set

from collections import deque
from contextlib import contextmanager

from pymysql import Connection, connect
from pymysql.cursors import DictCursor
from pymysql.constants import CLIENT

from ..util import get_logger


_logger = get_logger(__name__)

_DATABASE = 'database'
_PASSWORD = 'password'

class DatabaseConnectionPool():
    # connect will make database connection object. if you finish to use,
    # it will be saved in self._connections for reuse it.

    @property
    def database_name(self):
        return self._connection_config[_DATABASE]

    def __init__(self, config:Dict[str, Any]):
        self._connection_config = config | {'client_flag':CLIENT.MULTI_STATEMENTS}
        self._cached : Deque[Connection] = deque()
        self._connected : Set[Connection] = set()

    def __del__(self):
        self.close_all()

    def __str__(self):
        return (
            f"DatabaseConnectionPool(config={self._connection_config}) "
            f"/ {self._cached=}, {self._connected=}"
        )

    def __reduce__(self):
        return (DatabaseConnectionPool, (self._connection_config,))

    @contextmanager
    def open_cursor(self, commit:bool = False, *, query_to_log:bool=False) -> Iterator[DictCursor]:
        with self.connect() as connection:
            cur = connection.cursor(DictCursor)

            if query_to_log:
                old = cur.execute

                def _execute_and_log(*args, **kwds):
                    _logger.debug(args)
                    return old(*args, **kwds)

                cur.execute = _execute_and_log

            try:
                yield cur

                if commit:
                    connection.commit()
            except Exception as e:
                if commit:
                    _logger.info(f'The data will be rollbacked due to exception {e=}')
                    connection.rollback()
                raise
            finally:
                cur.close()

    @contextmanager
    def connect(self) -> Iterator[Connection]:
        connection = self._cached_connect()
        managed = True

        try:
            self._connected.add(connection)
            yield connection

            # sometimes, connection was removed from set if close_all was called before exit.
            if connection in self._connected:
                self._connected.remove(connection)
            else:
                managed = False
        except Exception as e:
            # connection will be closed and do not register it on cache
            _logger.info(f'The connection will be closed due to exception {e=}')
            connection.close()
            raise
        else:
            if managed:
                self._cached.append(connection)

    def clear_pool(self):
        for connection in self._cached:
            connection.close()

        self._cached.clear()

    def close_all(self):
        self.clear_pool()

        while self._connected and (connection := self._connected.pop()):
            if connection.open:
                connection.close()

    def _cached_connect(self) -> Connection:
        while self._cached:
            old_one = self._cached.pop()

            try:
                old_one.ping(reconnect=True)
                return old_one
            except:
                pass

            old_one.close()

        new_one = connect(**self._connection_config, cursorclass=DictCursor)

        return new_one

