from typing import cast
import pytest
from uuid import uuid4
import logging
import pickle

from pymysql.cursors import DictCursor

from ormdantic.database import DatabaseConnectionPool

from .tools import (
    use_temp_database, use_random_database_pool, 
    get_database_config
)

def test_use_random_database():
    with use_random_database_pool() as pool:
        with pool.open_cursor() as cursor:
            cursor.execute("show databases")
            result = cursor.fetchall()

            assert any(row['Database'] == pool.database_name for row in result)


def test_connect():
    with use_random_database_pool() as pool:
        with pool.connect() as connection:
            pass

        assert 1 == len(pool._cached)

        opened_connection = pool._cached[0]

        with pool.connect() as connection:
            assert opened_connection == connection

        with pytest.raises(RuntimeError, match='test'):
            with pool.connect() as connection:
                raise RuntimeError('test')
        
        assert 0 == len(pool._cached)


def test_connect_reconnect():
    with use_random_database_pool() as pool:
        connected = None
        with pool.connect() as connection:
            connected = connection

        if connected:
            connected.close()

        with pool.connect() as connection:
            assert connected == connection
            cursor = connection.cursor()
            cursor.execute('select 1')
            assert [{'1':1}] == cursor.fetchall()
            cursor.close()


def test_connect_close_if_ping_has_error(monkeypatch:pytest.MonkeyPatch):
    with use_random_database_pool() as pool:
        connected = None
        with pool.connect() as connection:
            connected = connection

        close_called = False

        def mock_ping(reconnect:bool=True):
            raise RuntimeError('test')

        def mock_close():
            nonlocal close_called
            close_called = True

        monkeypatch.setattr(connected, 'ping', mock_ping)
        original_close =  connected.close
        monkeypatch.setattr(connected, 'close', mock_close)

        with pool.connect() as connection:
            assert connected != connection

        assert close_called

        original_close()


def test_open_cursor(caplog : pytest.LogCaptureFixture):
    with use_random_database_pool() as pool:
        with pool.open_cursor(True) as cursor:
            cursor.execute("CREATE TABLE SAMPLE_TABLE(ID INT)")
            cursor.execute("INSERT SAMPLE_TABLE(ID) VALUES (1)")

        with pool.open_cursor() as cursor:
            with caplog.at_level(logging.DEBUG):
                cursor.execute("SELECT * FROM SAMPLE_TABLE")
                assert [dict(ID=1)] == cursor.fetchall()

            assert not caplog.text

        with pool.open_cursor(query_to_log=True) as cursor:
            with caplog.at_level(logging.DEBUG):
                cursor.execute("SELECT * FROM SAMPLE_TABLE")
                assert [dict(ID=1)] == cursor.fetchall()

            assert 'SELECT * FROM SAMPLE_TABLE' in caplog.text


def test_open_cursor_without_commit():
    with use_random_database_pool() as pool:
        with pool.open_cursor() as cursor:
            cursor.execute("CREATE TABLE SAMPLE_TABLE(ID INT)")

        with pool.open_cursor(False) as cursor:
            cursor.execute("INSERT SAMPLE_TABLE(ID) VALUES (1)")

            # test data is not exsited on new connection
            with pool.open_cursor() as cursor:
                cursor.execute("SELECT * FROM SAMPLE_TABLE")
                assert tuple() == cursor.fetchall()


def test_clear_pool():
    with use_random_database_pool() as pool:
        with pool.open_cursor() as cursor:
            cursor.execute("CREATE TABLE SAMPLE_TABLE(ID INT)")

        assert 1 == len(pool._cached)

        pool.clear_pool()

        assert 0 == len(pool._cached)


def test_close_all():
    with use_random_database_pool() as pool:
        with pool.open_cursor() as cursor:
            cursor.execute("CREATE TABLE SAMPLE_TABLE(ID INT)")
            assert 1 == len(pool._connected)

            pool.close_all()

            assert 0 == len(pool._connected)


def test_del():    
    database_name = f'TEST_{uuid4().hex}'

    with use_temp_database(database_name) as connection:
        pool = DatabaseConnectionPool(get_database_config(database_name))
        
        with pool.open_cursor() as cursor:
            cursor.execute('select 1')

        del pool

        # processlist does not reflect the current connection.
        # So, we need to retry to check it.
        retry = 3

        while retry > 0:
            try:
                cursor = connection.cursor(DictCursor)
                cursor.execute('show processlist')
                processlist = cursor.fetchall()

                cursor.close()

                assert [] == [row for row in processlist 
                    if row['db'] == database_name]
            except:
                retry -= 1
                if retry == 0:
                    raise
            else:
                break

 
def test_str():
    database_name = f'TEST_{uuid4().hex}'

    pool = DatabaseConnectionPool(get_database_config(database_name))

    assert str(pool).startswith(f'DatabaseConnectionPool(config=')
    assert 'cached' in str(pool)


def test_rollback_if_there_is_error():
    with use_random_database_pool() as pool:
        with pytest.raises(RuntimeError, match='test'):
            with pool.open_cursor(True) as cursor:
                cursor.execute("CREATE TABLE SAMPLE_TABLE(ID INT)")
                cursor.execute("INSERT SAMPLE_TABLE(ID) VALUES (1)")

                raise RuntimeError('test')

        with pool.open_cursor() as cursor:
            cursor.execute("SELECT ID FROM SAMPLE_TABLE")

            assert tuple() == cursor.fetchall()
        
        
def test_reduce():
    with use_random_database_pool() as pool:
        dumped = pickle.dumps(pool)

        loaded = cast(DatabaseConnectionPool, pickle.loads(dumped))

        assert pool._connection_config == loaded._connection_config


        
 