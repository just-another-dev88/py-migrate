import sys
from unittest.mock import MagicMock

# Dynamically mock 'oracledb' module before importing adapter logic
mock_oracledb = MagicMock()
sys.modules["oracledb"] = mock_oracledb

from pymigrate.database import get_adapter, OracleAdapter

def test_oracle_adapter_db_links_setup_and_teardown():
    # Reset the mock connect before the test
    mock_oracledb.connect.reset_mock()
    
    # Configure two Oracle shards
    config1 = {
        "type": "oracle",
        "user": "system",
        "password": "pwd1",
        "dsn": "host1:1521/FREEPDB1"
    }
    
    peers = {
        "oracle-shard-2": {
            "type": "oracle",
            "user": "system",
            "password": "pwd-with-special!#",
            "dsn": "host2:1521/FREEPDB2"
        },
        "postgres_target": {
            "type": "postgres",
            "user": "postgres",
            "password": "password",
            "host": "localhost",
            "database": "tgt"
        }
    }

    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_oracledb.connect.return_value = mock_connection

    adapter = get_adapter(config1, peer_configs=peers)
    
    # When we connect, it should connect and setup DB link to oracle_shard_2 (and ignore Postgres peer)
    adapter.connect()
    
    # Verify connected once
    mock_oracledb.connect.assert_called_once_with(
        user="system",
        password="pwd1",
        dsn="host1:1521/FREEPDB1"
    )
    
    # Verify DB Link commands were executed
    cursor_calls = mock_cursor.execute.call_args_list
    # Filter drop/create statements
    drop_calls = [c[0][0] for c in cursor_calls if "DROP DATABASE LINK" in c[0][0]]
    create_calls = [c[0][0] for c in cursor_calls if "CREATE DATABASE LINK" in c[0][0]]
    
    # Sanitization: "oracle-shard-2" -> "oracle_shard_2"
    assert len(drop_calls) == 1
    assert "DROP DATABASE LINK oracle_shard_2" in drop_calls[0]
    
    assert len(create_calls) == 1
    assert "CREATE DATABASE LINK oracle_shard_2" in create_calls[0]
    assert "CONNECT TO system" in create_calls[0]
    assert 'IDENTIFIED BY "pwd-with-special!#"' in create_calls[0]
    assert "USING 'host2:1521/FREEPDB2'" in create_calls[0]
    
    # Reset mock for close testing
    mock_cursor.execute.reset_mock()
    
    # When we close, it should drop the created DB links
    adapter.close()
    mock_cursor.execute.assert_called_once_with("DROP DATABASE LINK oracle_shard_2")


def test_oracle_adapter_fetch_stream_db_links():
    mock_oracledb.connect.reset_mock()
    
    config1 = {
        "type": "oracle",
        "user": "system",
        "password": "pwd1",
        "dsn": "host1:1521/FREEPDB1"
    }
    
    peers = {
        "oracle-shard-2": {
            "type": "oracle",
            "user": "system",
            "password": "pwd2",
            "dsn": "host2:1522/FREEPDB2"
        }
    }

    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = [("COL1",)]
    mock_cursor.fetchmany.side_effect = [[("val1",)], []]
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_oracledb.connect.return_value = mock_connection

    adapter = get_adapter(config1, peer_configs=peers)
    
    # Consume the stream
    list(adapter.fetch_stream("SELECT * FROM table"))
    
    # Verify that oracledb.connect was called (once for connect(), once for fetch_stream())
    assert mock_oracledb.connect.call_count == 2
    
    # Verify DB links were created for stream connection
    executed_sqls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    
    # Name should be sanitized (oracle_shard_2 instead of oracle-shard-2)
    assert any("DROP DATABASE LINK oracle_shard_2" in sql for sql in executed_sqls)
    assert any("CREATE DATABASE LINK oracle_shard_2" in sql for sql in executed_sqls)
