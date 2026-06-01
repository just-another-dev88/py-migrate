import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Generator, List, Tuple

logger = logging.getLogger("pymigrate.database")

class BaseDatabaseAdapter(ABC):
    """Abstract Base Class defining the contract for Database Adapters."""

    def __init__(self, config: Dict[str, Any], peer_configs: Dict[str, Any] = None):
        self.config = config
        self.peer_configs = peer_configs
        self.connection = None

    @abstractmethod
    def connect(self) -> None:
        """Establish the database connection."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the database connection and active cursors."""
        pass

    @abstractmethod
    def execute(self, query: str, params: Any = None) -> None:
        """Execute a write or utility query."""
        pass

    @abstractmethod
    def fetch_all(self, query: str, params: Any = None) -> List[Dict[str, Any]]:
        """Fetch all results for a query as a list of dictionaries."""
        pass

    @abstractmethod
    def fetch_stream(self, query: str, params: Any = None, chunk_size: int = 1000) -> Generator[List[Dict[str, Any]], None, None]:
        """Stream query results in chunks. Yields lists of dictionaries."""
        pass

    @abstractmethod
    def write_batch(self, table_name: str, columns: List[str], rows: List[Tuple]) -> int:
        """Write a batch of rows to a target table. Returns number of affected rows."""
        pass

    @abstractmethod
    def begin_transaction(self) -> None:
        """Explicitly start a transaction block."""
        pass

    @abstractmethod
    def commit(self) -> None:
        """Commit the active transaction."""
        pass

    @abstractmethod
    def rollback(self) -> None:
        """Rollback the active transaction."""
        pass


class PostgresAdapter(BaseDatabaseAdapter):
    """PostgreSQL Adapter using psycopg2."""

    def connect(self) -> None:
        import psycopg2
        if self.connection is None or self.connection.closed:
            logger.debug(f"Connecting to Postgres database: {self.config.get('database')} at {self.config.get('host')}")
            self.connection = psycopg2.connect(
                user=self.config.get("user"),
                password=self.config.get("password"),
                host=self.config.get("host"),
                port=self.config.get("port", 5432),
                database=self.config.get("database")
            )
            # Ensure autocommit is off so we manage transactions explicitly
            self.connection.autocommit = False

    def close(self) -> None:
        if self.connection and not self.connection.closed:
            logger.debug("Closing Postgres connection")
            self.connection.close()
            self.connection = None

    def execute(self, query: str, params: Any = None) -> None:
        self.connect()
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)

    def fetch_all(self, query: str, params: Any = None) -> List[Dict[str, Any]]:
        self.connect()
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            if cursor.description is None:
                return []
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def fetch_stream(self, query: str, params: Any = None, chunk_size: int = 1000) -> Generator[List[Dict[str, Any]], None, None]:
        self.connect()
        # For Postgres, we use server-side (named) cursors to stream without loading all rows into memory
        import uuid
        cursor_name = f"pymigrate_stream_{uuid.uuid4().hex}"
        
        # We need a separate connection or transaction to hold the server-side cursor cleanly
        # and prevent nested operations on the main connection.
        stream_conn = None
        try:
            import psycopg2
            stream_conn = psycopg2.connect(
                user=self.config.get("user"),
                password=self.config.get("password"),
                host=self.config.get("host"),
                port=self.config.get("port", 5432),
                database=self.config.get("database")
            )
            stream_conn.autocommit = False
            
            with stream_conn.cursor(cursor_name) as cursor:
                cursor.itersize = chunk_size
                cursor.execute(query, params)
                
                if cursor.description is None:
                    return
                columns = [col[0] for col in cursor.description]
                
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    yield [dict(zip(columns, row)) for row in rows]
            
            stream_conn.commit()
        except Exception as e:
            if stream_conn:
                stream_conn.rollback()
            raise e
        finally:
            if stream_conn:
                stream_conn.close()

    def write_batch(self, table_name: str, columns: List[str], rows: List[Tuple]) -> int:
        self.connect()
        if not rows:
            return 0
            
        col_list = ", ".join([f'"{col}"' for col in columns])
        val_placeholders = ", ".join(["%s"] * len(columns))
        query = f'INSERT INTO "{table_name}" ({col_list}) VALUES ({val_placeholders})'
        
        with self.connection.cursor() as cursor:
            cursor.executemany(query, rows)
            return cursor.rowcount

    def begin_transaction(self) -> None:
        self.connect()
        # In psycopg2, transactions are started automatically on first command, 
        # but we can call an empty statement or simply ensure autocommit is off.
        pass

    def commit(self) -> None:
        if self.connection and not self.connection.closed:
            logger.debug("Committing Postgres transaction")
            self.connection.commit()

    def rollback(self) -> None:
        if self.connection and not self.connection.closed:
            logger.debug("Rolling back Postgres transaction")
            self.connection.rollback()


class OracleAdapter(BaseDatabaseAdapter):
    """Oracle Database Adapter using oracledb in Thin mode."""

    def _setup_dblinks(self, conn) -> List[str]:
        created_dblinks = []
        if self.peer_configs:
            import re
            for peer_name, peer_cfg in self.peer_configs.items():
                if peer_cfg.get("type", "").lower() == "oracle":
                    # Sanitize peer name for dblink
                    sanitized_name = re.sub(r'[^a-zA-Z0-9_]', '_', peer_name)
                    if sanitized_name and sanitized_name[0].isdigit():
                        sanitized_name = "db_" + sanitized_name
                    
                    peer_user = peer_cfg.get("user")
                    peer_pwd = peer_cfg.get("password")
                    peer_dsn = peer_cfg.get("dsn")
                    
                    with conn.cursor() as cursor:
                        try:
                            cursor.execute(f"DROP DATABASE LINK {sanitized_name}")
                        except Exception:
                            pass
                        try:
                            cursor.execute(
                                f"CREATE DATABASE LINK {sanitized_name} "
                                f"CONNECT TO {peer_user} IDENTIFIED BY \"{peer_pwd}\" "
                                f"USING '{peer_dsn}'"
                            )
                            created_dblinks.append(sanitized_name)
                        except Exception as e:
                            logger.warning(f"Failed to create DB link {sanitized_name}: {e}")
        return created_dblinks

    def _teardown_dblinks(self, conn, created_dblinks: List[str]) -> None:
        if created_dblinks:
            with conn.cursor() as cursor:
                for dblink in created_dblinks:
                    try:
                        cursor.execute(f"DROP DATABASE LINK {dblink}")
                    except Exception as e:
                        logger.warning(f"Failed to drop DB link {dblink}: {e}")

    def connect(self) -> None:
        import oracledb
        if self.connection is None:
            logger.debug(f"Connecting to Oracle database (Thin mode): DSN={self.config.get('dsn')}")
            # Ensure oracledb Thin Mode is active (default in modern oracledb)
            self.connection = oracledb.connect(
                user=self.config.get("user"),
                password=self.config.get("password"),
                dsn=self.config.get("dsn")
            )
            self.connection.autocommit = False
            self._created_dblinks = self._setup_dblinks(self.connection)

    def close(self) -> None:
        if self.connection:
            logger.debug("Closing Oracle connection")
            if hasattr(self, "_created_dblinks") and self._created_dblinks:
                self._teardown_dblinks(self.connection, self._created_dblinks)
                self._created_dblinks = []
            try:
                self.connection.close()
            except Exception as e:
                logger.warning(f"Error while closing Oracle connection: {e}")
            self.connection = None

    def execute(self, query: str, params: Any = None) -> None:
        self.connect()
        with self.connection.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

    def fetch_all(self, query: str, params: Any = None) -> List[Dict[str, Any]]:
        self.connect()
        with self.connection.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            if cursor.description is None:
                return []
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def fetch_stream(self, query: str, params: Any = None, chunk_size: int = 1000) -> Generator[List[Dict[str, Any]], None, None]:
        self.connect()
        
        # Oracle thin connection for streaming to prevent mixing transaction states
        stream_conn = None
        stream_dblinks = []
        try:
            import oracledb
            stream_conn = oracledb.connect(
                user=self.config.get("user"),
                password=self.config.get("password"),
                dsn=self.config.get("dsn")
            )
            stream_conn.autocommit = False
            stream_dblinks = self._setup_dblinks(stream_conn)
            
            with stream_conn.cursor() as cursor:
                cursor.arraysize = chunk_size
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                    
                if cursor.description is None:
                    return
                columns = [col[0] for col in cursor.description]
                
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    yield [dict(zip(columns, row)) for row in rows]
                    
            stream_conn.commit()
        except Exception as e:
            if stream_conn:
                stream_conn.rollback()
            raise e
        finally:
            if stream_conn:
                if stream_dblinks:
                    try:
                        self._teardown_dblinks(stream_conn, stream_dblinks)
                    except Exception:
                        pass
                try:
                    stream_conn.close()
                except Exception:
                    pass

    def write_batch(self, table_name: str, columns: List[str], rows: List[Tuple]) -> int:
        self.connect()
        if not rows:
            return 0
            
        # Oracle uses bind variables (e.g. :1, :2, etc.) or name-based placeholders.
        # Let's use positional bind variables: :1, :2, ...
        col_list = ", ".join([f'"{col}"' for col in columns])
        val_placeholders = ", ".join([f":{i+1}" for i in range(len(columns))])
        query = f'INSERT INTO "{table_name}" ({col_list}) VALUES ({val_placeholders})'
        
        with self.connection.cursor() as cursor:
            cursor.executemany(query, rows)
            return cursor.rowcount

    def begin_transaction(self) -> None:
        self.connect()
        # Oracle connections always run inside a transaction session.
        pass

    def commit(self) -> None:
        if self.connection:
            logger.debug("Committing Oracle transaction")
            self.connection.commit()

    def rollback(self) -> None:
        if self.connection:
            logger.debug("Rolling back Oracle transaction")
            self.connection.rollback()


def get_adapter(db_config: Dict[str, Any], peer_configs: Dict[str, Any] = None) -> BaseDatabaseAdapter:
    """Factory to get the appropriate Database Adapter based on type."""
    db_type = db_config.get("type", "").lower()
    if db_type == "postgres" or db_type == "postgresql":
        return PostgresAdapter(db_config, peer_configs=peer_configs)
    elif db_type == "oracle":
        return OracleAdapter(db_config, peer_configs=peer_configs)
    else:
        raise ValueError(f"Unsupported database type: {db_type}. Supported types are: 'oracle', 'postgres'")
