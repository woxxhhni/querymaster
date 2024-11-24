from __future__ import annotations
import asyncpg
import oracledb
from typing import Dict, Union, List, AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from configparser import ConfigParser
from .database_errors import ConfigurationError, ConnectionError as DBConnectionError

class DatabaseConnector:
    """Base class for database connectors"""
    
    @staticmethod
    def validate_config(config: Dict[str, str], required_fields: List[str]) -> None:
        """Validate configuration has all required fields"""
        missing = [field for field in required_fields if field not in config]
        if missing:
            raise ConfigurationError(f"Missing required fields: {', '.join(missing)}")

class OracleConnector:
    """Oracle database connector"""
    
    def __init__(self, user: str, password: str, host: str, port: int, sid: str, timeout: int = 60) -> None:
        """
        Initialize Oracle connector.
        
        Args:
            user: Database username
            password: Database password
            host: Database host
            port: Database port
            sid: Database SID
            timeout: Connection timeout in seconds
        """
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.sid = sid
        self.timeout = timeout
        
        try:
            # 使用 thick mode
            oracledb.init_oracle_client()
        except Exception as e:
            raise ConfigurationError(f"Failed to initialize Oracle client: {str(e)}")

    @contextmanager
    def get_connection(self) -> Generator[oracledb.Connection, None, None]:
        """Get Oracle database connection"""
        conn = None
        try:
            # Use full connection string
            dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={self.host})(PORT={self.port}))(CONNECT_DATA=(SID={self.sid})))"
            
            conn = oracledb.connect(
                user=self.user,
                password=self.password,
                dsn=dsn,
                encoding="UTF-8"
            )
            yield conn
        except oracledb.DatabaseError as e:
            error_obj = e.args[0]
            raise DBConnectionError(
                f"Oracle connection failed: {error_obj.message}",
                error_code=str(error_obj.code)
            )
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

class PostgresConnector:
    """PostgreSQL database connector"""
    
    def __init__(
        self, 
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        pool_size: int = 10
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.pool_size = pool_size
        self.pool = None

    async def initialize_pool(self) -> None:
        """Initialize connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                min_size=1,
                max_size=self.pool_size
            )
        except Exception as e:
            raise DBConnectionError(f"Failed to create connection pool: {str(e)}")

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Get PostgreSQL database connection"""
        if not self.pool:
            await self.initialize_pool()
            
        try:
            async with self.pool.acquire() as conn:
                yield conn
        except Exception as e:
            raise DBConnectionError(f"Failed to acquire connection: {str(e)}")

def get_connector(config_file: Union[str, Path], section: str) -> Union[OracleConnector, PostgresConnector]:
    """
    Create database connector from configuration file.
    
    Args:
        config_file: Path to configuration file
        section: Configuration section name (e.g. 'ORACLE', 'cmrods_prd')
    """
    config = ConfigParser()
    config.read(Path(config_file))
    
    sections = config.sections()
    if not sections:
        raise ConfigurationError("Configuration file is empty or cannot be read")
    
    if section not in sections:
        raise ConfigurationError(f"Section '{section}' not found in configuration. Available sections: {', '.join(sections)}")
    
    db_config = dict(config[section])
    
    if 'driver' not in db_config:
        raise ConfigurationError(f"Missing 'driver' in section '{section}'")
    
    # 根据 driver 判断数据库类型
    if db_config['driver'].lower() == 'oracledb':
        required = ['user', 'password', 'host', 'port', 'sid']
        DatabaseConnector.validate_config(db_config, required)
        
        return OracleConnector(
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=int(db_config['port']),
            sid=db_config['sid']
        )
    
    elif 'postgresql' in db_config['driver'].lower():
        required = ['host', 'port', 'user', 'password', 'dbname']
        DatabaseConnector.validate_config(db_config, required)
        
        return PostgresConnector(
            host=db_config['host'],
            port=int(db_config['port']),
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['dbname']
        )
    
    else:
        raise ConfigurationError(f"Unsupported database driver: {db_config['driver']}")
