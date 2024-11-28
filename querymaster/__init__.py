"""
QueryMaster - Asynchronous SQL Query Execution Tool

A comprehensive database query management system that supports asynchronous 
operations for Oracle and PostgreSQL databases.

Features:
    - Asynchronous query execution
    - Multiple database support (Oracle, PostgreSQL)
    - Parameter management (config file and runtime)
    - Output format support (CSV, Parquet)
    - Independent file execution
    - Comprehensive error handling
    - Connection pooling and retry mechanism
"""

__version__ = "1.0.0"
__author__ = "Yi Ren"
__license__ = "MIT"

from .core import QueryMaster
from .query_executor import QueryExecutor
from .db_connectors import OracleConnector, PostgresConnector
from .database_errors import DatabaseError, ConfigurationError
from .logger import QueryLogger

__all__ = [
    "QueryMaster",
    "QueryExecutor",
    "OracleConnector",
    "PostgresConnector",
    "DatabaseError",
    "ConfigurationError",
    "QueryLogger"
]
