"""
QueryMaster - Asynchronous SQL Query Execution Tool

A comprehensive database query management system that supports asynchronous 
operations for Oracle and PostgreSQL databases.

Features:
    - Asynchronous query execution
    - Multiple database support (Oracle, PostgreSQL)
    - Connection pooling
    - Query parameterization
    - Multiple output formats (CSV, Parquet, JSON)
    - Comprehensive logging
    - Error handling
"""

__version__ = "1.0.0"
__author__ = "Yi Ren"
__license__ = "MIT"

from .core import QueryMaster
from .db_connectors import OracleConnector, PostgresConnector
from .database_errors import DatabaseError, ConfigurationError
from .logger import QueryLogger

__all__ = [
    "QueryMaster",
    "OracleConnector",
    "PostgresConnector",
    "DatabaseError",
    "ConfigurationError",
    "QueryLogger"
]
