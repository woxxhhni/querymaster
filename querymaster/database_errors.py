from typing import Optional, Any, Dict


class DatabaseError(Exception):
    """Base exception for all database-related errors"""
    def __init__(self, message: str, error_code: Optional[str] = None, details: Optional[Dict] = None) -> None:
        """
        Initialize database error with detailed information.

        Args:
            message: Error message describing what went wrong
            error_code: Optional error code from the database
            details: Optional dictionary containing additional error details
        """
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        super().__init__(self.message)


class ConnectionError(DatabaseError):
    """Raised when a database connection cannot be established"""
    def __init__(
        self,
        message: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(message, *args, **kwargs)
        self.host = host
        self.port = port


class QueryError(DatabaseError):
    """Raised when a database query fails to execute"""
    def __init__(
        self, 
        message: str, 
        query: Optional[str] = None, 
        params: Optional[Dict] = None, 
        *args, 
        **kwargs
    ) -> None:
        """
        Initialize query error with query details.

        Args:
            message: Error message describing what went wrong
            query: The SQL query that failed
            params: The parameters that were used in the query
        """
        super().__init__(message, *args, **kwargs)
        self.query = query
        self.params = params


class PoolError(DatabaseError):
    """Raised when there are issues with the connection pool"""
    def __init__(
        self,
        message: str,
        pool_size: Optional[int] = None,
        current_connections: Optional[int] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(message, *args, **kwargs)
        self.pool_size = pool_size
        self.current_connections = current_connections


class ConfigurationError(DatabaseError):
    """Raised when there are database configuration issues"""
    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None,
        config_value: Optional[Any] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(message, *args, **kwargs)
        self.config_key = config_key
        self.config_value = config_value


class TransactionError(DatabaseError):
    """Raised when there are transaction-related issues"""
    def __init__(
        self, 
        message: str, 
        transaction_id: Optional[str] = None, 
        *args, 
        **kwargs
    ) -> None:
        """
        Initialize transaction error.

        Args:
            message: Error message describing what went wrong
            transaction_id: Optional identifier for the failed transaction
        """
        super().__init__(message, *args, **kwargs)
        self.transaction_id = transaction_id


class TimeoutError(DatabaseError):
    """Raised when a database operation times out"""
    def __init__(
        self, 
        message: str, 
        operation: Optional[str] = None, 
        timeout_value: Optional[float] = None, 
        *args, 
        **kwargs
    ) -> None:
        """
        Initialize timeout error.

        Args:
            message: Error message describing what went wrong
            operation: The operation that timed out
            timeout_value: The timeout value that was exceeded
        """
        super().__init__(message, *args, **kwargs)
        self.operation = operation
        self.timeout_value = timeout_value


class DataError(DatabaseError):
    """Raised when there are issues with the data being processed"""
    def __init__(
        self, 
        message: str, 
        column: Optional[str] = None, 
        value: Optional[Any] = None, 
        expected_type: Optional[str] = None, 
        *args, 
        **kwargs
    ) -> None:
        """
        Initialize data error.

        Args:
            message: Error message describing what went wrong
            column: The column where the data error occurred
            value: The problematic value
            expected_type: The expected data type
        """
        super().__init__(message, *args, **kwargs)
        self.column = column
        self.value = value
        self.expected_type = expected_type
