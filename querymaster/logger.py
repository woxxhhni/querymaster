import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Union, Optional
from datetime import datetime

class QueryLogger:
    """Database query logger for tracking query execution and performance"""
    
    def __init__(
        self,
        log_file: Union[str, Path],
        name: str = "QueryMaster",
        log_level: int = logging.INFO,
        max_size: int = 10 * 1024 * 1024,  # 10MB
        backup_count: int = 5,
        console_output: bool = True
    ) -> None:
        """
        Initialize the query logger.
        
        Args:
            log_file: Path to log file
            name: Logger name (unique identifier)
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            max_size: Maximum size of each log file in bytes
            backup_count: Number of backup log files to keep
            console_output: Whether to also output logs to console
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)
        
        # Clear existing handlers to avoid duplicates
        self.logger.handlers = []
        
        # Ensure log directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create rotating file handler
        file_handler = RotatingFileHandler(
            filename=log_path,
            maxBytes=max_size,
            backupCount=backup_count,
            encoding='utf-8'
        )
        
        # Set formatter for detailed log entries
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(levelname)s - [%(name)s] - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Add console handler if requested
        if console_output:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def log(
        self, 
        message: str, 
        level: Union[int, str] = logging.INFO,
        extra: Optional[dict] = None
    ) -> None:
        """
        Log a message with the specified level and extra information.
        
        Args:
            message: Log message
            level: Log level (can be string or integer constant)
            extra: Additional fields to include in log entry
        """
        if isinstance(level, str):
            level = getattr(logging, level.upper(), logging.INFO)
            
        self.logger.log(level, message, extra=extra)

    def query_start(self, file_path: Union[str, Path]) -> None:
        """Log query file execution start"""
        self.info(f"Starting execution of query file: {file_path}")

    def query_end(self, file_path: Union[str, Path], execution_time: float) -> None:
        """Log query file execution completion"""
        self.info(f"Query file {file_path} completed in {execution_time:.2f} seconds")

    def query_error(self, file_path: Union[str, Path], error: Exception) -> None:
        """Log query file execution error"""
        self.error(f"Query file {file_path} failed with error: {str(error)}")

    def info(self, message: str) -> None:
        """Log an info message"""
        self.log(message, logging.INFO)

    def error(self, message: str) -> None:
        """Log an error message"""
        self.log(message, logging.ERROR)

    def warning(self, message: str) -> None:
        """Log a warning message"""
        self.log(message, logging.WARNING)
