from __future__ import annotations
import asyncio
import pandas as pd
from typing import Dict, List, Optional, Union, Any
from pathlib import Path
from configparser import ConfigParser
from concurrent.futures import ThreadPoolExecutor
from .db_connectors import get_connector, OracleConnector, PostgresConnector
from .logger import QueryLogger
from .database_errors import DatabaseError, ConfigurationError
import time


class QueryMaster:
    """
    Database Query Manager Class
    A comprehensive database query management system that supports asynchronous 
    operations for Oracle and PostgreSQL databases.

    Features:
        - Asynchronous query execution
        - Connection pooling and management
        - Concurrent query control
        - Query logging and monitoring
        - Multiple output formats support
        - Batch query processing
        - Error handling and recovery
        - Resource management

    Supported Databases:
        - Oracle
        - PostgreSQL

    Supported Output Formats:
        - CSV
        - Parquet
        - JSON

    Example:
        # Initialize query manager
        query_master = QueryMaster(
            section="ORACLE",  # or "cmrods_prd" for PostgreSQL
            config_file="config/connection.ini",
            max_concurrent_queries=5
        )

        # Execute single query
        result = await query_master.execute_query(
            query="SELECT * FROM users WHERE status = :status",
            params={"status": "active"},
            output_file="active_users.csv"
        )
    """

    def __init__(
        self, 
        section: str, 
        config_file: Union[str, Path], 
        log_file: Optional[Union[str, Path]] = None, 
        max_concurrent_queries: int = 5,
        thread_pool: Optional[ThreadPoolExecutor] = None
    ) -> None:
        """
        Initialize the QueryMaster instance.

        Args:
            section: Database section name from config file
            config_file: Path to database configuration file
            log_file: Path to log file for query operations
            max_concurrent_queries: Maximum number of concurrent queries allowed
            thread_pool: Optional custom thread pool for Oracle queries
        """
        config_path = Path(config_file)
        if not config_path.exists():
            raise ConfigurationError(f"Configuration file not found: {config_path}")
            
        if max_concurrent_queries < 1:
            raise ConfigurationError("max_concurrent_queries must be greater than 0")
            
        self.section = section
        logger_name = f"QueryMaster_{section}"
        self.logger = QueryLogger(
            log_file=log_file,
            name=logger_name
        ) if log_file else QueryLogger(name=logger_name)
        self.db_connector = get_connector(config_file, section)
        self.semaphore = asyncio.Semaphore(max_concurrent_queries)
        
        # Initialize thread pool for Oracle queries
        if isinstance(self.db_connector, OracleConnector):
            self.thread_pool = thread_pool or ThreadPoolExecutor(
                max_workers=max_concurrent_queries,
                thread_name_prefix="oracle_query_"
            )
        else:
            self.thread_pool = None

    async def execute_query(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None, 
        output_file: Optional[Union[str, Path]] = None, 
        output_format: str = "csv",
        _silent: bool = False,
        _log_save: bool = False
    ) -> pd.DataFrame:
        """
        Execute a single SQL query.

        Args:
            query: SQL query string to execute
            params: Optional parameters for query substitution
            output_file: Optional path to save query results
            output_format: Output format (csv/parquet/json)
            _silent: Whether to suppress logging
            _log_save: Whether to log save operations

        Returns:
            pandas DataFrame containing query results
        """
        start_time = time.time()
        try:
            # Process query parameters if provided
            if params:
                processed_query = query
                for key, value in params.items():
                    if f"{{{key}}}" in processed_query:
                        processed_query = processed_query.replace(f"{{{key}}}", str(value))
                query = processed_query

            # Execute query based on database type
            if isinstance(self.db_connector, OracleConnector):
                df = await self._execute_oracle_query(query, {})
            else:
                async with self.db_connector.get_connection() as conn:
                    result = await conn.fetch(query)
                    if result:
                        df = pd.DataFrame([dict(row) for row in result])
                    else:
                        df = pd.DataFrame()

            # Save results if output file specified
            if output_file:
                output_path = Path(output_file)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                await self.save_output_chunked(df, output_path, output_format)

            # Log execution details if not silent
            execution_time = time.time() - start_time
            if not _silent:
                query_preview = query[:200] + "..." if len(query) > 200 else query
                self.logger.info(
                    f"Query executed in {execution_time:.2f} seconds:\n"
                    f"Query: {query_preview}\n"
                    f"Rows returned: {len(df)}"
                )

            return df
            
        except Exception as e:
            execution_time = time.time() - start_time
            if not _silent:
                self.logger.error(f"Query failed: {str(e)}")
            raise

    async def _execute_oracle_query(self, query: str, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Execute Oracle query in thread pool.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            pandas DataFrame containing query results
        """
        def _execute():
            with self.db_connector.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                if cursor.description:  # For SELECT queries
                    columns = [desc[0] for desc in cursor.description]
                    results = cursor.fetchall()
                    return pd.DataFrame(results, columns=columns)
                return pd.DataFrame()  # For non-SELECT queries

        return await asyncio.get_running_loop().run_in_executor(
            self.thread_pool, _execute
        )

    async def execute_queries_from_file(
        self, 
        file_path: Union[str, Path], 
        params: Optional[Dict[str, Any]] = None,
        output_file: Optional[Union[str, Path]] = None, 
        output_format: str = "csv"
    ) -> List[pd.DataFrame]:
        """
        Execute multiple queries from a file in parallel.
        Skip execution if output file already exists.

        Args:
            file_path: Path to SQL file containing queries
            params: Optional parameters for query substitution
            output_file: Optional path to save query results
            output_format: Output format (csv/parquet/json)

        Returns:
            List of pandas DataFrames containing query results
        """
        file_path = Path(file_path)
        if not file_path.exists():
            self.logger.error(f"Query file not found: {file_path}")
            raise FileNotFoundError(f"Query file not found: {file_path}")
        
        # Skip if output file exists
        if output_file and Path(output_file).exists():
            self.logger.info(f"Output file {output_file} already exists, skipping execution")
            return [pd.DataFrame()]
        
        try:
            self.logger.info(f"Starting execution of query file: {file_path}")
            
            # Split file into individual queries
            queries = file_path.read_text().strip().split(";")
            queries = [q.strip() for q in queries if q.strip()]

            if not queries:
                self.logger.warning(f"No valid queries found in file: {file_path}")
                return []

            # Prepare tasks for parallel execution
            tasks = []
            found_first_select = False
            
            for i, query in enumerate(queries, 1):
                is_select = any(line.lstrip().upper().startswith('SELECT') 
                              for line in query.split('\n') 
                              if line.strip() and not line.strip().startswith('--'))
                
                should_output = is_select and not found_first_select
                if is_select:
                    found_first_select = True

                task = self.execute_query(
                    query, 
                    params=params,
                    output_file=output_file if should_output else None,
                    output_format=output_format,
                    _silent=True,
                    _log_save=False
                )
                tasks.append(task)
            
            # Execute all queries in parallel
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Check for errors
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self.logger.error(f"Query {i+1} failed: {str(result)}")
                    raise result
            
            return results
                
        except Exception as e:
            self.logger.error(f"Failed to process SQL file {file_path}: {str(e)}")
            raise

    async def save_output_chunked(
        self, 
        df: pd.DataFrame, 
        output_file: Path, 
        output_format: str, 
        chunksize: int = 1000
    ) -> None:
        """
        Save query results to file in chunks.
        
        Args:
            df: DataFrame containing query results
            output_file: Path to save the output file
            output_format: Format to save (csv/parquet/json)
            chunksize: Number of rows per chunk for CSV output
        """
        try:
            # Define supported output formats and their save functions
            supported_formats = {
                "csv": lambda: df.to_csv(output_file, index=False, header=True, chunksize=chunksize),
                "parquet": lambda: df.to_parquet(output_file, index=False),
                "json": lambda: df.to_json(output_file, orient="records", lines=True)
            }
            
            if output_format not in supported_formats:
                raise ValueError(f"Unsupported output format: {output_format}")
                
            # Execute save operation in thread pool
            await asyncio.get_running_loop().run_in_executor(
                None, supported_formats[output_format]
            )
            self.logger.info(f"Results saved to {output_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save results to {output_file}: {str(e)}")
            raise

    async def __aenter__(self):
        """Async context manager entry"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Async context manager exit.
        Ensures proper cleanup of resources.
        """
        if self.thread_pool:
            self.thread_pool.shutdown(wait=True)
        if isinstance(self.db_connector, PostgresConnector) and self.db_connector.pool:
            await self.db_connector.pool.close()
