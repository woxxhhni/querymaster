from querymaster.config_manager import QueryConfigManager
from querymaster.core import QueryMaster
from pathlib import Path
import asyncio
from typing import Optional, List, Dict, Any, Union

class QueryExecutor:
    """
    Synchronous wrapper for QueryMaster to execute database queries without async syntax
    """
    def __init__(
        self,
        connection_config: str,
        query_config: str,
        oracle_max_queries: int = 3,
        postgres_max_queries: int = 5,
        query_parameters: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize QueryExecutor

        Args:
            connection_config: Path to database connection config file
            query_config: Path to query configuration file
            oracle_max_queries: Maximum concurrent Oracle queries
            postgres_max_queries: Maximum concurrent Postgres queries
            query_parameters: Optional dictionary of query parameters
        """
        self.config_path = Path(connection_config)
        self.query_config_path = Path(query_config)
        self.oracle_max_queries = oracle_max_queries
        self.postgres_max_queries = postgres_max_queries
        self.query_parameters = query_parameters
        
        # Create necessary directories
        #Path("output/oracle").mkdir(parents=True, exist_ok=True)
        #Path("output/postgres").mkdir(parents=True, exist_ok=True)
        #Path("logs").mkdir(exist_ok=True)
        #Path("configs").mkdir(exist_ok=True)

    def execute_queries(
        self,
        oracle_configs: Optional[List[Dict[str, Any]]] = None,
        postgres_configs: Optional[List[Dict[str, Any]]] = None,
        return_results: bool = True
    ) -> Optional[Dict[str, List[Any]]]:
        """
        Execute queries for both Oracle and PostgreSQL databases

        Args:
            oracle_configs: Optional list of Oracle query configurations
            postgres_configs: Optional list of PostgreSQL query configurations
            return_results: Whether to return query results (default: True)

        Returns:
            Dictionary containing results for both databases if return_results is True,
            None otherwise
        """
        return asyncio.run(
            self._execute_queries_async(
                oracle_configs, 
                postgres_configs, 
                return_results,
                self.query_parameters
            )
        )

    async def _execute_queries_async(
        self,
        oracle_configs: Optional[List[Dict[str, Any]]],
        postgres_configs: Optional[List[Dict[str, Any]]],
        return_results: bool,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, List[Any]]]:
        """
        Internal async method to execute queries

        Args:
            oracle_configs: Optional list of Oracle query configurations
            postgres_configs: Optional list of PostgreSQL query configurations
            return_results: Whether to return query results
            parameters: Optional dictionary of query parameters

        Returns:
            Dictionary containing results for both databases if return_results is True,
            None otherwise
        """
        try:
            # Initialize configuration manager if configs not provided
            if oracle_configs is None or postgres_configs is None:
                config_manager = QueryConfigManager(config_file=self.query_config_path)
                oracle_configs = oracle_configs or config_manager.get_database_configs("ORACLE")
                postgres_configs = postgres_configs or config_manager.get_database_configs("cmrods_prd")

            # Initialize QueryMaster instances
            oracle_master = QueryMaster(
                section="ORACLE",
                config_file=self.config_path,
                log_file="logs/oracle_queries.log",
                max_concurrent_queries=self.oracle_max_queries
            )
            
            postgres_master = QueryMaster(
                section="cmrods_prd",
                config_file=self.config_path,
                log_file="logs/postgres_queries.log",
                max_concurrent_queries=self.postgres_max_queries
            )

            # Execute queries
            async with oracle_master, postgres_master:
                oracle_results, postgres_results = await asyncio.gather(
                    oracle_master.execute_multiple_files(oracle_configs, parameters=parameters),
                    postgres_master.execute_multiple_files(postgres_configs, parameters=parameters)
                )

            if return_results:
                return {
                    "oracle": oracle_results,
                    "postgres": postgres_results
                }
            return None

        except Exception as e:
            print(f"Error executing queries: {str(e)}")
            raise
