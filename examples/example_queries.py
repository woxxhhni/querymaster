from querymaster.query_executor import QueryExecutor
from pathlib import Path

def main():
    try:
        # Get configuration file paths
        config_path = Path(__file__).parent.parent / "connection.ini"
        query_config_path = Path(__file__).parent.parent / "configs/query_configs.csv"
        
        # Initialize QueryExecutor
        executor = QueryExecutor(
            connection_config=str(config_path),
            query_config=str(query_config_path),
            oracle_max_queries=3,
            postgres_max_queries=5
        )

        # Execute queries without returning results
        print("\nExecuting all queries...")
        executor.execute_queries(return_results=False)
        print("\nAll queries completed successfully!")

        # Or execute queries with results if needed
        # results = executor.execute_queries(return_results=True)
        # oracle_results = results["oracle"]
        # postgres_results = results["postgres"]

    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
