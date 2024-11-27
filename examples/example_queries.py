from querymaster.query_executor import QueryExecutor
from pathlib import Path
from datetime import datetime, timedelta

def main():
    try:
        # Get configuration file paths
        config_path = Path(__file__).parent.parent / "connection.ini"
        query_config_path = Path(__file__).parent.parent / "configs/query_configs.csv"
        
        # Prepare query parameters
        today = datetime.now()
        # Format: YYYY-MM-DD
        t1_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')  # T-1 (Yesterday)
        t2_date = (today - timedelta(days=2)).strftime('%Y-%m-%d')  # T-2 (Day before yesterday)
        # Format: YYYYMMDD
        t1_date_compact = (today - timedelta(days=1)).strftime('%Y%m%d')  # T-1 (Yesterday)
        t2_date_compact = (today - timedelta(days=2)).strftime('%Y%m%d')  # T-2 (Day before yesterday)
        
        # Initialize QueryExecutor
        executor = QueryExecutor(
            connection_config=str(config_path),
            query_config=str(query_config_path),
            oracle_max_queries=3,
            postgres_max_queries=5
        )

        # Execute queries with parameters
        print("\nExecuting all queries...")
        executor.execute_queries(
            return_results=False,
            params={
                "dt_t1_yyyy-mm-dd": t1_date,    # T-1 date (YYYY-MM-DD)
                "dt_t2_yyyy-mm-dd": t2_date,    # T-2 date (YYYY-MM-DD)
                "dt_t1_yyyymmdd": t1_date_compact,  # T-1 date (YYYYMMDD)
                "dt_t2_yyyymmdd": t2_date_compact   # T-2 date (YYYYMMDD)
            }
        )
        print("\nAll queries completed successfully!")

    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
