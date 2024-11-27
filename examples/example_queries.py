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
            postgres_max_queries=5,
            query_parameters={
                "dt_t1_yyyy-mm-dd": t1_date,    
                "dt_t2_yyyy-mm-dd": t2_date,    
                "dt_t1_yyyymmdd": t1_date_compact,  
                "dt_t2_yyyymmdd": t2_date_compact   
            }
        )

        # Execute queries with parameters
        print("\nExecuting all queries...")
        executor.execute_queries()
        print("\nAll queries completed successfully!")

    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
