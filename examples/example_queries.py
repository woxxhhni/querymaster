from querymaster.config_manager import QueryConfigManager
from querymaster.core import QueryMaster
from pathlib import Path
import asyncio

async def main():
    try:
        config_path = Path(__file__).parent.parent / "connection.ini"
        query_config_path = Path(__file__).parent.parent / "configs/query_configs.csv"
        
        # Initialize configuration manager with specific config file location
        config_manager = QueryConfigManager(config_file=query_config_path)
        
        # Initialize QueryMaster instances
        oracle_master = QueryMaster(
            section="ORACLE",
            config_file=config_path,
            log_file="logs/oracle_queries.log",
            max_concurrent_queries=3
        )
        
        postgres_master = QueryMaster(
            section="cmrods_prd",
            config_file=config_path,
            log_file="logs/postgres_queries.log",
            max_concurrent_queries=5
        )

        # Get configurations
        oracle_configs = config_manager.get_database_configs("ORACLE")
        postgres_configs = config_manager.get_database_configs("cmrods_prd")

        # Execute queries in parallel
        print("\nExecuting all queries...")
        async with oracle_master, postgres_master:
            await asyncio.gather(
                oracle_master.execute_multiple_files(oracle_configs),
                postgres_master.execute_multiple_files(postgres_configs)
            )
        
        print("\nAll queries completed successfully!")

    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        raise

if __name__ == "__main__":
    # Create necessary directories
    Path("output/oracle").mkdir(parents=True, exist_ok=True)
    Path("output/postgres").mkdir(parents=True, exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    Path("configs").mkdir(exist_ok=True)
    
    # Run async main function
    asyncio.run(main())
