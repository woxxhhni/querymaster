# QueryMaster

QueryMaster is a Python-based asynchronous SQL query execution tool that supports Oracle and Postgres databases. It simplifies query management, execution, and logging while enabling output in customizable formats (CSV, Parquet, JSON).

## Features

- **Database Support:** Oracle and PostgreSQL
- **Asynchronous Execution:** Handles multiple queries in parallel
- **File-Based Queries:** Reads queries from SQL files
- **Parameter Support:** Supports query parameterization
- **Output Formats:** Saves results as CSV, Parquet, or JSON
- **Connection Pooling:** Efficient database connection management
- **Error Handling:** Comprehensive error tracking and logging
- **Progress Tracking:** Monitors query execution progress
- **Resource Management:** Automatic cleanup of connections and threads

## Installation

```bash
pip install -r requirements.txt
```

## Project Structure

```plaintext
querymaster/
├── querymaster/                 # Core package
│   ├── __init__.py             # Package initialization
│   ├── core.py                 # Main query execution module
│   ├── db_connectors.py        # Database connection handlers
│   ├── logger.py               # Logging utility
│   └── database_errors.py      # Custom error definitions
├── config/                     # Configuration files
│   ├── connection.ini          # Database connection settings
│   └── query_configs.csv       # Query configurations
├── queries/                    # SQL query files
│   ├── query1.sql             # Example query file
│   └── query2.sql             # Example query file
├── examples/                   # Example scripts
│   └── example_queries.py      # Usage examples
├── logs/                      # Log files directory
├── requirements.txt           # Dependencies
└── README.md                  # Documentation
```

## Usage Example

```python
from querymaster.core import QueryMaster
from pathlib import Path
import asyncio

async def main():
    # Initialize QueryMaster
    query_master = QueryMaster(
        section="ORACLE",
        config_file="config/connection.ini",
        log_file="logs/queries.log",
        max_concurrent_queries=3
    )

    # Execute queries from file
    async with query_master:
        results = await query_master.execute_queries_from_file(
            file_path="queries/query1.sql",
            params={"dt": "2024-01-01"},
            output_file="output/results.csv"
        )

if __name__ == "__main__":
    asyncio.run(main())
```

## Configuration

### Database Connection (connection.ini)
```ini
[ORACLE]
user=username
password=password
host=hostname
port=1521
sid=sid_name

[POSTGRES]
user=username
password=password
host=hostname
port=5432
dbname=database_name
```

### Query Configuration (query_configs.csv)
```csv
database,file_path,params,output_file
ORACLE,queries/query1.sql,{"dt": "2024-01-01"},output/result1.csv
POSTGRES,queries/query2.sql,{"status": "active"},output/result2.csv
```

## Features in Detail

### Asynchronous Execution
- Executes multiple queries concurrently
- Configurable concurrency limits
- Automatic resource management

### Output Formats
- CSV: Default format with chunked writing support
- Parquet: Efficient columnar storage format
- JSON: Line-delimited JSON format

### Error Handling
- Detailed error logging
- Connection error recovery
- Transaction management
- Resource cleanup

### Connection Management
- Connection pooling
- Automatic cleanup
- Thread pool for Oracle operations
- Async pool for PostgreSQL

## Dependencies

- Python 3.7+
- pandas
- asyncpg
- oracledb
- pyarrow (for Parquet support)

## License

This project is licensed under the MIT License - see the LICENSE file for details.
