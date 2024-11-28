# QueryMaster

QueryMaster is a Python-based asynchronous SQL query execution tool that supports Oracle and Postgres databases. It simplifies query management, execution, and logging while enabling output in customizable formats (CSV, Parquet, JSON).

## Features

- **Database Support:** Oracle and PostgreSQL
- **Asynchronous Execution:** Parallel query execution with configurable concurrency
- **Parameter Management:**
  - Configuration file parameters
  - Runtime parameters (takes precedence)
  - Automatic parameter substitution
- **Output Management:**
  - CSV and Parquet formats
  - Automatic directory creation
  - Skip existing files
- **Error Handling:**
  - Independent file execution
  - Detailed error logging
  - Connection timeout control
- **Resource Management:**
  - Connection pooling
  - Automatic cleanup
  - Configurable retry mechanism

## Usage Example

```python
from querymaster.query_executor import QueryExecutor
from pathlib import Path
from datetime import datetime, timedelta

def main():
    # Initialize executor
    executor = QueryExecutor(
        connection_config="config/connection.ini",
        query_config="config/query_configs.csv",
        oracle_max_queries=3,
        postgres_max_queries=5
    )

    # Execute queries
    executor.execute_queries()

if __name__ == "__main__":
    main()
```

## Configuration

### Query Configuration (query_configs.csv)
```csv
database,file_path,params,output_file
ORACLE,queries/query1.sql,{"dt_t1":"2024-01-18"},output/result1.csv
POSTGRES,queries/query2.sql,{"dt_t1":"2024-01-18"},output/result2.parquet
```

### Parameter Precedence
1. Runtime parameters (if provided)
2. Configuration file parameters (as fallback)

### Output Formats
- CSV: Default format
- Parquet: Efficient columnar storage
- Automatic format detection from file extension

## Error Handling
- Independent file execution
- Empty DataFrame for failed queries
- Detailed error logging
- Automatic retry mechanism

## Dependencies
- Python 3.7+
- pandas
- asyncpg
- oracledb
- pyarrow (for Parquet support)

## License
MIT License
