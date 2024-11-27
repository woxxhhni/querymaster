import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import json


class QueryConfigManager:
    """
    Query configuration manager for managing and parsing query configuration files
    """
    
    def __init__(self, config_file: Union[str, Path] = "config/query_configs.csv"):
        """
        Initialize the query configuration manager
        
        Args:
            config_file: Path to CSV configuration file, can be string or Path object
                        defaults to config/query_configs.csv
        
        Raises:
            FileNotFoundError: When configuration file does not exist
        """
        self.config_file = Path(config_file)
        if not self.config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
        self.df = pd.read_csv(self.config_file)
        self._validate_columns()
    
    def _validate_columns(self) -> None:
        """
        Validate if CSV file contains all required columns
        
        Raises:
            ValueError: When required columns are missing
        """
        required_columns = {'database', 'file_path', 'params', 'output_file'}
        missing_columns = required_columns - set(self.df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns in config file: {missing_columns}")
    
    def get_database_configs(self, database: str) -> List[Dict[str, Any]]:
        """
        Get all query configurations for specified database
        
        Args:
            database: Database identifier
            
        Returns:
            List of dictionaries containing query configurations
        """
        df_filtered = self.df[self.df['database'] == database]
        configs = []
        for _, row in df_filtered.iterrows():
            config = {
                "query_file": row["file_path"],
                "output_file": row["output_file"],
                "params": eval(row["params"]) if pd.notna(row["params"]) else {}
            }
            configs.append(config)
        return configs
    
    def get_all_databases(self) -> List[str]:
        """
        Get list of all configured databases
        
        Returns:
            List of database names
        """
        return self.df['database'].unique().tolist()
    
    def add_config(self, 
                  database: str, 
                  file_path: Union[str, Path], 
                  params: Dict, 
                  output_file: Union[str, Path],
                  save: bool = True) -> None:
        """
        Add a new query configuration
        
        Args:
            database: Database identifier
            file_path: SQL file path, can be string or Path object
            params: Query parameters dictionary
            output_file: Output file path, can be string or Path object
            save: Whether to save to file immediately, defaults to True
        """
        new_row = {
            'database': database,
            'file_path': str(Path(file_path)),  # Convert to string for storage
            'params': json.dumps(params),
            'output_file': str(Path(output_file))  # Convert to string for storage
        }
        self.df = pd.concat([self.df, pd.DataFrame([new_row])], ignore_index=True)
        if save:
            self.save()
    
    def remove_config(self, 
                     database: str, 
                     file_path: Optional[Union[str, Path]] = None,
                     save: bool = True) -> None:
        """
        Remove query configuration(s)
        
        Args:
            database: Database identifier
            file_path: SQL file path (optional), can be string or Path object
                      If not specified, removes all configs for the database
            save: Whether to save to file immediately, defaults to True
        """
        if file_path:
            file_path_str = str(Path(file_path))
            self.df = self.df[~((self.df['database'] == database) & 
                               (self.df['file_path'] == file_path_str))]
        else:
            self.df = self.df[self.df['database'] != database]
        
        if save:
            self.save()
    
    def save(self) -> None:
        """
        Save configurations to CSV file
        """
        self.df.to_csv(self.config_file, index=False)
    
    def get_config_count(self) -> int:
        """
        Get total number of configurations
        
        Returns:
            Total number of configuration entries
        """
        return len(self.df)
    
    def get_databases_summary(self) -> Dict[str, int]:
        """
        Get configuration count summary for each database
        
        Returns:
            Dictionary mapping database names to their configuration counts
        """
        return self.df['database'].value_counts().to_dict()


if __name__ == "__main__":
    # Usage example
    try:
        config_manager = QueryConfigManager()
        
        # Get all databases
        databases = config_manager.get_all_databases()
        print(f"Configured databases: {databases}")
        
        # Get Oracle configurations
        oracle_configs = config_manager.get_database_configs("ORACLE")
        print(f"Number of Oracle configurations: {len(oracle_configs)}")
        
        # Add new configuration
        config_manager.add_config(
            database="ORACLE",
            file_path="queries/oracle/new_query.sql",
            params={"dt": "2024-01-01"},
            output_file="output/oracle/new_query.csv"
        )
        
        # Get statistics
        summary = config_manager.get_databases_summary()
        print(f"Database configuration summary: {summary}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
