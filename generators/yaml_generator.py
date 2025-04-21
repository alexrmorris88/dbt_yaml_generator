"""
Module for generating dbt YAML files from Snowflake metadata.
"""

import os
import logging
import textwrap
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class DbtYamlGenerator:
    """Generator for dbt YAML files based on Snowflake metadata"""
    
    def __init__(self, snowflake_connector, tests_config: Dict[str, Any]):
        """Initialize with Snowflake connector and tests configuration"""
        self.snowflake = snowflake_connector
        self.tests_config = tests_config
    
    def generate_model_yaml(self, schema: str, tables: List[str]) -> Dict[str, Any]:
        """Generate the full dbt YAML structure for all tables in a schema"""
        yaml_structure = {
            "version": 2,
            "models": []
        }
        
        for table in tables:
            # Get table metadata
            table_description = self.snowflake.get_table_description(schema, table)
            columns = self.snowflake.get_columns(schema, table)
            column_descriptions = self.snowflake.get_column_descriptions(schema, table)
            
            # Build table model structure
            table_model = {
                "name": table,
                "description": table_description or f"Data from {schema}.{table}",
                "columns": []
            }
            
            # Process each column
            for column_info in columns:
                column_name = column_info["name"]
                column_structure = {
                    "name": column_name,
                    "description": column_descriptions.get(column_name, f"Column {column_name} from {table}")
                }
                
                # Add tests if defined in the tests config
                tests = self._get_tests_for_column(column_name)
                if tests:
                    column_structure["tests"] = tests
                
                table_model["columns"].append(column_structure)
            
            yaml_structure["models"].append(table_model)
        
        return yaml_structure
    
    def _get_tests_for_column(self, column_name: str) -> List[Any]:
        """Get the list of tests for a column from the tests configuration"""
        tests = []
        
        if "tests" not in self.tests_config:
            return tests
        
        # Search for the column in the tests config
        for column_config in self.tests_config["tests"]:
            if column_config.get("column") == column_name:
                column_tests = column_config.get("tests", [])
                
                # Process each test definition
                for test in column_tests:
                    if isinstance(test, str):
                        # Simple test (e.g., "not_null")
                        tests.append(test)
                    elif isinstance(test, dict):
                        # Complex test with parameters (e.g., {"relationships": {"to": "ref('users')", "field": "id"}})
                        tests.append(test)
                
                # Break once we find the column (assuming column names are unique in the config)
                break
        
        return tests
    
    def write_yaml_file(self, yaml_structure: Dict[str, Any], output_path: str) -> bool:
        """Write the YAML structure to a file in the exact format specified"""
        try:
            # Create directory if it doesn't exist
            directory = os.path.dirname(output_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory)
            
            # Format the YAML manually to match the required specification
            with open(output_path, 'w') as f:
                # Write version
                f.write("version: 2\n\n")
                f.write("models:\n")
                
                # Process each model
                for model in yaml_structure["models"]:
                    f.write(f"  - name: {model['name']}\n")
                    
                    # Format description with proper indentation and single quotes
                    description = model['description'].replace("'", "''")  # Escape single quotes
                    wrapped_desc = textwrap.fill(description, width=70)
                    # Replace newlines with newline + indentation
                    wrapped_desc = wrapped_desc.replace("\n", "\n      ")
                    
                    # Write the description
                    f.write(f"    description:\n      '{wrapped_desc}'\n")
                    f.write("    columns:\n")
                    
                    # Process each column
                    for column in model["columns"]:
                        f.write(f"      - name: {column['name']}\n")
                        
                        # Format column description with proper indentation and single quotes
                        col_description = column['description'].replace("'", "''")  # Escape single quotes
                        wrapped_col_desc = textwrap.fill(col_description, width=65)
                        # Replace newlines with newline + indentation
                        wrapped_col_desc = wrapped_col_desc.replace("\n", "\n          ")
                        
                        # Write the column description
                        f.write(f"        description: '{wrapped_col_desc}'\n")
                        
                        # Process tests if any
                        if "tests" in column and column["tests"]:
                            f.write("        tests:\n")
                            for test in column["tests"]:
                                if isinstance(test, str):
                                    # Simple test
                                    f.write(f"          - {test}\n")
                                else:
                                    # Complex test with parameters
                                    for test_name, test_params in test.items():
                                        f.write(f"          - {test_name}:\n")
                                        for param_name, param_value in test_params.items():
                                            if isinstance(param_value, list):
                                                f.write(f"              {param_name}:\n")
                                                for item in param_value:
                                                    f.write(f"                - '{item}'\n")
                                            else:
                                                f.write(f"              {param_name}: {param_value}\n")
                    
                    # Add extra newline between models
                    f.write("\n")
            
            logger.info(f"Successfully wrote dbt YAML to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to write YAML file: {e}")
            return False