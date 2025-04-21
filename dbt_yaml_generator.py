#!/usr/bin/env python3

"""
Main entry point for the dbt YAML generator for Snowflake.
"""

import os
import sys
import argparse
import logging
from typing import Dict, List, Any

# Import local modules
from connectors.snowflake import SnowflakeConnector
from generators.yaml_generator import DbtYamlGenerator
from utils.config_loader import (
    load_env_file, 
    get_snowflake_config, 
    load_tests_config, 
    get_yaml_output_path
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for the script"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='dbt YAML Generator for Snowflake')
    parser.add_argument('--env-file', default='.env', help='Path to environment variables file')
    parser.add_argument('--tests-config', default='tests_config.yaml', help='Path to tests configuration file')
    parser.add_argument('--output', help='Output path for the dbt YAML file (overrides env var)')
    parser.add_argument('--schema', help='Schema to use (overrides env var)')
    args = parser.parse_args()
    
    try:
        # Load environment variables from .env file
        load_env_file(args.env_file)
        
        # Get Snowflake configuration from environment variables
        snowflake_config = get_snowflake_config()
        
        # Override schema if specified in command line
        if args.schema:
            snowflake_config['schema'] = args.schema
            
        # Get output path
        output_path = args.output if args.output else get_yaml_output_path()
        
        # Load tests configuration
        tests_config = load_tests_config(args.tests_config)
        
        # Create Snowflake connector and connect
        connector = SnowflakeConnector(snowflake_config)
        connector.connect()
        
        try:
            # Get schema and tables
            schema = snowflake_config['schema']
            tables = connector.get_tables(schema)
            
            if not tables:
                logger.warning(f"No tables found in schema: {schema}")
                return 0
            
            logger.info(f"Found {len(tables)} tables in schema {schema}")
            
            # Create YAML generator
            yaml_generator = DbtYamlGenerator(connector, tests_config)
            
            # Generate YAML structure
            yaml_structure = yaml_generator.generate_model_yaml(schema, tables)
            
            # Write YAML file
            success = yaml_generator.write_yaml_file(yaml_structure, output_path)
            
            if success:
                logger.info(f"Successfully generated dbt YAML for {len(tables)} tables in {schema}")
                return 0
            else:
                logger.error("Failed to write YAML file")
                return 1
            
        finally:
            # Close the connection
            connector.close()
    
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())