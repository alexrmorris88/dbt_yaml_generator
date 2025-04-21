"""
Configuration and environment variables loading module.
"""

import os
import yaml
import json
import logging
from typing import Dict, Any, Optional
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

def load_env_file(env_file: str = ".env") -> bool:
    """Load environment variables from .env file"""
    if os.path.exists(env_file):
        load_dotenv(env_file, override=True)
        logger.info(f"Loaded environment variables from {env_file}")
        return True
    else:
        logger.warning(f"Environment file {env_file} not found")
        return False

def get_snowflake_config() -> Dict[str, Any]:
    """Get Snowflake connection configuration from environment variables"""
    # Load required connection parameters
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    database = os.getenv("DATABASE")
    schema = os.getenv("SCHEMA")
    auth_method = os.getenv("AUTH_METHOD", "password").lower()
    
    # Check required parameters
    if not all([account, user, database, schema]):
        missing = []
        if not account: missing.append("SNOWFLAKE_ACCOUNT")
        if not user: missing.append("SNOWFLAKE_USER")
        if not database: missing.append("DATABASE")
        if not schema: missing.append("SCHEMA")
        
        missing_vars = ", ".join(missing)
        logger.error(f"Missing required environment variables: {missing_vars}")
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    # Build basic config
    config = {
        "account": account,
        "user": user,
        "database": database,
        "schema": schema,
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "authentication": {"method": auth_method}
    }
    
    # Add method-specific configuration
    if auth_method == "password":
        password = os.getenv("SNOWFLAKE_PASSWORD")
        if not password:
            logger.error("SNOWFLAKE_PASSWORD is required for password authentication")
            raise ValueError("SNOWFLAKE_PASSWORD is required for password authentication")
        config["password"] = password
    
    elif auth_method == "okta":
        okta_url = os.getenv("OKTA_URL")
        if not okta_url:
            logger.error("OKTA_URL is required for Okta authentication")
            raise ValueError("OKTA_URL is required for Okta authentication")
        config["authentication"]["okta_url"] = okta_url
    
    elif auth_method == "token":
        token = os.getenv("SNOWFLAKE_TOKEN")
        if not token:
            logger.error("SNOWFLAKE_TOKEN is required for token authentication")
            raise ValueError("SNOWFLAKE_TOKEN is required for token authentication")
        config["authentication"]["token"] = token
    
    elif auth_method not in ["browser", "externalbrowser"]:
        logger.warning(f"Unrecognized authentication method: {auth_method}")
    
    return config

def load_tests_config(file_path: str) -> Dict[str, Any]:
    """Load test configuration from YAML or JSON file"""
    if not os.path.exists(file_path):
        logger.warning(f"Tests configuration file not found: {file_path}")
        return {"tests": []}
    
    try:
        with open(file_path, 'r') as f:
            if file_path.endswith('.yaml') or file_path.endswith('.yml'):
                config = yaml.safe_load(f)
            elif file_path.endswith('.json'):
                config = json.load(f)
            else:
                logger.error(f"Unsupported file format for tests configuration: {file_path}")
                raise ValueError(f"Unsupported file format for tests configuration: {file_path}")
        
        logger.info(f"Loaded tests configuration from {file_path}")
        return config
    except Exception as e:
        logger.error(f"Failed to load tests configuration: {e}")
        raise

def get_yaml_output_path() -> str:
    """Get the output path for dbt YAML files"""
    output_path = os.getenv("DBT_YAML_OUTPUT_PATH")
    if not output_path:
        logger.warning("DBT_YAML_OUTPUT_PATH not specified, using default './models/schema.yml'")
        output_path = "./models/schema.yml"
    
    # Create directories if they don't exist
    directory = os.path.dirname(output_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"Created directory for output: {directory}")
    
    return output_path