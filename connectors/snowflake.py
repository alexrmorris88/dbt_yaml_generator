"""
Snowflake connector implementation with multiple authentication methods.
"""

import os
import re
import logging
from typing import Dict, List, Optional, Any, Tuple
import snowflake.connector
from dotenv import load_dotenv

from utils.description_generator import DescriptionGenerator

logger = logging.getLogger(__name__)

class SnowflakeConnector:
    """Connector implementation for Snowflake with support for multiple authentication methods"""
    
    def __init__(self, config: Dict[str, str]):
        """Initialize with connection parameters"""
        # Process the configuration to replace environment variables
        self.config = self._process_env_variables(config)
        self.conn = None
        self.description_generator = DescriptionGenerator()
    
    def _process_env_variables(self, config: Dict[str, str]) -> Dict[str, str]:
        """Replace environment variable placeholders in config values"""
        processed_config = {}
        
        for key, value in config.items():
            if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                # Direct environment variable reference: ${ENV_VAR}
                env_var = value[2:-1]
                env_value = os.getenv(env_var)
                if env_value:
                    processed_config[key] = env_value
                    logger.info(f"Replaced environment variable {env_var} for config key '{key}'")
                else:
                    logger.warning(f"Environment variable {env_var} not found for key '{key}'. Using empty string.")
                    processed_config[key] = ''
            elif isinstance(value, str):
                # Look for ${ENV_VAR} pattern within strings
                matches = re.findall(r'\${([A-Za-z0-9_]+)}', value)
                if matches:
                    processed_value = value
                    for env_var in matches:
                        env_value = os.getenv(env_var)
                        if env_value:
                            processed_value = processed_value.replace(f'${{{env_var}}}', env_value)
                            logger.info(f"Replaced environment variable {env_var} in string for config key '{key}'")
                        else:
                            logger.warning(f"Environment variable {env_var} not found in string for key '{key}'. Using empty string.")
                            processed_value = processed_value.replace(f'${{{env_var}}}', '')
                    processed_config[key] = processed_value
                else:
                    processed_config[key] = value
            elif isinstance(value, dict):
                # Recursively process nested dictionaries
                processed_config[key] = self._process_env_variables(value)
            else:
                processed_config[key] = value
        
        return processed_config
    
    def connect(self) -> Any:
        """Connect to Snowflake database with support for different authentication methods"""
        # Extract authentication config
        auth_config = self.config.get('authentication', {})
        auth_method = auth_config.get('method', 'password')
        
        # Build connection parameters based on authentication method
        conn_params = {
            'account': self.config.get('account'),
            'warehouse': self.config.get('warehouse'),
            'database': self.config.get('database'),
            'schema': self.config.get('schema'),
            'role': self.config.get('role', 'ACCOUNTADMIN')
        }
        
        # Add authentication parameters based on method
        if auth_method == 'password':
            # Traditional username/password authentication
            conn_params['user'] = self.config.get('user')
            conn_params['password'] = self.config.get('password')
            logger.info(f"Using password authentication for user: {conn_params['user']}")
        
        elif auth_method == 'browser' or auth_method == 'externalbrowser':
            # External browser authentication (opens a browser window)
            conn_params['user'] = self.config.get('user')
            conn_params['authenticator'] = 'externalbrowser'
            # Add an empty password - the Snowflake connector still expects this parameter
            # even with browser-based authentication
            conn_params['password'] = ''
            logger.info(f"Using external browser authentication for user: {conn_params['user']}")
        
        elif auth_method == 'okta':
            # Okta authentication
            conn_params['user'] = self.config.get('user')
            okta_url = auth_config.get('okta_url') or self.config.get('okta_url')
            if not okta_url:
                raise ValueError("Okta URL is required for Okta authentication")
            
            # For Okta authentication, let's try using externalbrowser instead
            # This often works better than directly passing the Okta URL
            conn_params['authenticator'] = 'externalbrowser'
            
            # Add an empty password - required for both authenticator types
            conn_params['password'] = ''
            
            logger.info(f"Using Okta (external browser) authentication for user: {conn_params['user']}")
        
        elif auth_method == 'token':
            # Token-based authentication
            token = auth_config.get('token') or self.config.get('token')
            if not token:
                raise ValueError("Token is required for token authentication")
            
            conn_params['token'] = token
            logger.info("Using token-based authentication")
        
        else:
            raise ValueError(f"Unsupported authentication method: {auth_method}")
        
        # Log connection parameters (excluding sensitive info)
        safe_params = {k: v for k, v in conn_params.items() if k not in ['password', 'token']}
        logger.info(f"Connecting to Snowflake with parameters: {safe_params}")
        
        # Connect to Snowflake
        self.conn = snowflake.connector.connect(**conn_params)
        logger.info(f"Connected to Snowflake database: {self.config.get('database')}")
        return self.conn
    
    def get_schemas(self) -> List[str]:
        """Get list of schemas in the database"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("SHOW SCHEMAS")
            schemas = [row[1] for row in cursor.fetchall()]
            return schemas
        finally:
            cursor.close()
    
    def get_tables(self, schema: str) -> List[str]:
        """Get list of tables in a schema"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SHOW TABLES IN SCHEMA {schema}")
            tables = [row[1] for row in cursor.fetchall()]
            return tables
        finally:
            cursor.close()
    
    def get_columns(self, schema: str, table: str) -> List[Dict[str, str]]:
        """Get column information for a table"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"DESCRIBE TABLE {schema}.{table}")
            columns = []
            for row in cursor.fetchall():
                column_info = {
                    'name': row[0],
                    'type': row[1],
                    'nullable': row[3]
                }
                columns.append(column_info)
            return columns
        finally:
            cursor.close()
    
    def get_sample_data(self, schema: str, table: str, column: str, sample_size: int = 100) -> List[Any]:
        """Get sample data from a column"""
        cursor = self.conn.cursor()
        try:
            # Get non-null values for better analysis
            cursor.execute(f"SELECT {column} FROM {schema}.{table} WHERE {column} IS NOT NULL SAMPLE ({sample_size} ROWS)")
            # Extract values from the single-column result
            sample_data = [row[0] for row in cursor.fetchall()]
            return sample_data
        except Exception as e:
            logger.warning(f"Error getting sample data for {schema}.{table}.{column}: {e}")
            return []
        finally:
            cursor.close()
    
    def get_table_description(self, schema: str, table: str) -> str:
        """Get AI-generated description for a table using sample data and spacy"""
        cursor = self.conn.cursor()
        try:
            # First try with Cortex AI function if available
            try:
                sql = f"SELECT AI_DESCRIBE_TABLE('{schema}.{table}')"
                cursor.execute(sql)
                result = cursor.fetchone()
                if result and result[0]:
                    return result[0]
            except Exception as e:
                logger.info(f"AI_DESCRIBE_TABLE function not available, using spacy-based description: {e}")
            
            # Try getting existing comment from information schema
            try:
                sql = f"SELECT COMMENT FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table}'"
                cursor.execute(sql)
                result = cursor.fetchone()
                if result and result[0]:
                    return result[0]
            except Exception as e:
                logger.info(f"Table comment not available: {e}")
            
            # If previous methods failed, generate description using spacy
            # Get all columns to analyze table structure
            columns = self.get_columns(schema, table)
            
            # Generate description using our NLP-based generator
            description = self.description_generator.generate_table_description(table, columns)
            
            return description
        finally:
            cursor.close()
    
    def get_column_descriptions(self, schema: str, table: str) -> Dict[str, str]:
        """Get descriptions for columns using sample data and spacy"""
        cursor = self.conn.cursor()
        try:
            columns = self.get_columns(schema, table)
            descriptions = {}
            
            # First try with Cortex AI function if available
            try:
                sql = f"SELECT AI_DESCRIBE_COLUMNS('{schema}.{table}')"
                cursor.execute(sql)
                result = cursor.fetchone()
                if result and result[0]:
                    # Parse the result into a dictionary of column name -> description
                    if isinstance(result[0], dict):
                        return result[0]
            except Exception as e:
                logger.info(f"AI_DESCRIBE_COLUMNS function not available, using spacy-based descriptions: {e}")
            
            # Try getting existing comments from information schema
            existing_comments = {}
            try:
                sql = f"""
                SELECT column_name, comment 
                FROM information_schema.columns 
                WHERE table_schema = '{schema}' 
                AND table_name = '{table}'
                """
                cursor.execute(sql)
                for row in cursor.fetchall():
                    if row[1]:  # If comment exists
                        existing_comments[row[0]] = row[1]
            except Exception as e:
                logger.info(f"Column comments not available: {e}")
            
            # Process each column
            for column_info in columns:
                column_name = column_info['name']
                data_type = column_info['type']
                
                # First use existing comment if available
                if column_name in existing_comments:
                    descriptions[column_name] = existing_comments[column_name]
                    continue
                
                # Otherwise, get sample data and generate description
                sample_data = self.get_sample_data(schema, table, column_name)
                
                # Generate description using our NLP-based generator
                description = self.description_generator.generate_column_description(
                    column_name, data_type, sample_data
                )
                
                descriptions[column_name] = description
            
            return descriptions
        finally:
            cursor.close()
    
    def close(self) -> None:
        """Close the Snowflake connection"""
        if self.conn:
            self.conn.close()
            logger.info("Closed Snowflake connection")
            self.conn = None