# DBT YAML Generator for Snowflake

This tool connects to Snowflake and automatically generates `schema.yml` files for dbt, including intelligent table and column descriptions using spaCy natural language processing, along with configurable tests based on your requirements.

## Features

- **Multiple Authentication Methods**: Support for password, browser-based, and Okta SSO authentication
- **Intelligent Descriptions using spaCy**: Analyzes column data and names with spaCy NLP to generate meaningful descriptions
- **Configurable Tests**: Apply dbt tests based on a configuration file
- **Flexible Output**: Generate YAML files in your preferred location in the dbt project structure

## How It Works

1. **Connects to Snowflake** using your preferred authentication method
2. **Retrieves table and column metadata** from your specified schema
3. **Analyzes data patterns** by sampling up to 100 non-null rows for each column
4. **Generates descriptions** using spaCy natural language processing:
   - Analyzes column names and converts them to readable format
   - Examines data patterns and relationships
   - Identifies special column types (IDs, dates, amounts, etc.)
   - Creates context-aware descriptions
5. **Applies tests** based on your configuration
6. **Creates properly formatted dbt YAML files** with the exact indentation and structure required

## Installation

1. Clone this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Download spaCy language model:
   ```bash
   python -m spacy download en_core_web_sm
   ```
4. Copy the example environment file and configure:
   ```bash
   cp .env.example .env
   ```

## Configuration

### Environment Variables

Configure your connection by editing the `.env` file:

```
# Snowflake Credentials
SNOWFLAKE_ACCOUNT=xyz12345.us-east-1
SNOWFLAKE_USER=my_user
SNOWFLAKE_PASSWORD=my_password

# Database Configuration
DATABASE=MY_DATABASE
SCHEMA=MY_SCHEMA
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ACCOUNTADMIN

# Authentication Method (options: password, browser, okta)
AUTH_METHOD=password

# For Okta SSO authentication
OKTA_URL=https://your-company.okta.com

# Output Configuration
DBT_YAML_OUTPUT_PATH=./models/staging/MY_SCHEMA/schema.yml
```

### Tests Configuration

Create a `tests_config.yaml` file to specify which columns should have tests and what tests to apply:

```yaml
tests:
  - column: id
    tests:
      - not_null
      - unique

  - column: email
    tests:
      - not_null
      - relationships:
          to: ref('users')
          field: email

  - column: created_at
    tests:
      - not_null
      - accepted_values:
          values: ['2023-01-01', '2023-12-31']
```

## Usage

Run the script to generate dbt YAML files:

```bash
python dbt_yaml_generator.py
```

### Command-Line Options

```
usage: main.py [-h] [--env-file ENV_FILE] [--tests-config TESTS_CONFIG] [--output OUTPUT] [--schema SCHEMA]

dbt YAML Generator for Snowflake

optional arguments:
  -h, --help            show this help message and exit
  --env-file ENV_FILE   Path to environment variables file
  --tests-config TESTS_CONFIG
                        Path to tests configuration file
  --output OUTPUT       Output path for the dbt YAML file (overrides env var)
  --schema SCHEMA       Schema to use (overrides env var)
```

## spaCy-Based Description Generation

This tool uses spaCy natural language processing to generate intelligent descriptions:

1. **For Columns**: Analyzes the column name, data type, and samples up to 100 non-null values to generate meaningful descriptions
2. **For Tables**: Examines column names, patterns, and relationships to infer the table's purpose

The description generation has multiple fallback mechanisms:
1. First tries Snowflake Cortex functions if available
2. Then checks for existing database comments
3. Finally uses spaCy-based analysis as the reliable fallback

### How spaCy Enhances Descriptions

- **Column Name Analysis**: Converts technical column names like `cust_id` to readable formats like "Customer identifier"
- **Data Pattern Recognition**: Identifies common patterns in the data using NLP
- **Entity Detection**: Uses spaCy's entity recognition for columns containing text
- **Special Column Detection**: Automatically recognizes common column types like IDs, dates, flags, etc.
- **Smart Table Context**: Infers table purpose based on the relationships between columns

## Example Output

The generated YAML file will look like this:

```yaml
version: 2

models:
  - name: users
    description:
      'Contains information about user accounts including identifiers, names,
      and status information'
    columns:
      - name: id
        description: 'Primary identifier for each user record'
        tests:
          - not_null
          - unique

      - name: email
        description: 'User email address used for authentication and communication'
        tests:
          - not_null
          - relationships:
              to: ref('users')
              field: email

      - name: created_at
        description: 'Date when the user account was created'
        tests:
          - not_null
```

## Requirements

- Python 3.7+
- snowflake-connector-python
- PyYAML
- python-dotenv
- spaCy with en_core_web_sm language model
