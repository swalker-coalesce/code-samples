# Coalesce Node Metadata Loader for Snowflake

This repository contains tools for loading Coalesce node metadata into Snowflake, primarily featuring a robust stored procedure for automated data loading.

## Overview

The `load_coalesce_nodes` stored procedure provides a secure and efficient way to fetch node metadata from the Coalesce API and load it into Snowflake tables. It's designed to be reusable across different workspaces and includes comprehensive error handling and logging.

## Features

- 🔒 Secure API token handling using Snowflake secrets
- 🔄 Automatic table creation and schema management
- 📝 Detailed execution logging
- ⚠️ Comprehensive error handling
- 🔍 Parameter validation
- 📊 Support for variant (JSON) data storage
- 🚀 Optimized for large node sets

## Prerequisites

Before using the stored procedure, ensure you have:

1. A Snowflake account with appropriate permissions
2. A valid Coalesce API token
3. A configured Snowflake security integration for external API access
4. Necessary grants on the target database and schema

## Table Schema

The procedure creates (if not exists) or uses a table with the following schema:

```sql
CREATE TABLE IF NOT EXISTS <database>.<schema>.<table> (
    workspace_id STRING,        -- Coalesce workspace identifier
    node_id STRING,             -- Unique identifier for the node
    datetime_added TIMESTAMP_NTZ, -- When the record was loaded
    node_data VARIANT         -- Complete node metadata as JSON
)
```

## Usage

### Setting the Context

Before calling the stored procedure, it's recommended to set your database and schema context using `USE` statements. This helps avoid any ambiguity and makes the procedure call cleaner:

```sql
-- Set the working database and schema
USE DATABASE YOUR_DATABASE;
USE SCHEMA YOUR_SCHEMA;

-- Now you can call the procedure with the context already set
CALL LOAD_COALESCE_NODES(
    15,                                  -- WORKSPACE_ID
    CURRENT_DATABASE(),                  -- TARGET_DATABASE
    CURRENT_SCHEMA(),                    -- TARGET_SCHEMA
    'NODES',                            -- TARGET_TABLE
    'https://app.australia-southeast1.gcp.coalescesoftware.io'   -- COALESCE_BASE_URL
);
```

### Basic Example

You can also call the procedure with fully qualified parameters:

```sql
CALL SWALKER_DB_DEV.DEMO_DEV.LOAD_COALESCE_NODES(
    15,                                  -- WORKSPACE_ID
    'YOUR_DATABASE',                     -- TARGET_DATABASE
    'YOUR_SCHEMA',                       -- TARGET_SCHEMA
    'NODES',                            -- TARGET_TABLE
    'https://app.australia-southeast1.gcp.coalescesoftware.io'   -- COALESCE_BASE_URL
);
```

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| WORKSPACE_ID | NUMBER | Your Coalesce workspace identifier |
| TARGET_DATABASE | STRING | Database where the table will be created/updated |
| TARGET_SCHEMA | STRING | Schema within the database |
| TARGET_TABLE | STRING | Name of the table to store the data |
| COALESCE_BASE_URL | STRING | Base URL for the Coalesce API |

## Required Setup

Run these statements in order to configure the necessary components. Note that some steps require ACCOUNTADMIN privileges.

### 1. Create the API Token Secret
This secret will store your Coalesce API token securely in Snowflake:

```sql
CREATE SECRET your_secret_name
  TYPE = GENERIC_STRING
  SECRET_STRING = 'your-coalesce-api-token';
```

### 2. Create the Network Rule
Define the allowed network endpoints for Coalesce API access:

```sql
CREATE OR REPLACE NETWORK RULE coalesce_api_rule
  ALLOWED_NETWORK_RULES = ('https://app.australia-southeast1.gcp.coalescesoftware.io')
  AS 'Coalesce API network rule';
```

### 3. Create the API Integration
Set up the API integration for Coalesce access:

```sql
CREATE OR REPLACE API INTEGRATION coalesce_api_integration
  TYPE = API_INTEGRATION
  ENABLED = TRUE
  API_ALLOWED_PREFIXES = ('https://app.australia-southeast1.gcp.coalescesoftware.io');
```

### 4. Create the External Access Integration
This global integration binds the network rules and secrets (requires ACCOUNTADMIN):

```sql
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION coalesce_access_integration
  ALLOWED_NETWORK_RULES = (YOUR_DATABASE.YOUR_SCHEMA.coalesce_api_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (YOUR_DATABASE.YOUR_SCHEMA.your_secret_name)
  ENABLED = TRUE;
```

### 5. Grant Usage Permissions
Allow your role to use the integrations:

```sql
GRANT USAGE ON INTEGRATION coalesce_api_integration TO ROLE your_role;
GRANT USAGE ON INTEGRATION coalesce_access_integration TO ROLE your_role;
```

### 6. Verify the Setup
Confirm the integrations are properly configured:

```sql
SHOW API INTEGRATIONS;
SHOW EXTERNAL ACCESS INTEGRATIONS;
```

### 7. Create the Stored Procedure
Deploy the Python stored procedure that will fetch and load the Coalesce metadata:

```sql
CREATE OR REPLACE PROCEDURE LOAD_COALESCE_NODES(
    WORKSPACE_ID NUMBER,
    TARGET_DATABASE STRING,
    TARGET_SCHEMA STRING,
    TARGET_TABLE STRING,
    COALESCE_BASE_URL STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
external_access_integrations = (coalesce_access_integration)
PACKAGES = ('requests', 'snowflake-snowpark-python')
HANDLER = 'run_load'
SECRETS = ('token' = YOUR_DATABASE.YOUR_SCHEMA.your_secret_name)
AS
$$
import requests
import json
from datetime import datetime
from snowflake.snowpark.types import StructType, StructField, StringType, VariantType, TimestampType
from typing import Dict, Any, Optional
import _snowflake


class CoalesceNodeLoader:
    def __init__(self, snowpark_session, workspace_id: int, database: str, schema: str, 
                 table: str, base_url: str):
        self.session = snowpark_session
        self.workspace_id = workspace_id
        self.database = database
        self.schema = schema
        self.table = table
        self.base_url = base_url
        
        token = _snowflake.get_generic_secret_string('token')
        self.headers = {
            "accept": "application/json",
            "authorization": "Bearer " + token
        }
        self.log_message = ""

    @property
    def full_table_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"

    def append_log(self, message: str) -> None:
        """Add a message to the log with a newline."""
        self.log_message += f"{message}\n"

    def validate_parameters(self) -> str:
        """Validate all input parameters and return error message if any."""
        errors = []
        
        if not isinstance(self.workspace_id, (int, float)) or self.workspace_id <= 0:
            errors.append(f"WORKSPACE_ID must be a positive number, got: {self.workspace_id}")
        
        if not isinstance(self.database, str) or not self.database.strip():
            errors.append(f"TARGET_DATABASE must be a non-empty string, got: {self.database}")
        if not isinstance(self.schema, str) or not self.schema.strip():
            errors.append(f"TARGET_SCHEMA must be a non-empty string, got: {self.schema}")
        if not isinstance(self.table, str) or not self.table.strip():
            errors.append(f"TARGET_TABLE must be a non-empty string, got: {self.table}")
            
        if not isinstance(self.base_url, str) or not self.base_url.strip():
            errors.append(f"COALESCE_BASE_URL must be a non-empty string, got: {self.base_url}")
        elif not self.base_url.startswith(('http://', 'https://')):
            errors.append(f"COALESCE_BASE_URL must start with http:// or https://, got: {self.base_url}")
        elif 'coalescesoftware' not in self.base_url.lower():
            errors.append(f"COALESCE_BASE_URL must contain 'coalescesoftware', got: {self.base_url}")
        
        return "\n".join(errors) if errors else ""

    def ensure_table_exists(self) -> None:
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.full_table_name} (
            workspace_id STRING,
            node_id STRING,
            datetime_added TIMESTAMP_NTZ,
            node_data VARIANT
        )
        """
        self.session.sql(create_table_sql).collect()

    def get_node_list(self) -> list[str]:
        url = f"{self.base_url}/api/v1/workspaces/{self.workspace_id}/nodes"
        response = requests.request("GET", url, headers=self.headers, data={})
        
        if response.status_code != 200:
            raise Exception(f"Failed to fetch node list. Status code: {response.status_code}, Response: {response.text}")
        
        response_data = json.loads(response.text)
        return [node['id'] for node in response_data['data']]

    def get_node_metadata(self, node_id: str) -> Optional[Dict]:
        url = f"{self.base_url}/api/v1/workspaces/{self.workspace_id}/nodes/{node_id}"
        response = requests.request("GET", url, headers=self.headers, data={})
        
        if response.status_code != 200:
            self.append_log(f"Warning: Failed to fetch metadata for node {node_id}")
            return None
            
        return json.loads(response.text)

    def load_data_to_snowflake(self, nodes_data: list[Dict]) -> int:
        if not nodes_data:
            return 0
            
        schema = StructType([
            StructField("workspace_id", StringType()),
            StructField("node_id", StringType()),
            StructField("datetime_added", TimestampType()),
            StructField("node_data", VariantType())
        ])
        
        df = self.session.create_dataframe(nodes_data, schema)
        df.write.mode("append").save_as_table(self.full_table_name)
        return len(nodes_data)

    def execute(self) -> str:
        """Execute the node metadata loading process and return log messages."""
        self.log_message = ""  # Reset log
        self.append_log(f"Starting procedure for workspace {self.workspace_id}...")
        
        try:
            # Validate parameters
            validation_errors = self.validate_parameters()
            if validation_errors:
                self.append_log("Parameter validation failed:")
                self.append_log(validation_errors)
                return self.log_message

            # Create table if needed
            self.append_log(f"Ensuring table {self.full_table_name} exists...")
            self.ensure_table_exists()

            # Get node list
            self.append_log("Fetching node list from Coalesce API...")
            node_ids = self.get_node_list()
            self.append_log(f"Found {len(node_ids)} nodes to process")

            # Process nodes
            current_time = datetime.now()
            nodes_data = []
            
            for node_id in node_ids:
                node_data = self.get_node_metadata(node_id)
                if node_data is not None:
                    nodes_data.append({
                        'workspace_id': str(self.workspace_id),
                        'node_id': node_id,
                        'datetime_added': current_time,
                        'node_data': node_data
                    })
                    self.append_log(f"Processed node {node_id}")

            if not nodes_data:
                self.append_log("No valid node data to load")
                return self.log_message

            # Load data
            records_loaded = self.load_data_to_snowflake(nodes_data)
            self.append_log(f"Successfully saved {records_loaded} records to {self.full_table_name}")

        except Exception as e:
            self.append_log(f"Error: {str(e)}")
            import traceback
            self.append_log(f"Stack trace: {traceback.format_exc()}")
            
        return self.log_message


def run_load(snowpark_session, WORKSPACE_ID, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, COALESCE_BASE_URL):
    """Entry point for the stored procedure"""
    loader = CoalesceNodeLoader(
        snowpark_session, WORKSPACE_ID, TARGET_DATABASE, TARGET_SCHEMA, 
        TARGET_TABLE, COALESCE_BASE_URL
    )
    return loader.execute()
$$;

-- 8. Grant usage and verify references
-- Verify the stored procedure references the correct integrations and secret:
CREATE OR REPLACE PROCEDURE LOAD_COALESCE_NODES(...)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.12'
  external_access_integrations = (coalesce_access_integration)
  PACKAGES = ('requests', 'snowflake-snowpark-python')
  HANDLER = 'run_load'
  SECRETS = ('token' = YOUR_DATABASE.YOUR_SCHEMA.your_secret_name)
  ...

-- Grant usage on the procedure
GRANT USAGE ON PROCEDURE LOAD_COALESCE_NODES(NUMBER, STRING, STRING, STRING, STRING) TO ROLE your_role;

-- 9. Call the stored procedure
-- First set the context
USE DATABASE YOUR_DATABASE;
USE SCHEMA YOUR_SCHEMA;

-- Then call the procedure
CALL LOAD_COALESCE_NODES(
    15,                                                           -- WORKSPACE_ID
    CURRENT_DATABASE(),                                          -- TARGET_DATABASE
    CURRENT_SCHEMA(),                                           -- TARGET_SCHEMA
    'NODES',                                                    -- TARGET_TABLE
    'https://app.australia-southeast1.gcp.coalescesoftware.io'  -- COALESCE_BASE_URL
);

-- Verify the data was loaded
SELECT COUNT(*) FROM NODES;
SELECT * FROM NODES LIMIT 5;

## Querying the Data

After loading the data, you can query it using standard SQL. Here are some example queries:

### Get All Nodes
```sql
SELECT * FROM your_table;
```

### Get Specific Node Types
```sql
SELECT 
    node_id,
    node_data:node_type::STRING as node_type,
    node_data:name::STRING as node_name
FROM your_table
WHERE node_data:node_type::STRING = 'YOUR_NODE_TYPE';
```

## Using the Flattened Node View

For easier querying of node metadata, a view called `FLATTENED_NODE_VIEW` is provided. This view flattens the JSON structure into columns for simpler querying and better performance.

### Setting Up the View Context

The view uses the same database and schema context as your node data:

```sql
-- Set the context to match your node data location
USE DATABASE YOUR_DATABASE;
USE SCHEMA YOUR_SCHEMA;

-- Now you can query the view directly
SELECT * FROM FLATTENED_NODE_VIEW;
```

### Available Columns

The view provides these pre-flattened columns:

#### Node Identification
- `node_id`: Unique identifier for the node
- `node_name`: Name of the node
- `node_type`: Type of node (e.g., TABLE, VIEW)
- `description`: Node description

#### Location Information
- `location_name`: Connection/location name
- `database_name`: Database name
- `schema_name`: Schema name
- `table_name`: Table name

#### Configuration
- `materialization_type`: How the node is materialized
- `is_multisource`: Whether node has multiple sources
- `incremental_key`: Key used for incremental loading
- `partition_by`: Partitioning columns
- `cluster_by`: Clustering columns

#### Join Information
- `join_type`: Type of join (for join nodes)
- `join_condition`: Join conditions

#### SQL Information
- `sql_query`: SQL query (for SQL nodes)
- `dependencies`: Array of dependent node IDs

#### Raw JSON Access
- `config_json`: Complete configuration JSON
- `metadata_json`: Complete metadata JSON
- `node_data`: Complete node data JSON

### Example Queries

```sql
-- Find all table nodes
SELECT node_name, database_name, schema_name, table_name
FROM FLATTENED_NODE_VIEW
WHERE node_type = 'TABLE';

-- Find nodes with specific materialization
SELECT node_name, materialization_type
FROM FLATTENED_NODE_VIEW
WHERE materialization_type = 'VIEW';

-- Analyze join conditions
SELECT node_name, join_type, join_condition
FROM FLATTENED_NODE_VIEW
WHERE join_type IS NOT NULL;

-- Find dependencies
SELECT node_name, dependencies
FROM FLATTENED_NODE_VIEW
WHERE ARRAY_SIZE(dependencies) > 0;

-- Complex filtering using JSON
SELECT node_name, config_json
FROM FLATTENED_NODE_VIEW
WHERE config_json:someProperty::string = 'someValue';
```

### Notes
- The view automatically excludes system and temporary nodes
- NULL values are expected for fields that don't apply to certain node types
- The original JSON is preserved in the `node_data` column for advanced querying

## Error Handling

The procedure includes comprehensive error handling for:
- Invalid parameters
- API failures
- Network issues
- Permission problems
- Data validation errors

All errors are logged and returned in the procedure output.

## Best Practices

1. **Regular Updates**: Schedule regular runs to keep metadata current
2. **Monitoring**: Review execution logs for any failures
3. **Data Retention**: Implement a retention policy for historical data
4. **Performance**: Index frequently queried fields from the VARIANT column
5. **Security**: Regularly rotate API tokens and review security integration settings
6. **Context Management**: Always set your database and schema context using `USE` statements before running the procedure

## Troubleshooting

### Common Issues and Solutions

1. **API Token Invalid**
   - Verify your API token is current
   - Check the secret is properly configured

2. **Security Integration Error**
   - Verify the integration name
   - Check URL prefixes are correctly configured
   - Ensure proper grants are in place

3. **Permission Denied**
   - Verify user has proper roles and grants
   - Check database/schema permissions

4. **Network Issues**
   - Verify security integration network rules
   - Check Coalesce API endpoint accessibility

## Contributing

Feel free to submit issues and enhancement requests!

## License

This project is licensed under the MIT License - see the LICENSE file for details.
