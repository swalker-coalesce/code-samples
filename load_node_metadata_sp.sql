/*
Description: 
    This stored procedure fetches node metadata from Coalesce API and loads it into a Snowflake table.
    It handles both the initial node list retrieval and detailed metadata for each node.
    The target table will be automatically created if it doesn't exist.

IMPORTANT DEPLOYMENT NOTE:
    When deploying this procedure, replace the following placeholders with your actual values:
    - COALESCE_API_INTEGRATION: Your actual API integration name
    - COALESCE_API_TOKEN: Your fully qualified secret name (e.g., DATABASE.SCHEMA.SECRET_NAME)
    The angle brackets <> used in the examples below are for documentation only and should not be 
    used in the actual deployment.

Parameters:
    WORKSPACE_ID      - NUMBER   : The Coalesce workspace ID to fetch nodes from
    TARGET_DATABASE   - STRING   : Target Snowflake database for data loading
    TARGET_SCHEMA     - STRING   : Target schema within the database
    TARGET_TABLE      - STRING   : Target table name for the node metadata (defaults to COALESCE_NODE_METADATA)
    COALESCE_BASE_URL - STRING   : Base URL for the Coalesce API (e.g., 'https://app.coalescesoftware.io')
    SECURITY_INTEGRATION - STRING: Name of the Snowflake security integration for external access

Table Schema:
    The procedure will create (if not exists) or use an existing table with the following schema:
    - workspace_id     STRING        : Coalesce workspace identifier
    - node_id         STRING        : Unique identifier for the node
    - datetime_added  TIMESTAMP_NTZ : When the record was loaded
    - node_data      VARIANT       : Complete node metadata as JSON

Returns:
    STRING containing execution logs and error messages if any

Prerequisites:
    1. A configured security integration in Snowflake for external API access
    2. Appropriate permissions on the target database and schema
    3. Valid Coalesce API token

Usage Examples:

    1. Basic usage with default security integration:
    CALL <YOUR_DATABASE>.<YOUR_SCHEMA>.LOAD_COALESCE_NODES(
        15,                                  -- WORKSPACE_ID
        '<YOUR_DATABASE>',                   -- TARGET_DATABASE
        '<YOUR_SCHEMA>',                     -- TARGET_SCHEMA
        'COALESCE_NODE_METADATA',           -- TARGET_TABLE
        'https://app.coalescesoftware.io'   -- COALESCE_BASE_URL
    );

    2. Using current context:
    USE DATABASE <YOUR_DATABASE>;
    USE SCHEMA <YOUR_SCHEMA>;
    
    CALL LOAD_COALESCE_NODES(
        15,                                  -- WORKSPACE_ID
        CURRENT_DATABASE(),                  -- TARGET_DATABASE
        CURRENT_SCHEMA(),                    -- TARGET_SCHEMA
        'COALESCE_NODE_METADATA',           -- TARGET_TABLE
        'https://app.coalescesoftware.io'   -- COALESCE_BASE_URL
    );

Error Handling:
    - The procedure validates all input parameters before execution
    - API errors are caught and logged
    - Table creation errors are caught and logged
    - Full error stack traces are included in the output

Notes:
    - The procedure uses VARIANT type to store the complete node metadata
    - Existing data in the table is preserved (append-only)
    - Failed node fetches are logged but don't stop the procedure
    - The security integration must have appropriate network rules for Coalesce API access
*/

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
external_access_integrations=(COALESCE_API_INTEGRATION_SSW)
PACKAGES = ('requests', 'snowflake-snowpark-python')
HANDLER = 'run_load'
SECRETS = ('token' = SWALKER_DB_DEV.DEMO_DEV.COALESCE_API_TOKEN_SSW_US)  -- Note: Use actual fully qualified secret name when deploying
AS
$$
import requests
import json
from datetime import datetime
from snowflake.snowpark.types import StructType, StructField, StringType, VariantType, TimestampType
from typing import Dict, Any, Optional
import _snowflake


class CoalesceNodeLoader:
    """
    Handles loading Coalesce node metadata into Snowflake.
    
    This class manages the entire ETL process of fetching node metadata from 
    Coalesce API and loading it into a Snowflake table. It handles:
    - Parameter validation
    - Table creation/validation
    - API interactions
    - Data transformation
    - Snowflake data loading
    
    Attributes:
        session: Snowpark session for database operations
        workspace_id (int): Coalesce workspace identifier
        database (str): Target Snowflake database name
        schema (str): Target Snowflake schema name
        table (str): Target Snowflake table name
        base_url (str): Coalesce API base URL
        headers (dict): API request headers including authentication
        log_message (str): String to accumulate execution log messages
    """
    
    def __init__(self, snowpark_session, workspace_id: int, database: str, schema: str, 
                 table: str, base_url: str):
        """
        Initialize the CoalesceNodeLoader with connection and configuration parameters.
        
        Args:
            snowpark_session: Active Snowpark session
            workspace_id (int): Coalesce workspace ID
            database (str): Target Snowflake database name
            schema (str): Target Snowflake schema name
            table (str): Target Snowflake table name
            base_url (str): Coalesce API base URL
        """
        self.session = snowpark_session
        self.workspace_id = workspace_id
        self.database = database
        self.schema = schema
        self.table = table
        self.base_url = base_url
        
        # Get API token from Snowflake secret using get_generic_secret_string
        token = _snowflake.get_generic_secret_string('token')
        self.headers = {
            "accept": "application/json",
            "authorization": "Bearer " + token
        }
        self.log_message = ""

    @property
    def full_table_name(self) -> str:
        """
        Get the fully qualified Snowflake table name.
        
        Returns:
            str: Fully qualified table name in format 'DATABASE.SCHEMA.TABLE'
        
        Example:
            >>> loader.full_table_name
            'SWALKER_DB_DEV.DEMO_DEV.NODES'
        """
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
        """
        Create the target Snowflake table if it doesn't exist.
        
        Creates a table with the following schema:
        - workspace_id: STRING
        - node_id: STRING
        - datetime_added: TIMESTAMP_NTZ
        - node_data: VARIANT
        
        The table is created using 'CREATE TABLE IF NOT EXISTS' so it's safe
        to call multiple times.
        
        Raises:
            Exception: If table creation fails
        """
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
        """
        Fetch list of node IDs from Coalesce API.
        
        Makes a GET request to the Coalesce API to retrieve all nodes
        in the specified workspace.
        
        Returns:
            list[str]: List of node IDs from the workspace
        
        Raises:
            Exception: If API request fails or returns non-200 status code
        
        Example:
            >>> node_ids = loader.get_node_list()
            >>> print(f"Found {len(node_ids)} nodes")
        """
        url = f"{self.base_url}/api/v1/workspaces/{self.workspace_id}/nodes"
        response = requests.request("GET", url, headers=self.headers, data={})
        
        if response.status_code != 200:
            raise Exception(f"Failed to fetch node list. Status code: {response.status_code}, Response: {response.text}")
        
        response_data = json.loads(response.text)
        return [node['id'] for node in response_data['data']]

    def get_node_metadata(self, node_id: str) -> Optional[Dict]:
        """
        Fetch detailed metadata for a specific node.
        
        Makes a GET request to the Coalesce API to retrieve detailed
        metadata for a single node.
        
        Args:
            node_id (str): ID of the node to fetch metadata for
        
        Returns:
            Optional[Dict]: Node metadata if successful, None if request fails
        
        Example:
            >>> metadata = loader.get_node_metadata("53ae7eb3-7d3e-4ab5-b0f5-a427a3533191")
            >>> if metadata:
            ...     print(f"Node type: {metadata.get('nodeType')}")
        """
        url = f"{self.base_url}/api/v1/workspaces/{self.workspace_id}/nodes/{node_id}"
        response = requests.request("GET", url, headers=self.headers, data={})
        
        if response.status_code != 200:
            self.append_log(f"Warning: Failed to fetch metadata for node {node_id}")
            return None
            
        return json.loads(response.text)

    def load_data_to_snowflake(self, nodes_data: list[Dict]) -> int:
        """
        Load node metadata into Snowflake table.
        
        Creates a Snowflake DataFrame from the provided node data and
        loads it into the target table using append mode.
        
        Args:
            nodes_data (list[Dict]): List of node metadata dictionaries
                Each dict should contain:
                - workspace_id: str
                - node_id: str
                - datetime_added: datetime
                - node_data: dict
        
        Returns:
            int: Number of records loaded
        
        Example:
            >>> nodes = [{"workspace_id": "15", "node_id": "123", ...}]
            >>> loaded = loader.load_data_to_snowflake(nodes)
            >>> print(f"Loaded {loaded} records")
        """
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

            # Ensure table exists
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

            # Load to Snowflake
            if not nodes_data:
                self.append_log("No valid node data to load")
                return self.log_message

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
