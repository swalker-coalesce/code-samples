-- Example usage:

/*
Quick Start Guide for LOAD_COALESCE_NODES Stored Procedure

Purpose:
    Loads Coalesce node metadata into a Snowflake table. The procedure will:
    - Create the target table if it doesn't exist
    - Fetch all nodes from the specified Coalesce workspace
    - Store both raw JSON and parsed metadata
    - Preserve existing data (append-only)

Required Parameters:
    1. WORKSPACE_ID: Your Coalesce workspace number (found in Coalesce URL)
    2. TARGET_DATABASE: Where to store the data
    3. TARGET_SCHEMA: Schema within the database
    4. TARGET_TABLE: Name for the table (defaults to COALESCE_NODE_METADATA)
    5. COALESCE_BASE_URL: Usually 'https://app.coalescesoftware.io'

Common Issues:
    - Invalid API token → Check your Coalesce API token
    - Security integration error → Verify the integration name and permissions
    - Permission denied → Ensure you have CREATE TABLE rights if table doesn't exist
    - Network error → Verify security integration allows access to Coalesce API

Example Call:
*/


USE DATABASE <YOUR_DATABASE>;
USE SCHEMA <YOUR_SCHEMA>;

-- Now you can call the procedure with the context already set
CALL LOAD_COALESCE_NODES(
    15,                                  -- WORKSPACE_ID
    CURRENT_DATABASE(),                  -- TARGET_DATABASE
    CURRENT_SCHEMA(),                    -- TARGET_SCHEMA
    'COALESCE_NODE_METADATA',           -- TARGET_TABLE
    'https://app.coalescesoftware.io'   -- COALESCE_BASE_URL
);

/*
After successful execution:
    - Check the returned messages for execution status
    - Query the table to verify data: SELECT * FROM <YOUR_DATABASE>.<YOUR_SCHEMA>.COALESCE_NODE_METADATA
    - View node metadata: SELECT node_data:nodeType, node_data:name FROM COALESCE_NODE_METADATA

Need help?
    - Check the full procedure documentation above
    - Verify all parameters are correct
    - Ensure you have appropriate permissions
    - Contact your Snowflake admin for security integration issues
*/