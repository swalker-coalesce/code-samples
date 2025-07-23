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

## Required Setup

Run these statements in order to configure the necessary components. Note that some steps require ACCOUNTADMIN privileges.

### 0. Set Context and Variables
First, set your working context and store common variables:

```sql
-- Set your working context
USE ROLE ACCOUNTADMIN;
USE DATABASE <YOUR_DATABASE>;
USE SCHEMA <YOUR_SCHEMA>;

-- Store common variables
SET COALESCE_URL = 'https://app.australia-southeast1.gcp.coalescesoftware.io';
SET CURRENT_DB = CURRENT_DATABASE();
SET CURRENT_SCHEMA = CURRENT_SCHEMA();
```

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
CREATE OR REPLACE  NETWORK RULE coalesce_api_rule
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ($COALESCE_URL);
```


### 3. Create the External Access Integration
This global integration binds the network rules and secrets (requires ACCOUNTADMIN):

```sql
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION coalesce_access_integration
  ALLOWED_NETWORK_RULES = (coalesce_api_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (your_secret_name)
  ENABLED = TRUE;
```

### 4. Grant Usage Permissions
Allow your role to use the integrations:

```sql
GRANT USAGE ON INTEGRATION coalesce_access_integration TO ROLE your_role;
```

### 5. Verify the Setup
Confirm the integrations are properly configured:

```sql
SHOW EXTERNAL ACCESS INTEGRATIONS;
```

### 6. Deploy the Stored Procedure
Review and deploy `load_node_metadata_sp.sql`. This file contains:
- Complete stored procedure implementation
- Python code for API interaction and data loading
- Comprehensive inline documentation
- Parameter validation and error handling

### 7. Run the Stored Procedure
Use `load_node_metadata_example.sql` which provides:
- Step-by-step usage guide
- Example procedure calls
- Common troubleshooting tips
- Verification queries

Since you've already set your context in step 0, you can run the procedure using:

```sql
-- Simple execution using current context
CALL LOAD_COALESCE_NODES(
    15,                     -- WORKSPACE_ID
    $CURRENT_DB,           -- TARGET_DATABASE
    $CURRENT_SCHEMA,       -- TARGET_SCHEMA
    'COALESCE_NODE_METADATA',           -- TARGET_TABLE
    $COALESCE_URL         -- COALESCE_BASE_URL
);

-- Verify the data was loaded
SELECT COUNT(*) FROM COALESCE_NODE_METADATA;

-- View some sample data
SELECT 
    node_data:name::STRING as node_name,
    node_data:nodeType::STRING as node_type,
    node_data:database::STRING as database_name,
    node_data:schema::STRING as schema_name
FROM COALESCE_NODE_METADATA
LIMIT 5;

-- Check the most recent load
SELECT 
    COUNT(*) as node_count,
    MAX(datetime_added) as last_loaded,
    workspace_id
FROM COALESCE_NODE_METADATA
GROUP BY workspace_id
ORDER BY last_loaded DESC;
```

For automated jobs or different contexts, you can also use the fully qualified version:

```sql
CALL $CURRENT_DB.$CURRENT_SCHEMA.LOAD_COALESCE_NODES(
    15,                     -- WORKSPACE_ID
    $CURRENT_DB,           -- TARGET_DATABASE
    $CURRENT_SCHEMA,       -- TARGET_SCHEMA
    'COALESCE_NODE_METADATA',           -- TARGET_TABLE
    $COALESCE_URL         -- COALESCE_BASE_URL
);
```

### 9. Create the Flattened View
Deploy `view_node_data.sql` to create a view that:
- Flattens the JSON metadata into columns
- Provides easier access to common node properties
- Enables simpler querying without JSON path syntax
- Excludes system and temporary nodes

## Querying the Data

After loading the data, you can either:
1. Query the raw table directly (see examples in `load_node_metadata_example.sql`)
2. Use the flattened view for easier access (see `view_node_data.sql`)

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

