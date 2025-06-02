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
    node_id STRING,            -- Unique identifier for the node
    datetime_added TIMESTAMP_NTZ, -- When the record was loaded
    node_data VARIANT         -- Complete node metadata as JSON
)
```

## Usage

### Basic Example

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

## Security Setup

### 1. Create a Security Integration

```sql
CREATE SECURITY INTEGRATION your_integration_name
  TYPE = API_INTEGRATION
  ENABLED = TRUE
  API_ALLOWED_PREFIXES = ('https://app.australia-southeast1.gcp.coalescesoftware.io');
```

### 2. Store Your API Token

```sql
CREATE SECRET your_secret_name
  TYPE = GENERIC_STRING
  SECRET_STRING = 'your-coalesce-api-token';
```

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
