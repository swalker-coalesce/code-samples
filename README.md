# Coalesce Utilities for Snowflake

This repository contains a collection of utilities and tools for working with Coalesce and Snowflake. These tools help with monitoring, debugging, and managing Coalesce jobs in Snowflake environments.

## Components

### 1. Coalesce Run ID Function
[`/coalesce_run_id_function`](./coalesce_run_id_function)

A Snowflake function that extracts the Coalesce run ID from query tags in the current session. Features:
- Retrieves run ID from Coalesce query tags
- Useful for tracking and debugging job executions
- Can be used in node mappings and SQL queries
- Returns -2 when no run ID is found in workspace execution

Configuration:
```json
{
    "run_id_function_database": "<YOUR DATABASE>",
    "run_id_function_schema": "<YOUR SCHEMA>"
}
```

### 2. Coalesce Snowflake Logs
[`/coalesce_snowflake_logs`](./coalesce_snowflake_logs)

SQL queries and tools for analyzing Coalesce execution logs in Snowflake. Capabilities:
- Extracts detailed execution information from query history
- Tracks changes between executions
- Monitors performance metrics
- Detects query modifications

Key features:
- JSON parsing of query tags
- Performance comparison
- Change tracking
- Execution pattern analysis

### 3. Coalesce Metadata Snowflake
[`/coalesce_metadata_snowflake`](./coalesce_metadata_snowflake)

A collection of tools for loading and managing Coalesce node metadata in Snowflake and Microsoft Purview. Contains:

1. **Snowflake Loader** (`load_node_metadata_sp.sql`):
   - Loads node metadata from Coalesce API into Snowflake
   - Automatic table creation and schema management
   - Comprehensive error handling and logging
   - Support for incremental updates

2. **Purview Integration** (`load_node_metadata_purview_sp.sql`):
   - Exports node metadata to Microsoft Purview
   - Maps Coalesce nodes to Atlas entities
   - Maintains relationships and lineage
   - Batch processing support

3. **Metadata View** (`view_node_data.sql`):
   - Flattened view of node metadata
   - Easy access to common node properties
   - Simplified querying interface

4. **Example Usage** (`load_node_metadata_example.sql`):
   - Implementation examples
   - Common use cases
   - Configuration templates

Key features:
- API integration with error handling
- Flexible configuration options
- Comprehensive documentation
- Multiple deployment examples
