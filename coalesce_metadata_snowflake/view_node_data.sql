/*
Description:
    This view flattens the JSON node metadata from Coalesce nodes into queryable columns.
    It extracts commonly used fields from the VARIANT column and maintains the original JSON
    for additional querying if needed.

Usage:
    -- First set the context:
    USE DATABASE <YOUR_DATABASE>;
    USE SCHEMA <YOUR_SCHEMA>;
    
    -- Then query the view:
    SELECT * FROM FLATTENED_NODE_VIEW WHERE node_type = 'TABLE';
    SELECT * FROM FLATTENED_NODE_VIEW WHERE database_name = 'MY_DB';

Note: 
    - Some fields might be NULL depending on the node type
    - The original node_data is preserved in the view for custom JSON path queries
    - This view reads from the COALESCE_NODE_METADATA table that LOAD_COALESCE_NODES procedure writes to
*/

CREATE OR REPLACE VIEW FLATTENED_NODE_VIEW AS
SELECT
    -- Node identification
    node_data:"id"::STRING                        AS node_id,
    node_data:"name"::STRING                      AS node_name,
    node_data:"nodeType"::STRING                  AS node_type,
    node_data:"description"::STRING               AS description,
    
    -- Location information
    node_data:"locationName"::STRING              AS location_name,
    node_data:"database"::STRING                  AS database_name,
    node_data:"schema"::STRING                    AS schema_name,
    node_data:"table"::STRING                     AS table_name,
    
    -- Configuration
    node_data:"materializationType"::STRING       AS materialization_type,
    node_data:"isMultisource"::BOOLEAN           AS is_multisource,
    node_data:"config":"incrementalKey"::STRING   AS incremental_key,
    node_data:"config":"partitionBy"::ARRAY       AS partition_by,
    node_data:"config":"clusterBy"::ARRAY         AS cluster_by,
    
    -- Join information
    node_data:"metadata":"join":"joinType"::STRING       AS join_type,
    node_data:"metadata":"join":"joinCondition"::STRING  AS join_condition,
    
    -- SQL information
    node_data:"metadata":"sql"::STRING            AS sql_query,
    node_data:"metadata":"dependencies"::ARRAY    AS dependencies,
    
    -- Raw JSON objects for custom querying
    node_data:"config"                            AS config_json,
    node_data:"metadata"                          AS metadata_json,
    node_data                                     AS node_data
FROM COALESCE_NODE_METADATA  -- Uses current database and schema context

-- Optional: Add some common filters to exclude system tables
WHERE node_data:"nodeType"::STRING NOT IN ('SYSTEM', 'TEMPORARY');