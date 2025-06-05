# Coalesce Snowflake Logs

This repository contains SQL queries for analyzing and tracking Coalesce execution logs in Snowflake. These queries help monitor job execution, track changes, and analyze performance patterns.

## Base Query - Coalesce Execution Log

This query extracts detailed execution information from Snowflake's query history for Coalesce jobs:

```sql
SELECT
    ROW_NUMBER() OVER (
        PARTITION BY coalesce_node_id, coalesce_stage_name, query_type 
        ORDER BY start_time DESC
    ) AS row_number_,
    MD5(coalesce_node_id || coalesce_stage_name || query_type) AS hash_column,
    *
FROM (
    SELECT
        TO_OBJECT(TO_VARIANT(query_tag)):"coalesce" AS coalesce_tags,
        TO_OBJECT(TO_VARIANT(query_tag)):"coalesce"."runID"::NUMBER AS coalesce_run_id,
        TO_OBJECT(TO_VARIANT(query_tag)):"coalesce"."nodeID"::STRING AS coalesce_node_id,
        TO_OBJECT(TO_VARIANT(query_tag)):"coalesce"."nodeName"::STRING AS coalesce_node_name,
        TO_OBJECT(TO_VARIANT(query_tag)):"coalesce"."stageName"::STRING AS coalesce_stage_name,
        TO_OBJECT(TO_VARIANT(query_tag)):"coalesce"."storageLocation"::STRING AS coalesce_storage_location,
        * 
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE query_tag LIKE '%coalesce%'
)
```

### Query Components:

1. **JSON Parsing**:
   - Extracts Coalesce metadata from query tags
   - Converts JSON fields to strongly-typed columns
   - Preserves all original query history fields

2. **Row Numbering**:
   - Assigns row numbers within each node/stage/query combination
   - Enables tracking of repeated executions
   - Ordered by start time descending

3. **Hash Generation**:
   - Creates a unique identifier for each node/stage/query combination
   - Facilitates change tracking and comparisons

## Example Implementation - Change Tracking Table

Here's how to create a table for tracking changes between executions:

```sql
CREATE OR REPLACE TABLE <YOUR_DATABASE>.<YOUR_SCHEMA>.coalesce_execution_log AS
WITH current_execution AS (
    -- Base query from above
    SELECT 
        row_number_,
        hash_column,
        coalesce_run_id,
        coalesce_node_id,
        coalesce_node_name,
        coalesce_stage_name,
        query_text,
        total_elapsed_time,
        bytes_scanned,
        rows_produced,
        start_time,
        end_time,
        execution_status
    FROM (
        -- Insert base query here
    )
    WHERE row_number_ = 1  -- Get most recent execution only
),
previous_execution AS (
    -- Same base query but for previous execution
    SELECT 
        row_number_,
        hash_column,
        coalesce_run_id AS prev_run_id,
        query_text AS prev_query_text,
        total_elapsed_time AS prev_elapsed_time,
        bytes_scanned AS prev_bytes_scanned,
        rows_produced AS prev_rows_produced,
        start_time AS prev_start_time,
        end_time AS prev_end_time,
        execution_status AS prev_status
    FROM (
        -- Insert base query here
    )
    WHERE row_number_ = 2  -- Get second most recent execution
)
SELECT 
    c.*,
    p.prev_run_id,
    p.prev_query_text,
    p.prev_elapsed_time,
    p.prev_bytes_scanned,
    p.prev_rows_produced,
    p.prev_start_time,
    p.prev_end_time,
    p.prev_status,
    -- Calculate changes
    (c.total_elapsed_time - p.prev_elapsed_time) AS elapsed_time_diff,
    (c.bytes_scanned - p.prev_bytes_scanned) AS bytes_scanned_diff,
    (c.rows_produced - p.prev_rows_produced) AS rows_produced_diff,
    -- Detect query changes
    CASE 
        WHEN HASH(c.query_text) != HASH(p.prev_query_text) THEN 'CHANGED'
        ELSE 'UNCHANGED'
    END AS query_change_status
FROM current_execution c
LEFT JOIN previous_execution p
    ON c.hash_column = p.hash_column;
```

### Table Features:

1. **Execution Comparison**:
   - Tracks current and previous execution metrics
   - Calculates performance differences
   - Detects query text changes

2. **Performance Metrics**:
   - Elapsed time differences
   - Bytes scanned changes
   - Row count variations

3. **Status Tracking**:
   - Execution status comparison
   - Query change detection
   - Run ID correlation

## Usage Examples

1. **Find Performance Regressions**:
```sql
SELECT *
FROM <YOUR_DATABASE>.<YOUR_SCHEMA>.coalesce_execution_log
WHERE elapsed_time_diff > 1000
ORDER BY elapsed_time_diff DESC;
```

2. **Track Query Changes**:
```sql
SELECT 
    coalesce_node_name,
    coalesce_stage_name,
    query_change_status,
    query_text,
    prev_query_text
FROM <YOUR_DATABASE>.<YOUR_SCHEMA>.coalesce_execution_log
WHERE query_change_status = 'CHANGED';
```

3. **Monitor Execution Patterns**:
```sql
SELECT 
    coalesce_node_name,
    AVG(total_elapsed_time) AS avg_elapsed_time,
    AVG(bytes_scanned) AS avg_bytes_scanned,
    COUNT(*) AS execution_count
FROM <YOUR_DATABASE>.<YOUR_SCHEMA>.coalesce_execution_log
GROUP BY coalesce_node_name
ORDER BY avg_elapsed_time DESC;
```

## Notes

- Queries filter out Streamlit-related executions
- User filtering can be adjusted based on needs
- Consider retention periods for historical data
- Performance impact of scanning query history
