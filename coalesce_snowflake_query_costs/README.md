# Coalesce Snowflake Query Costs

This query analyzes the compute costs of Snowflake queries executed by Coalesce over the past 30 days. It extracts cost attribution from query tags and calculates per-job and per-run costs based on compute credits used.

## Query

```sql
WITH rate as (
    select date, currency, effective_rate as rate
    from snowflake.organization_usage.rate_sheet_daily
    where account_locator = current_account()
    and usage_type = 'compute'
),
wh_bill AS (
   SELECT SUM(credits_used_compute) AS compute_credits,
        SUM(credits_used_compute * r.rate) AS compute_amount
   FROM snowflake.account_usage.warehouse_metering_history h
   left join rate r on r.date = h.start_time::date
   WHERE start_time >= DATE_TRUNC('DAY', DATEADD(DAY, -30, CURRENT_DATE))
   AND start_time < DATE_TRUNC('DAY', CURRENT_DATE)
),
tag_credits AS (
   SELECT COALESCE(NULLIF(TRY_PARSE_JSON(query_tag):coalesce:jobName::string, ''), 'untagged') AS job_name,
       COALESCE(NULLIF(TRY_PARSE_JSON(query_tag):coalesce:nodeName::string, ''), 'untagged') AS node_name, 
       COALESCE(NULLIF(TRY_PARSE_JSON(query_tag):coalesce:runType::string, ''), 'untagged') AS run_type, 
       COALESCE(NULLIF(TRY_PARSE_JSON(query_tag):coalesce:runID::string, ''), 'untagged') AS run_id, 
       SUM(credits_attributed_compute) AS credits,
       SUM(credits_attributed_compute * r.rate) AS amount
   FROM snowflake.account_usage.query_attribution_history h
   left join rate r on r.date = h.start_time::date
   WHERE start_time >= DATE_TRUNC('DAY', DATEADD(DAY, -30, CURRENT_DATE))
   AND start_time < DATE_TRUNC('DAY', CURRENT_DATE)
   GROUP BY ALL
),
total_credit AS (
   SELECT SUM(credits) AS sum_all_credits,
        SUM(amount) AS sum_all_amount
   FROM tag_credits
),
run_detail AS (
    SELECT tc.job_name, 
        tc.node_name,
        tc.run_type,
        tc.run_id,
        tc.credits / t.sum_all_credits * w.compute_credits AS attributed_credits,
        tc.amount / t.sum_all_amount * w.compute_amount AS attributed_amount
    FROM tag_credits tc, total_credit t, wh_bill w
  
),
job_detail AS (
    SELECT job_name, 
        run_id,
        COUNT(node_name) AS nodes_per_job,
        SUM(attributed_credits) AS credits_per_job,
        SUM(attributed_amount) AS amount_per_job
    FROM run_detail
    GROUP BY ALL
)

SELECT job_name,
    COUNT(DISTINCT run_id)AS number_of_runs_past_30_days,
    MAX(nodes_per_job) AS max_nodes_per_job,
    ROUND(SUM(amount_per_job),2) AS total_run_cost,
    ROUND(MIN(amount_per_job),2) AS min_run_cost,
    ROUND(AVG(amount_per_job),2) AS avg_run_cost,
    ROUND(MAX(amount_per_job),2) AS max_run_cost
FROM job_detail 
GROUP BY ALL
ORDER BY 4 DESC;
```

## What This Query Does

This query gets costs from tagged Snowflake queries executed by Coalesce over the past 30 days. It:

1. **Calculates Daily Rates**: Gets the effective compute rate for each day from the organization rate sheet
2. **Total Warehouse Billing**: Sums up total compute credits and costs for the past 30 days
3. **Extracts Coalesce Tags**: Parses query tags to identify Coalesce jobs, nodes, run types, and run IDs
4. **Cost Attribution**: Distributes warehouse costs proportionally based on attributed compute credits
5. **Per-Job Analysis**: Aggregates costs by job name, showing:
   - Number of runs in the past 30 days
   - Maximum nodes per job
   - Total, minimum, average, and maximum run costs
6. **Cost Ranking**: Orders results by total run cost (descending) to identify the most expensive jobs

The query handles untagged queries by labeling them as 'untagged' and provides comprehensive cost analysis for Coalesce job execution in Snowflake.