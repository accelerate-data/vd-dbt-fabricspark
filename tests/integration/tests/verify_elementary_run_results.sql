-- Verify that model_run_results captured our model executions.
-- We have 4 models (stg_customers, stg_orders, mart_customer_summary, mart_daily_revenue).
-- After 2 dbt run invocations, we expect at least 4 distinct models tracked.

with run_summary as (
    select
        count(*) as total_results,
        count(distinct unique_id) as distinct_models,
        count(case when status = 'success' then 1 end) as success_count,
        count(case when status = 'error' then 1 end) as error_count
    from {{ target.lakehouse }}.elementary.model_run_results
)

-- Fail if we don't have at least 4 distinct models or if any errors
select
    total_results,
    distinct_models,
    success_count,
    error_count,
    case
        when distinct_models < 4 then 'Expected at least 4 distinct models, got ' || cast(distinct_models as string)
        when error_count > 0 then 'Found ' || cast(error_count as string) || ' model errors in run results'
    end as failure_reason
from run_summary
where distinct_models < 4 or error_count > 0
