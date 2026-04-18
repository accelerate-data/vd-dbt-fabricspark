-- Verify that dbt_invocations captured all our integration test commands.
-- We expect at least: seed, run (x2), test (x2) = 5 invocations minimum.
-- This test FAILS if fewer than 5 invocations are recorded.

with invocation_summary as (
    select
        count(*) as total_invocations,
        count(distinct invocation_id) as distinct_invocations,
        count(case when command = 'seed' then 1 end) as seed_count,
        count(case when command = 'run' then 1 end) as run_count,
        count(case when command = 'test' then 1 end) as test_count
    from {{ target.lakehouse }}.elementary.dbt_invocations
)

-- Fail if we don't have at least 5 invocations (seed + run + test + run + test)
select
    total_invocations,
    distinct_invocations,
    seed_count,
    run_count,
    test_count,
    'Expected at least 5 invocations (1 seed + 2 run + 2 test), got ' || cast(total_invocations as string) as failure_reason
from invocation_summary
where total_invocations < 5
