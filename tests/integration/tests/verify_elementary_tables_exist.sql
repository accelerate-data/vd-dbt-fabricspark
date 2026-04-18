-- Verify that critical elementary tables exist and have data.
-- Tables are split into two groups:
--   must_have_data: tables that MUST have rows after a dbt run
--   can_be_empty:   tables that may legitimately be empty (no snapshots, no metrics, etc.)
-- This test FAILS only if a must_have_data table has 0 rows.

{% set must_have_data = [
    'dbt_invocations',
    'dbt_run_results',
    'dbt_tests',
    'dbt_models',
    'dbt_seeds',
] %}

{% set can_be_empty = [
    'dbt_columns',
    'dbt_metrics',
    'elementary_test_results',
    'model_run_results',
    'snapshot_run_results',
] %}

{% set ns = namespace(union_parts=[]) %}

{% for table_name in must_have_data %}
    {% set query %}
        select
            '{{ table_name }}' as table_name,
            'must_have_data' as check_type,
            count(*) as row_count
        from {{ target.lakehouse }}.elementary.{{ table_name }}
    {% endset %}
    {% set ns.union_parts = ns.union_parts + [query] %}
{% endfor %}

{% for table_name in can_be_empty %}
    {% set query %}
        select
            '{{ table_name }}' as table_name,
            'can_be_empty' as check_type,
            count(*) as row_count
        from {{ target.lakehouse }}.elementary.{{ table_name }}
    {% endset %}
    {% set ns.union_parts = ns.union_parts + [query] %}
{% endfor %}

with table_counts as (
    {{ ns.union_parts | join('\n    union all\n    ') }}
)

-- Only fail on must_have_data tables that are empty
select
    table_name,
    check_type,
    row_count
from table_counts
where check_type = 'must_have_data' and row_count = 0
