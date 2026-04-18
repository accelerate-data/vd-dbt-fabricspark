{{
    config(
        materialized='incremental',
        unique_key='order_date',
        incremental_strategy='merge',
        file_format='delta'
    )
}}

-- Materialization: INCREMENTAL (merge strategy on Delta)
-- Tests: incremental merge with unique_key on Fabric Spark
-- Run 1: full load. Run 2: should merge without duplicates.

select
    order_date,
    count(order_id) as order_count,
    sum(amount) as revenue,
    sum(case when status = 'completed' then amount else 0 end) as completed_revenue,
    sum(case when status = 'pending' then 1 else 0 end) as pending_orders,
    sum(case when status = 'cancelled' then 1 else 0 end) as cancelled_orders,
    current_timestamp() as _loaded_at
from {{ ref('stg_orders') }}

{% if is_incremental() %}
    where order_date >= (select max(order_date) from {{ this }})
{% endif %}

group by order_date
