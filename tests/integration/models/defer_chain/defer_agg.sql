{{
    config(
        materialized='incremental',
        unique_key='order_date',
        incremental_strategy='merge',
        file_format='delta'
    )
}}

-- INCREMENTAL materialization: daily aggregation
-- This is the key model for defer+clone testing:
--   - Clone from prod so is_incremental()=true
--   - Defer upstream ref('defer_base') to prod when not selected

select
    order_date,
    count(order_id) as order_count,
    sum(amount) as total_amount,
    sum(amount_with_tax) as total_with_tax,
    current_timestamp() as _loaded_at
from {{ ref('defer_base') }}

{% if is_incremental() %}
    where order_date >= (select max(order_date) from {{ this }})
{% endif %}

group by order_date
