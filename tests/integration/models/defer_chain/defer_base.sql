{{ config(materialized='table') }}

-- TABLE materialization: base orders with enrichment
-- Used as upstream for incremental defer_agg

select
    order_id,
    customer_id,
    order_date,
    amount,
    status,
    amount * 1.1 as amount_with_tax,
    current_timestamp() as _loaded_at
from {{ ref('seed_orders') }}
