-- Materialization: TABLE (default for staging/)
-- Tests: basic table materialization with type casting

select
    order_id,
    customer_id,
    cast(order_date as date) as order_date,
    cast(amount as decimal(10, 2)) as amount,
    status,
    current_timestamp() as _loaded_at
from {{ ref('seed_orders') }}
