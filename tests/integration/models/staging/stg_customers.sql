-- Materialization: TABLE (default for staging/)
-- Tests: basic table materialization on Fabric Spark

select
    customer_id,
    first_name,
    last_name,
    email,
    cast(created_at as date) as created_at,
    current_timestamp() as _loaded_at
from {{ ref('seed_customers') }}
