{{ config(materialized='table') }}

-- TABLE materialization: separate chain (not connected to defer_base/agg/summary)
-- Should NOT be cloned or built when running defer_agg chain only

select
    customer_id,
    first_name,
    last_name,
    email,
    current_timestamp() as _loaded_at
from {{ ref('seed_customers') }}
