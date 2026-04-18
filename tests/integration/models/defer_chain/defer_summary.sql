{{ config(materialized='view') }}

-- VIEW materialization: summary on top of incremental
-- Tests that views downstream of incremental work with defer

select
    order_date,
    order_count,
    total_amount,
    total_with_tax,
    total_with_tax - total_amount as tax_amount,
    case
        when order_count > 2 then 'high'
        when order_count > 1 then 'medium'
        else 'low'
    end as volume_tier
from {{ ref('defer_agg') }}
