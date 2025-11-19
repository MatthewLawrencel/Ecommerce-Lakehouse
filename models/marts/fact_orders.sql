{{ config(materialized='table') }}

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    o.total_amount,
    c.created_at as customer_created_at
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c
  on o.customer_id = c.customer_id
