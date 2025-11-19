{{ config(materialized='table') }}

select
    status,
    count(order_id) as total_orders
from {{ ref('fact_orders') }}
group by status
order by total_orders desc
