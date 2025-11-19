{{ config(materialized='table') }}

select
    order_date,
    sum(total_amount) as daily_revenue,
    count(order_id) as total_orders
from {{ ref('fact_orders') }}
where status = 'Delivered'
group by order_date
order by order_date
