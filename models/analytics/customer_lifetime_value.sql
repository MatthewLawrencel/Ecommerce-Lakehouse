{{ config(materialized='table') }}

select
    customer_id,
    count(order_id) as total_orders,
    sum(total_amount) as lifetime_value,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date
from {{ ref('fact_orders') }}
where status = 'Delivered'
group by customer_id
order by lifetime_value desc
