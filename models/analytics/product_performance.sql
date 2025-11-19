{{ config(materialized='table') }}

select
    product_name,
    category,
    sum(quantity) as total_units_sold,
    sum(price * quantity) as total_revenue
from {{ ref('fact_order_items') }}
group by product_name, category
order by total_revenue desc
