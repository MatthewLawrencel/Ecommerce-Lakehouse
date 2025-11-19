{{ config(materialized='table') }}

select
    oi.order_item_id,
    oi.order_id,
    oi.product_name,
    oi.category,
    oi.price,
    oi.quantity,
    o.customer_id,
    o.order_date
from {{ ref('stg_order_items') }} oi
left join {{ ref('stg_orders') }} o
  on oi.order_id = o.order_id
