{{ config(materialized='view') }}

select
    order_id,
    customer_id,
    cast(order_date as date) as order_date,
    status,
    cast(total_amount as int64) as total_amount
from {{ source('raw_ecommerce', 'raw_orders') }}
