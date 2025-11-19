{{ config(materialized='view') }}

select
    order_item_id,
    order_id,
    product_name,
    category,
    cast(price as int64) as price,
    cast(quantity as int64) as quantity
from {{ source('raw_ecommerce', 'raw_order_items') }}
