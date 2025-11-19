{{ config(materialized='view') }}

select
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    cast(created_at as date) as created_at
from {{ source('raw_ecommerce', 'raw_customers') }}
