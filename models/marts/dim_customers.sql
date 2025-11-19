{{ config(materialized='table') }}

select
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    created_at
from {{ ref('stg_customers') }}
