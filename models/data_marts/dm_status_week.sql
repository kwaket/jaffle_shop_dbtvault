{{
    config(
        enabled=True,
        materialized='table'
    )
}}


with sat_order_details as (
    select * from {{ ref('sat_order_details') }}
)


SELECT to_char(order_date, 'IYYY-IW') week, status, count(*)
FROM sat_order_details
GROUP BY week, status
