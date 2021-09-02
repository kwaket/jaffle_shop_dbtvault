with sat_order_details as (
    select * from {{ ref('sat_order_details') }}
)


SELECT to_char(order_date, 'IYYY-IW') week, status, count(*)
FROM sat_order_details
{{ dbt_utils.group_by(n=2) }}
