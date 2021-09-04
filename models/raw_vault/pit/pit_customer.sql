with dates_customer_combinations as (
	select customer_pk, current_date - 6 as load_date
	from {{ ref('hub_customer') }}),
sat_details as (
	select customer_pk, load_date, first_name, last_name, email
    from {{ ref('sat_customer_details') }}),
pit_raw as (
    select
        dc.customer_pk, dc.load_date, max(sd.load_date) as load_date_details
    from dates_customer_combinations dc join sat_details sd
        on dc.customer_pk = sd.customer_pk
    group by 1, 2
)

select customer_pk, load_date, load_date_details
from pit_raw
