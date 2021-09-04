with all_load_dates as (
	select load_date from {{ ref('hub_customer') }}
	union
	select load_date from {{ ref('sat_customer_details') }}),
dates_customer_combinations as (
	select c.customer_pk, d.load_date
	from all_load_dates d
		join {{ ref('hub_customer') }} c
			on d.load_date != c.load_date or d.load_date = c.load_date),
sat_details as (
	select customer_pk, load_date, first_name, last_name, email from {{ ref('sat_customer_details') }}),
pit_raw as (
    select
        c.customer_pk
        , c.load_date load_date
        , s.load_date load_date_changes
        , max(s.load_date) over (partition by c.customer_pk order by c.load_date, c.customer_pk) as load_date_details
    from dates_customer_combinations c left join sat_details s
        on c.customer_pk = s.customer_pk and c.load_date = s.load_date
    {{ dbt_utils.group_by(n=3) }}
)

select customer_pk, load_date, load_date_details
from pit_raw
