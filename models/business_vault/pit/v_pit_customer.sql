with all_load_dates as (
    select load_date::date from {{ ref('hub_customer') }}
    union
    select load_date::date from {{ ref('sat_customer_details') }}
    union
    select load_date::date from {{ ref('sat_customer_crm_data') }}),
dates_customer_combinations as (
    select c.customer_pk, d.load_date
    from all_load_dates d
        cross join {{ ref('hub_customer') }} as c),
sat_details as (
    select customer_pk, load_date, first_name, last_name, email
    from {{ ref('sat_customer_details') }}),
sat_crm as (
    select customer_pk, load_date, country, age
    from {{ ref('sat_customer_crm_data') }})
select
    dc.customer_pk
    , dc.load_date load_date

    , max(sd.load_date) over (partition by dc.customer_pk order by dc.load_date) as load_date_details
    , max(sc.load_date) over (partition by dc.customer_pk order by dc.load_date) as load_date_crm_data
from dates_customer_combinations dc
left join sat_details sd
    on dc.customer_pk = sd.customer_pk and dc.load_date = sd.load_date::date
left join sat_crm sc
    on dc.customer_pk = sc.customer_pk and dc.load_date = sc.load_date::date
