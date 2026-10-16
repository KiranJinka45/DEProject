{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with date_spine as (
    select
        datum::date as calendar_date
    from generate_series(
        '2022-01-01'::date,
        '2030-12-31'::date,
        '1 day'::interval
    ) as datum
),

transformed as (
    select
        cast(to_char(calendar_date, 'YYYYMMDD') as integer) as date_sk,
        calendar_date,
        extract(year from calendar_date)::integer as year_number,
        extract(quarter from calendar_date)::integer as quarter_number,
        concat('Q', extract(quarter from calendar_date)::text, '-', extract(year from calendar_date)::text) as year_quarter,
        extract(month from calendar_date)::integer as month_number,
        to_char(calendar_date, 'Month') as month_name,
        to_char(calendar_date, 'YYYY-MM') as year_month,
        extract(week from calendar_date)::integer as week_of_year,
        extract(isodow from calendar_date)::integer as day_of_week_iso,
        to_char(calendar_date, 'Day') as day_name,
        case when extract(isodow from calendar_date) in (6, 7) then true else false end as is_weekend,
        case 
            when extract(month from calendar_date) = 1 and extract(day from calendar_date) = 1 then true
            when extract(month from calendar_date) = 7 and extract(day from calendar_date) = 4 then true
            when extract(month from calendar_date) = 12 and extract(day from calendar_date) = 25 then true
            else false
        end as is_us_holiday
    from date_spine
)

select * from transformed
