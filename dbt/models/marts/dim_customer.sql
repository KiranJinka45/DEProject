{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with customer_source as (
    select * from {{ ref('stg_customers') }}
),

orders_agg as (
    select
        customer_id,
        min(order_timestamp) as first_order_timestamp,
        max(order_timestamp) as most_recent_order_timestamp,
        count(distinct order_id) as total_lifetime_orders,
        coalesce(sum(case when is_revenue_recognized then total_amount_usd else 0 end), 0.00) as lifetime_revenue_usd
    from {{ ref('stg_orders') }}
    group by customer_id
),

scd2_windows as (
    select
        customer_id,
        email,
        first_name,
        last_name,
        full_name,
        country_code,
        city,
        signup_timestamp,
        updated_at as valid_from,
        lead(updated_at) over (
            partition by customer_id 
            order by updated_at asc, _load_ts asc
        ) as valid_to,
        _load_ts,
        _record_hash
    from customer_source
),

final as (
    select
        {{ generate_surrogate_key(['c.customer_id', 'c.valid_from']) }} as customer_sk,
        c.customer_id,
        c.email,
        c.first_name,
        c.last_name,
        c.full_name,
        c.country_code,
        c.city,
        c.signup_timestamp,
        c.valid_from,
        coalesce(c.valid_to, '9999-12-31 23:59:59+00'::timestamp with time zone) as valid_to,
        case when c.valid_to is null then true else false end as is_current_record,
        
        -- Customer Lifetime Analytics Attributes
        coalesce(o.total_lifetime_orders, 0) as total_lifetime_orders,
        coalesce(o.lifetime_revenue_usd, 0.00) as lifetime_revenue_usd,
        o.first_order_timestamp,
        o.most_recent_order_timestamp,
        case 
            when coalesce(o.lifetime_revenue_usd, 0) >= 5000 then 'VIP / Platinum'
            when coalesce(o.lifetime_revenue_usd, 0) >= 2000 then 'Gold'
            when coalesce(o.lifetime_revenue_usd, 0) >= 500 then 'Silver'
            else 'Bronze'
        end as customer_ltv_tier,
        c._load_ts,
        c._record_hash
    from scd2_windows c
    left join orders_agg o
        on c.customer_id = o.customer_id
)

select * from final
