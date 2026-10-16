{{
    config(
        materialized='incremental',
        unique_key='order_sk',
        schema='gold'
    )
}}

with orders_source as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
        where _load_ts > (select coalesce(max(_load_ts), '1970-01-01'::timestamp with time zone) from {{ this }})
    {% endif %}
),

order_items_summary as (
    select
        order_id,
        count(distinct order_item_id) as total_line_items,
        sum(quantity) as total_units_ordered,
        sum(discount_amount) as total_discount_amount
    from {{ ref('stg_order_items') }}
    group by order_id
),

customers as (
    select * from {{ ref('dim_customer') }}
),

all_orders as (
    select
        order_id,
        customer_id,
        order_timestamp,
        row_number() over (
            partition by customer_id 
            order by order_timestamp asc
        ) as customer_order_sequence
    from {{ ref('stg_orders') }}
),

final as (
    select
        {{ generate_surrogate_key(['o.order_id']) }} as order_sk,
        o.order_id,
        cast(to_char(o.order_date, 'YYYYMMDD') as integer) as order_date_sk,
        coalesce(c.customer_sk, {{ generate_surrogate_key(["'UNKNOWN_CUSTOMER'"]) }}) as customer_sk,
        o.customer_id,
        o.order_status,
        o.order_timestamp,
        o.order_date,
        o.currency,
        o.total_amount as original_total_amount,
        o.total_amount_usd,
        coalesce(i.total_line_items, 0) as total_line_items,
        coalesce(i.total_units_ordered, 0) as total_units_ordered,
        coalesce(i.total_discount_amount, 0.00) as total_discount_amount,
        o.shipping_address,
        o.payment_method,
        o.is_revenue_recognized,
        case when ao.customer_order_sequence = 1 then true else false end as is_first_order,
        ao.customer_order_sequence,
        o._load_ts,
        o._batch_id,
        o._source_file,
        o._record_hash
    from orders_source o
    left join order_items_summary i
        on o.order_id = i.order_id
    left join all_orders ao
        on o.order_id = ao.order_id
    left join customers c
        on o.customer_id = c.customer_id
        and (
            (o.order_timestamp >= c.valid_from and o.order_timestamp < c.valid_to)
            or (c.is_current_record = true and c.valid_to is null)
        )
)

select * from final
