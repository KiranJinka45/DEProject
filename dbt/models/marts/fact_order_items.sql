{{
    config(
        materialized='incremental',
        unique_key='order_item_sk',
        schema='gold'
    )
}}

with items_source as (
    select * from {{ ref('stg_order_items') }}
    {% if is_incremental() %}
        where _load_ts > (select coalesce(max(_load_ts), '1970-01-01'::timestamp with time zone) from {{ this }})
    {% endif %}
),

orders as (
    select 
        order_id,
        order_date,
        order_timestamp,
        currency,
        order_status,
        is_revenue_recognized
    from {{ ref('stg_orders') }}
),

products as (
    select 
        product_sk,
        product_id,
        cost_price,
        retail_price
    from {{ ref('dim_product') }}
),

final as (
    select
        {{ generate_surrogate_key(['i.order_item_id']) }} as order_item_sk,
        i.order_item_id,
        {{ generate_surrogate_key(['i.order_id']) }} as order_sk,
        i.order_id,
        coalesce(p.product_sk, {{ generate_surrogate_key(["'UNKNOWN_PRODUCT'"]) }}) as product_sk,
        i.product_id,
        cast(to_char(o.order_date, 'YYYYMMDD') as integer) as order_date_sk,
        o.order_timestamp,
        o.order_status,
        o.is_revenue_recognized,
        i.quantity,
        i.unit_price,
        i.discount_amount,
        i.subtotal as gross_subtotal,
        i.calculated_net_subtotal,
        {{ convert_to_usd('i.calculated_net_subtotal', "coalesce(o.currency, 'USD')") }} as net_subtotal_usd,
        coalesce(p.cost_price, 0.00) * i.quantity as total_cost_usd,
        round(
            {{ convert_to_usd('i.calculated_net_subtotal', "coalesce(o.currency, 'USD')") }} 
            - (coalesce(p.cost_price, 0.00) * i.quantity),
            2
        ) as gross_margin_usd,
        i._load_ts,
        i._batch_id,
        i._source_file,
        i._record_hash
    from items_source i
    left join orders o
        on i.order_id = o.order_id
    left join products p
        on i.product_id = p.product_id
)

select * from final
