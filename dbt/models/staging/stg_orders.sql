{{
    config(
        materialized='view',
        schema='silver'
    )
}}

with source_data as (
    select * from {{ source('bronze', 'bronze_orders') }}
),

typecasted as (
    select
        cast(order_id as text) as order_id,
        cast(customer_id as text) as customer_id,
        lower(trim(cast(order_status as text))) as order_status,
        cast(order_timestamp as timestamp with time zone) as order_timestamp,
        cast(total_amount as numeric(12, 2)) as total_amount,
        upper(trim(coalesce(cast(currency as text), 'USD'))) as currency,
        cast(shipping_address as text) as shipping_address,
        lower(trim(coalesce(cast(payment_method as text), 'unknown'))) as payment_method,
        cast(_load_ts as timestamp with time zone) as _load_ts,
        cast(_batch_id as text) as _batch_id,
        cast(_source_file as text) as _source_file,
        cast(_record_hash as text) as _record_hash
    from source_data
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by order_id 
            order by order_timestamp desc, _load_ts desc
        ) as row_num
    from typecasted
),

final as (
    select
        order_id,
        customer_id,
        order_status,
        order_timestamp,
        cast(order_timestamp as date) as order_date,
        total_amount,
        currency,
        {{ convert_to_usd('total_amount', 'currency') }} as total_amount_usd,
        shipping_address,
        payment_method,
        case 
            when order_status in ('completed', 'delivered', 'shipped') then true
            else false
        end as is_revenue_recognized,
        _load_ts,
        _batch_id,
        _source_file,
        _record_hash
    from deduplicated
    where row_num = 1
)

select * from final
