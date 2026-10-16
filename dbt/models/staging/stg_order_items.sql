{{
    config(
        materialized='view',
        schema='silver'
    )
}}

with source_data as (
    select * from {{ source('bronze', 'bronze_order_items') }}
),

typecasted as (
    select
        cast(order_item_id as text) as order_item_id,
        cast(order_id as text) as order_id,
        cast(product_id as text) as product_id,
        cast(quantity as integer) as quantity,
        cast(unit_price as numeric(10, 2)) as unit_price,
        cast(coalesce(discount_amount, 0.00) as numeric(10, 2)) as discount_amount,
        cast(subtotal as numeric(12, 2)) as subtotal,
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
            partition by order_item_id 
            order by _load_ts desc
        ) as row_num
    from typecasted
),

final as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount_amount,
        subtotal,
        round((unit_price * quantity) - discount_amount, 2) as calculated_net_subtotal,
        _load_ts,
        _batch_id,
        _source_file,
        _record_hash
    from deduplicated
    where row_num = 1
)

select * from final
