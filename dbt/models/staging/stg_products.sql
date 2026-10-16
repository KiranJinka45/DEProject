{{
    config(
        materialized='view',
        schema='silver'
    )
}}

with source_data as (
    select * from {{ source('bronze', 'bronze_products') }}
),

typecasted as (
    select
        cast(product_id as text) as product_id,
        upper(trim(cast(sku as text))) as sku,
        trim(cast(product_name as text)) as product_name,
        lower(trim(cast(category as text))) as category,
        lower(trim(coalesce(cast(subcategory as text), 'general'))) as subcategory,
        cast(cost_price as numeric(10, 2)) as cost_price,
        cast(retail_price as numeric(10, 2)) as retail_price,
        cast(coalesce(is_active, true) as boolean) as is_active,
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
            partition by product_id 
            order by _load_ts desc
        ) as row_num
    from typecasted
),

final as (
    select
        product_id,
        sku,
        product_name,
        category,
        subcategory,
        cost_price,
        retail_price,
        round(retail_price - cost_price, 2) as unit_profit_margin,
        case 
            when retail_price > 0 then round(((retail_price - cost_price) / retail_price) * 100, 2)
            else 0.00
        end as margin_percentage,
        is_active,
        _load_ts,
        _batch_id,
        _source_file,
        _record_hash
    from deduplicated
    where row_num = 1
)

select * from final
