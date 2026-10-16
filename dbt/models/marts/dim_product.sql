{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with products_source as (
    select * from {{ ref('stg_products') }}
),

final as (
    select
        {{ generate_surrogate_key(['product_id']) }} as product_sk,
        product_id,
        sku,
        product_name,
        category,
        subcategory,
        cost_price,
        retail_price,
        unit_profit_margin,
        margin_percentage,
        is_active,
        case 
            when margin_percentage >= 50.0 then 'High Margin'
            when margin_percentage >= 25.0 then 'Medium Margin'
            else 'Low Margin'
        end as margin_tier,
        _load_ts,
        _record_hash
    from products_source
)

select * from final
