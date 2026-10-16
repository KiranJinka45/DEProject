{{
    config(
        materialized='view',
        schema='silver'
    )
}}

with source_data as (
    select * from {{ source('bronze', 'bronze_marketing_spend') }}
),

typecasted as (
    select
        cast(campaign_id as text) as campaign_id,
        trim(cast(channel as text)) as channel,
        cast(spend_date as date) as spend_date,
        cast(spend_amount as numeric(10, 2)) as spend_amount,
        cast(impressions as integer) as impressions,
        cast(clicks as integer) as clicks,
        cast(conversions as integer) as conversions,
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
            partition by campaign_id, spend_date 
            order by _load_ts desc
        ) as row_num
    from typecasted
),

final as (
    select
        campaign_id,
        channel,
        spend_date,
        spend_amount,
        impressions,
        clicks,
        conversions,
        case when impressions > 0 then round((cast(clicks as numeric) / impressions) * 100, 3) else 0.00 end as ctr_percentage,
        case when clicks > 0 then round(spend_amount / clicks, 2) else 0.00 end as cpc_usd,
        case when conversions > 0 then round(spend_amount / conversions, 2) else 0.00 end as cpa_usd,
        _load_ts,
        _batch_id,
        _source_file,
        _record_hash
    from deduplicated
    where row_num = 1
)

select * from final
