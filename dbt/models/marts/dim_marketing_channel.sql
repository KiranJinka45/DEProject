{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with spend_source as (
    select * from {{ ref('stg_marketing_spend') }}
),

aggregated as (
    select
        channel,
        count(distinct campaign_id) as total_campaigns,
        min(spend_date) as first_campaign_date,
        max(spend_date) as last_campaign_date,
        sum(spend_amount) as total_historical_spend_usd,
        sum(impressions) as total_impressions,
        sum(clicks) as total_clicks,
        sum(conversions) as total_conversions
    from spend_source
    group by channel
),

final as (
    select
        {{ generate_surrogate_key(['channel']) }} as channel_sk,
        channel as channel_name,
        total_campaigns,
        first_campaign_date,
        last_campaign_date,
        total_historical_spend_usd,
        total_impressions,
        total_clicks,
        total_conversions,
        case 
            when total_impressions > 0 then round((cast(total_clicks as numeric) / total_impressions) * 100, 3)
            else 0.00
        end as lifetime_ctr_pct,
        case 
            when total_clicks > 0 then round(total_historical_spend_usd / total_clicks, 2)
            else 0.00
        end as lifetime_cpc_usd,
        case 
            when total_conversions > 0 then round(total_historical_spend_usd / total_conversions, 2)
            else 0.00
        end as lifetime_cpa_usd
    from aggregated
)

select * from final
