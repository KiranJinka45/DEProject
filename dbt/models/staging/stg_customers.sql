{{
    config(
        materialized='view',
        schema='silver'
    )
}}

with source_data as (
    select * from {{ source('bronze', 'bronze_customers') }}
),

typecasted as (
    select
        cast(customer_id as text) as customer_id,
        lower(trim(cast(email as text))) as email,
        trim(cast(first_name as text)) as first_name,
        trim(cast(last_name as text)) as last_name,
        upper(trim(coalesce(cast(country_code as text), 'US'))) as country_code,
        trim(cast(city as text)) as city,
        cast(signup_timestamp as timestamp with time zone) as signup_timestamp,
        cast(updated_at as timestamp with time zone) as updated_at,
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
            partition by customer_id 
            order by updated_at desc, _load_ts desc
        ) as row_num
    from typecasted
),

final as (
    select
        customer_id,
        email,
        first_name,
        last_name,
        concat_ws(' ', first_name, last_name) as full_name,
        country_code,
        city,
        signup_timestamp,
        updated_at,
        _load_ts,
        _batch_id,
        _source_file,
        _record_hash
    from deduplicated
    where row_num = 1
)

select * from final
