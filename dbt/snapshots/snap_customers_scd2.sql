{% snapshot snap_customers_scd2 %}

{{
    config(
        target_database='ecommerce_warehouse',
        target_schema='silver',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

select
    customer_id,
    email,
    first_name,
    last_name,
    full_name,
    country_code,
    city,
    signup_timestamp,
    updated_at,
    _load_ts,
    _record_hash
from {{ ref('stg_customers') }}

{% endsnapshot %}
