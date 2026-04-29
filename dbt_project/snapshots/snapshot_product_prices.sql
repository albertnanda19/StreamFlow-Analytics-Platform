{% snapshot snapshot_product_prices %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}

select
    product_id,
    name            as product_name,
    category,
    price,
    stock_quantity,
    is_active,
    created_at,
    updated_at
from {{ source('postgres_source', 'products') }}

{% endsnapshot %}
