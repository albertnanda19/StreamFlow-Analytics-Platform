{{
    config(
        materialized='incremental',
        unique_key='item_id',
        on_schema_change='append_new_columns',
        incremental_strategy='delete+insert',
        tags=['silver']
    )
}}

with exploded as (
    select
        so.order_id,
        so.user_id,
        so.event_timestamp,
        so.processing_date,
        so.dbt_updated_at                                    as order_updated_at,
        {{ generate_surrogate_key(['so.order_id', 'item.product_id', 'item_index::varchar']) }} as item_id,
        item_index,
        item.product_id,
        item.product_name                                    as item_product_name,
        item.category                                        as item_category,
        item.quantity,
        item.unit_price,
        item.discount_pct
    from {{ ref('silver_orders') }} so,
    lateral flatten(input => so.items) item_index,
    lateral (select so.items[item_index] as item) as _

    {% if is_incremental() %}
    where so.dbt_updated_at > {{ incremental_predicate('so.dbt_updated_at') }}
    {% endif %}
),

product_enriched as (
    select
        e.*,
        p.name           as product_name,
        p.category       as product_category,
        p.price          as base_price
    from exploded e
    left join {{ source('postgres_source', 'products') }} p
        on e.product_id = p.product_id::varchar
),

final as (
    select
        item_id,
        order_id,
        user_id,
        item_index,
        product_id,
        coalesce(product_name,      item_product_name)  as product_name,
        coalesce(product_category,  item_category)      as category,
        base_price,
        quantity,
        unit_price,
        discount_pct,
        quantity * unit_price * (1 - discount_pct)      as item_revenue,
        quantity * unit_price * discount_pct            as discount_amount,
        discount_pct > 0                                as is_discounted,
        event_timestamp,
        processing_date,
        current_timestamp()                             as dbt_updated_at
    from product_enriched
)

select * from final
