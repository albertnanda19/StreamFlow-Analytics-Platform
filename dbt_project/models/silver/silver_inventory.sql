{{
    config(
        materialized='incremental',
        unique_key='event_id',
        on_schema_change='append_new_columns',
        incremental_strategy='delete+insert',
        tags=['silver']
    )
}}

with raw_inv as (
    select *
    from {{ source('bronze', 'inventory_state') }}

    {% if is_incremental() %}
    where ingestion_timestamp > {{ incremental_predicate('ingestion_timestamp') }}
    {% endif %}
),

deduplicated as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by event_id
                order by ingestion_timestamp desc
            ) as _rn
        from raw_inv
    ) r
    where _rn = 1
),

with_running_qty as (
    select
        *,
        new_quantity - previous_quantity    as quantity_change,
        new_quantity = 0                    as is_stock_out,
        new_quantity < {{ var('low_stock_threshold') }} as is_low_stock,
        sum(new_quantity - previous_quantity) over (
            partition by product_id
            order by event_timestamp
            rows between unbounded preceding and current row
        )                                   as running_quantity
    from deduplicated
),

product_enriched as (
    select
        i.*,
        p.name      as product_name,
        p.category  as product_category
    from with_running_qty i
    left join {{ source('postgres_source', 'products') }} p
        on i.product_id = p.product_id::varchar
)

select
    event_id,
    event_type,
    cast(event_timestamp as timestamp)  as event_timestamp,
    product_id,
    product_name,
    product_category,
    warehouse_id,
    previous_quantity,
    new_quantity,
    quantity_change,
    running_quantity,
    change_reason,
    reference_id,
    is_stock_out,
    is_low_stock,
    ingestion_timestamp,
    processing_date,
    current_timestamp()                 as dbt_updated_at
from product_enriched
