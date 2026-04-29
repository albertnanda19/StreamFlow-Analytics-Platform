{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='append_new_columns',
        incremental_strategy='delete+insert',
        tags=['silver']
    )
}}

with raw_orders as (
    select *
    from {{ source('bronze', 'orders') }}
    where is_valid = true

    {% if is_incremental() %}
        and ingestion_timestamp > {{ incremental_predicate('ingestion_timestamp') }}
    {% endif %}
),

deduplicated as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by order_id
                order by ingestion_timestamp desc
            ) as _rn
        from raw_orders
    ) ranked
    where _rn = 1
),

user_enriched as (
    select
        d.order_id,
        d.user_id,
        d.session_id,
        d.event_type,
        cast(d.event_timestamp as timestamp)  as event_timestamp,
        d.payment_method,
        d.subtotal,
        d.shipping_fee,
        d.total_amount,
        d.items,
        d.shipping_address,
        d.device_type,
        d.platform,
        d.coupon_code,
        d.ingestion_timestamp,
        d.processing_date,
        d.kafka_partition,
        d.kafka_offset,
        d.data_source,
        u.email                                          as user_email,
        coalesce(u.country, d.shipping_address.country) as user_country,
        coalesce(u.segment, 'UNKNOWN')                   as user_segment
    from deduplicated d
    left join {{ source('postgres_source', 'users') }} u
        on d.user_id = u.user_id::varchar
),

final as (
    select
        order_id,
        user_id,
        session_id,
        event_type,
        event_timestamp,
        upper(payment_method)                               as payment_method,
        subtotal,
        shipping_fee,
        total_amount,
        items,
        shipping_address,
        device_type,
        platform,
        coupon_code,
        user_email,
        user_country,
        user_segment,

        cardinality(items)                                  as order_item_count,

        (exists(
            select 1 from unnest(items) as t(item)
            where item.discount_pct > 0
        ))                                                  as has_discount,

        total_amount - shipping_fee                         as gross_revenue,

        total_amount > {{ var('high_value_order_threshold') }} as is_high_value,

        case
            when device_type = 'MOBILE'  then 'MOBILE'
            else 'NON-MOBILE'
        end                                                 as device_category,

        extract(hour from event_timestamp)                  as hour_of_day,
        extract(dow  from event_timestamp)                  as day_of_week,

        ingestion_timestamp,
        processing_date,
        data_source,
        current_timestamp()                                 as dbt_updated_at
    from user_enriched
)

select * from final
