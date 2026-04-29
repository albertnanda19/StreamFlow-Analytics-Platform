{{
    config(
        materialized='incremental',
        unique_key='session_id',
        on_schema_change='append_new_columns',
        incremental_strategy='delete+insert',
        tags=['silver']
    )
}}

with pv as (
    select *
    from {{ ref('silver_pageviews') }}

    {% if is_incremental() %}
    where ingestion_timestamp > {{ incremental_predicate('ingestion_timestamp') }}
    {% endif %}
),

session_agg as (
    select
        session_id,
        min(event_timestamp)                                as session_start,
        max(event_timestamp)                                as session_end,
        datediff('second', min(event_timestamp), max(event_timestamp))
                                                            as session_duration_seconds,
        count(*)                                            as page_count,
        count(distinct page_url)                            as unique_pages_visited,
        count(*) = 1                                        as is_bounce,
        array_agg(page_type order by event_timestamp)       as pages_visited,
        array_agg(distinct product_id) filter (
            where product_id is not null
        )                                                   as viewed_products,
        max(user_id)                                        as user_id,
        mode() within group (order by device_type)          as device_type,
        mode() within group (order by utm_source)           as utm_source,
        mode() within group (order by utm_medium)           as utm_medium,
        mode() within group (order by utm_campaign)         as utm_campaign,
        max(processing_date)                                as processing_date
    from pv
    group by session_id
),

with_conversion as (
    select
        s.*,
        (o.session_id is not null)  as has_converted
    from session_agg s
    left join (
        select distinct session_id
        from {{ ref('silver_orders') }}
    ) o on s.session_id = o.session_id
)

select
    session_id,
    session_start,
    session_end,
    session_duration_seconds,
    page_count,
    unique_pages_visited,
    is_bounce,
    pages_visited,
    viewed_products,
    user_id,
    device_type,
    utm_source,
    utm_medium,
    utm_campaign,
    has_converted,
    processing_date,
    current_timestamp()     as dbt_updated_at
from with_conversion
