{{
    config(
        materialized='incremental',
        unique_key='event_id',
        on_schema_change='append_new_columns',
        incremental_strategy='delete+insert',
        tags=['silver']
    )
}}

with raw_pv as (
    select *
    from {{ source('bronze', 'pageviews') }}
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
                partition by event_id
                order by ingestion_timestamp desc
            ) as _rn
        from raw_pv
    ) r
    where _rn = 1
),

user_enriched as (
    select
        pv.event_id,
        pv.event_type,
        cast(pv.event_timestamp as timestamp)   as event_timestamp,
        pv.session_id,
        pv.user_id,
        pv.page_url,
        upper(pv.page_type)                     as page_type,
        pv.referrer_url,
        pv.referrer_type,
        pv.product_id,
        pv.search_query,
        pv.device_type,
        pv.user_agent,
        pv.ip_masked,
        pv.duration_seconds,
        pv.utm_source,
        pv.utm_medium,
        pv.utm_campaign,
        pv.hour_of_day,
        pv.day_of_week,
        pv.processing_date,
        pv.ingestion_timestamp,
        u.email     as user_email,
        u.segment   as user_segment
    from deduplicated pv
    left join {{ source('postgres_source', 'users') }} u
        on pv.user_id = u.user_id::varchar
        and pv.user_id is not null
),

with_session_stats as (
    select
        *,
        row_number() over (
            partition by session_id
            order by event_timestamp
        )                                           as session_sequence_number,

        count(*) over (partition by session_id)     as _session_pageview_count
    from user_enriched
),

final as (
    select
        event_id,
        event_type,
        event_timestamp,
        session_id,
        user_id,
        page_url,
        page_type,
        referrer_url,
        referrer_type,
        product_id,
        search_query,
        device_type,
        user_agent,
        ip_masked,
        duration_seconds,
        utm_source,
        utm_medium,
        utm_campaign,
        hour_of_day,
        day_of_week,
        processing_date,
        ingestion_timestamp,
        user_email,
        user_segment,
        session_sequence_number,
        session_sequence_number = 1                 as is_entry_page,
        _session_pageview_count = 1                 as is_bounce,
        current_timestamp()                         as dbt_updated_at
    from with_session_stats
)

select * from final
