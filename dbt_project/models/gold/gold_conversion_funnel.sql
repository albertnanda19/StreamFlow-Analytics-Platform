{{
    config(
        materialized='table',
        tags=['gold']
    )
}}

with sessions as (
    select
        session_id,
        session_start::date         as session_date,
        utm_source,
        utm_medium,
        utm_campaign,
        has_converted,
        pages_visited,
        array_contains(pages_visited::varchar[], 'PRODUCT_DETAIL')  as viewed_product,
        array_contains(pages_visited::varchar[], 'CART')            as reached_cart,
        array_contains(pages_visited::varchar[], 'CHECKOUT')        as reached_checkout
    from {{ ref('silver_sessions') }}
),

funnel as (
    select
        session_date,
        coalesce(utm_source, 'direct')                  as channel,
        count(distinct session_id)                      as sessions_total,

        count(distinct session_id) filter (
            where viewed_product
        )                                               as sessions_with_product_view,

        count(distinct session_id) filter (
            where reached_cart
        )                                               as sessions_with_cart,

        count(distinct session_id) filter (
            where reached_checkout
        )                                               as sessions_with_checkout,

        count(distinct session_id) filter (
            where has_converted
        )                                               as sessions_converted
    from sessions
    group by session_date, channel
),

final as (
    select
        session_date,
        channel,
        sessions_total,
        sessions_with_product_view,
        sessions_with_cart,
        sessions_with_checkout,
        sessions_converted,

        {{ safe_divide('sessions_with_product_view::float', 'sessions_total') }}
                                                        as product_view_rate,
        {{ safe_divide('sessions_with_cart::float', 'sessions_with_product_view') }}
                                                        as cart_rate,
        {{ safe_divide('sessions_with_checkout::float', 'sessions_with_cart') }}
                                                        as checkout_rate,
        {{ safe_divide('sessions_converted::float', 'sessions_with_checkout') }}
                                                        as purchase_rate,
        {{ safe_divide('sessions_converted::float', 'sessions_total') }}
                                                        as overall_conversion_rate,

        current_timestamp()                             as dbt_updated_at
    from funnel
)

select * from final
