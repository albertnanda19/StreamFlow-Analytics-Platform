{{
    config(
        materialized='table',
        tags=['gold']
    )
}}

with daily_base as (
    select
        processing_date                             as revenue_date,
        payment_method,
        device_type,
        device_category,
        user_segment,
        user_country,
        order_id,
        total_amount,
        gross_revenue,
        order_item_count
    from {{ ref('silver_orders') }}
),

daily_items as (
    select
        so.processing_date  as revenue_date,
        oi.category         as product_category,
        oi.item_revenue
    from {{ ref('silver_order_items') }} oi
    inner join {{ ref('silver_orders') }} so using (order_id)
),

daily_totals as (
    select
        revenue_date,
        count(distinct order_id)                    as order_count,
        sum(total_amount)                           as total_revenue,
        avg(total_amount)                           as avg_order_value,
        sum(gross_revenue)                          as gross_revenue
    from daily_base
    group by revenue_date
),

by_payment as (
    select
        revenue_date,
        payment_method,
        sum(total_amount)   as payment_revenue,
        count(order_id)     as payment_orders
    from daily_base
    group by revenue_date, payment_method
),

by_device as (
    select
        revenue_date,
        device_type,
        device_category,
        sum(total_amount)   as device_revenue,
        count(order_id)     as device_orders
    from daily_base
    group by revenue_date, device_type, device_category
),

by_segment as (
    select
        revenue_date,
        user_segment,
        sum(total_amount)   as segment_revenue,
        count(order_id)     as segment_orders
    from daily_base
    group by revenue_date, user_segment
),

by_country as (
    select
        revenue_date,
        user_country,
        sum(total_amount)   as country_revenue,
        count(order_id)     as country_orders
    from daily_base
    group by revenue_date, user_country
),

by_category as (
    select
        revenue_date,
        product_category,
        sum(item_revenue)   as category_revenue
    from daily_items
    group by revenue_date, product_category
),

rolling as (
    select
        dt.revenue_date,
        dt.total_revenue,
        dt.order_count,
        dt.avg_order_value,
        dt.gross_revenue,
        lag(dt.total_revenue, 1) over (order by dt.revenue_date) as prev_day_revenue,
        avg(dt.total_revenue) over (
            order by dt.revenue_date
            rows between 6 preceding and current row
        )                                                          as revenue_7d_avg,
        avg(dt.total_revenue) over (
            order by dt.revenue_date
            rows between 29 preceding and current row
        )                                                          as revenue_30d_avg,
        rank() over (
            order by dt.total_revenue desc
        )                                                          as revenue_rank_90d
    from daily_totals dt
),

final as (
    select
        r.revenue_date,
        r.order_count,
        r.total_revenue,
        r.avg_order_value,
        r.gross_revenue,
        r.prev_day_revenue,
        {{ safe_divide('r.total_revenue - r.prev_day_revenue', 'r.prev_day_revenue') }} * 100
                                                        as dod_growth_pct,
        r.revenue_7d_avg,
        r.revenue_30d_avg,
        r.revenue_rank_90d,
        r.order_count > 0                               as row_count_check,
        current_timestamp()                             as dbt_updated_at
    from rolling r
)

select * from final
