{{
    config(
        materialized='table',
        tags=['mart']
    )
}}

with today_orders as (
    select
        count(distinct order_id)    as today_orders,
        sum(total_amount)           as today_revenue,
        count(distinct user_id)     as today_active_users
    from {{ ref('silver_orders') }}
    where processing_date = current_date
),

yesterday_orders as (
    select sum(total_amount) as yesterday_revenue
    from {{ ref('silver_orders') }}
    where processing_date = current_date - interval '1 day'
),

wow_base as (
    select sum(total_amount) as last_week_revenue
    from {{ ref('silver_orders') }}
    where processing_date between current_date - interval '14 days'
                              and current_date - interval '8 days'
),

mtd as (
    select sum(total_amount) as mtd_revenue
    from {{ ref('silver_orders') }}
    where date_trunc('month', processing_date) = date_trunc('month', current_date)
),

ytd as (
    select sum(total_amount) as ytd_revenue
    from {{ ref('silver_orders') }}
    where date_trunc('year', processing_date) = date_trunc('year', current_date)
),

new_users as (
    select count(distinct user_id) as today_new_users
    from {{ ref('silver_orders') }}
    where processing_date = current_date
      and user_segment = 'NEW'
),

active_products as (
    select count(distinct product_id) as active_products_today
    from {{ ref('silver_order_items') }} oi
    inner join {{ ref('silver_orders') }} so using (order_id)
    where so.processing_date = current_date
),

top_category as (
    select category as top_category_today
    from {{ ref('silver_order_items') }} oi
    inner join {{ ref('silver_orders') }} so using (order_id)
    where so.processing_date = current_date
    group by category
    order by sum(item_revenue) desc
    limit 1
),

funnel_today as (
    select overall_conversion_rate as today_conversion_rate
    from {{ ref('gold_conversion_funnel') }}
    where session_date = current_date
      and channel = 'direct'
    limit 1
)

select
    current_date                                                    as summary_date,

    today_orders.today_revenue,
    today_orders.today_orders,
    new_users.today_new_users,
    today_orders.today_active_users,

    yesterday_orders.yesterday_revenue,
    {{ safe_divide(
        'today_orders.today_revenue - yesterday_orders.yesterday_revenue',
        'yesterday_orders.yesterday_revenue'
    ) }} * 100                                                      as dod_growth_pct,

    {{ safe_divide(
        'today_orders.today_revenue - wow_base.last_week_revenue',
        'wow_base.last_week_revenue'
    ) }} * 100                                                      as wow_growth_pct,

    mtd.mtd_revenue,
    ytd.ytd_revenue,
    active_products.active_products_today,
    top_category.top_category_today,
    coalesce(funnel_today.today_conversion_rate, 0)                 as today_conversion_rate,
    current_timestamp()                                             as dbt_updated_at
from today_orders
cross join yesterday_orders
cross join wow_base
cross join mtd
cross join ytd
cross join new_users
cross join active_products
cross join top_category
left join funnel_today on true
