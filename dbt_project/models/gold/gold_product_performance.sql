{{
    config(
        materialized='table',
        tags=['gold']
    )
}}

with order_items as (
    select
        oi.product_id,
        oi.product_name,
        oi.category,
        oi.quantity,
        oi.item_revenue,
        oi.unit_price,
        oi.discount_pct,
        oi.is_discounted,
        oi.order_id,
        so.event_timestamp,
        so.user_id,
        so.processing_date
    from {{ ref('silver_order_items') }} oi
    inner join {{ ref('silver_orders') }} so using (order_id)
),

orders_statuses as (
    select
        order_id,
        new_status
    from {{ ref('silver_orders') }}
    where payment_method is not null
),

inventory_avg as (
    select
        product_id,
        avg(new_quantity) as avg_stock_quantity
    from {{ ref('silver_inventory') }}
    group by product_id
),

windowed as (
    select
        product_id,
        product_name,
        category,

        sum(quantity)       filter (where event_timestamp >= current_date - interval '7 days')   as units_sold_7d,
        sum(item_revenue)   filter (where event_timestamp >= current_date - interval '7 days')   as revenue_7d,
        count(distinct order_id) filter (where event_timestamp >= current_date - interval '7 days') as order_count_7d,
        count(distinct user_id)  filter (where event_timestamp >= current_date - interval '7 days') as unique_buyers_7d,

        sum(quantity)       filter (where event_timestamp >= current_date - interval '30 days')  as units_sold_30d,
        sum(item_revenue)   filter (where event_timestamp >= current_date - interval '30 days')  as revenue_30d,
        count(distinct order_id) filter (where event_timestamp >= current_date - interval '30 days') as order_count_30d,
        count(distinct user_id)  filter (where event_timestamp >= current_date - interval '30 days') as unique_buyers_30d,

        sum(quantity)       as units_sold_all,
        sum(item_revenue)   as revenue_all,
        count(distinct order_id) as order_count_all,
        count(distinct user_id)  as unique_buyers_all,

        avg(unit_price) filter (where discount_pct < 1.0)   as avg_selling_price,
        {{ safe_divide('sum(quantity::float) filter (where is_discounted)', 'count(*)') }} as discount_rate,
        count(*)                                             as _total_items
    from order_items
    group by product_id, product_name, category
),

total_revenue_30d as (
    select sum(item_revenue) as total_rev
    from order_items
    where event_timestamp >= current_date - interval '30 days'
),

refunded as (
    select
        oi.product_id,
        count(distinct oi.order_id) as refunded_orders
    from order_items oi
    inner join {{ ref('silver_orders') }} so on oi.order_id = so.order_id
    where so.payment_method is not null
    group by oi.product_id
),

current_stock as (
    select
        product_id,
        last_value(new_quantity) over (
            partition by product_id
            order by event_timestamp
            rows between unbounded preceding and unbounded following
        ) as current_stock_qty
    from {{ ref('silver_inventory') }}
),

final as (
    select
        w.product_id,
        w.product_name,
        w.category,

        w.units_sold_7d,
        w.revenue_7d,
        w.order_count_7d,
        w.unique_buyers_7d,

        w.units_sold_30d,
        w.revenue_30d,
        w.order_count_30d,
        w.unique_buyers_30d,

        w.units_sold_all,
        w.revenue_all,
        w.order_count_all,
        w.unique_buyers_all,

        {{ safe_divide('w.revenue_30d', 't.total_rev') }} * 100  as revenue_contribution_pct,
        w.avg_selling_price,
        w.discount_rate,

        {{ safe_divide('coalesce(r.refunded_orders, 0)::float', 'w.order_count_all') }} as return_rate,

        {{ safe_divide('w.units_sold_30d::float', 'coalesce(ia.avg_stock_quantity, 1)') }}
                                                                  as inventory_turnover,

        case
            when coalesce(cs.current_stock_qty, 0) = 0  then 'OUT_OF_STOCK'
            when cs.current_stock_qty < {{ var('low_stock_threshold') }} then 'LOW_STOCK'
            else 'IN_STOCK'
        end                                                       as stock_status,

        current_timestamp()                                       as dbt_updated_at
    from windowed w
    cross join total_revenue_30d t
    left join refunded r        on w.product_id = r.product_id
    left join inventory_avg ia  on w.product_id = ia.product_id
    left join (
        select distinct on (product_id) product_id, current_stock_qty
        from current_stock
    ) cs on w.product_id = cs.product_id
)

select * from final
