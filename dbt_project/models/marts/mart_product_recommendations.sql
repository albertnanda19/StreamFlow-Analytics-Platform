{{
    config(
        materialized='table',
        tags=['mart']
    )
}}

with item_pairs as (
    select
        a.order_id,
        a.product_id                    as product_id_a,
        a.product_name                  as product_name_a,
        b.product_id                    as product_id_b,
        b.product_name                  as product_name_b
    from {{ ref('silver_order_items') }} a
    inner join {{ ref('silver_order_items') }} b
        on  a.order_id = b.order_id
        and a.product_id < b.product_id
),

pair_counts as (
    select
        product_id_a,
        product_name_a,
        product_id_b,
        product_name_b,
        count(distinct order_id)        as co_occurrence_count
    from item_pairs
    group by product_id_a, product_name_a, product_id_b, product_name_b
    having count(distinct order_id) >= 10
),

product_order_counts as (
    select
        product_id,
        count(distinct order_id)        as total_orders
    from {{ ref('silver_order_items') }}
    group by product_id
),

final as (
    select
        pc.product_id_a,
        pc.product_name_a,
        pc.product_id_b,
        pc.product_name_b,
        pc.co_occurrence_count,
        pa.total_orders                 as orders_with_product_a,
        pb.total_orders                 as orders_with_product_b,
        {{ safe_divide('pc.co_occurrence_count::float', 'pa.total_orders') }}
                                        as confidence_a_to_b,
        {{ safe_divide('pc.co_occurrence_count::float', 'pb.total_orders') }}
                                        as confidence_b_to_a,
        current_timestamp()             as dbt_updated_at
    from pair_counts pc
    inner join product_order_counts pa on pa.product_id = pc.product_id_a
    inner join product_order_counts pb on pb.product_id = pc.product_id_b
    where {{ safe_divide('pc.co_occurrence_count::float', 'pa.total_orders') }} > 0.1
)

select * from final
order by co_occurrence_count desc
