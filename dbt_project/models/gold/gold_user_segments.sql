{{
    config(
        materialized='table',
        tags=['gold']
    )
}}

with order_base as (
    select
        user_id,
        order_id,
        total_amount,
        event_timestamp,
        processing_date
    from {{ ref('silver_orders') }}
),

rfm_base as (
    select
        user_id,
        datediff('day', max(event_timestamp), current_timestamp())  as recency_days,
        count(distinct order_id)                                    as frequency,
        sum(total_amount)                                           as monetary,
        avg(total_amount)                                           as avg_order_value,
        max(event_timestamp)                                        as last_order_at,
        min(event_timestamp)                                        as first_order_at
    from order_base
    where event_timestamp >= current_timestamp() - interval '{{ var("rfm_lookback_days") }} days'
    group by user_id
),

rfm_scored as (
    select
        *,
        ntile(5) over (order by recency_days asc)   as r_score,
        ntile(5) over (order by frequency    desc)  as f_score,
        ntile(5) over (order by monetary     desc)  as m_score
    from rfm_base
),

segmented as (
    select
        *,
        r_score::varchar || f_score::varchar || m_score::varchar   as rfm_score,
        case
            when r_score = 5 and f_score >= 4 and m_score >= 4     then 'Champions'
            when f_score >= 3 and m_score >= 3                      then 'Loyal Customers'
            when r_score <= 2 and f_score >= 3                      then 'At Risk'
            when r_score = 1 and f_score <= 2                       then 'Lost'
            when r_score = 5 and f_score = 1                        then 'New Customers'
            when r_score >= 4 and f_score <= 2                      then 'Potential'
            else 'Others'
        end                                                         as segment
    from rfm_scored
)

select
    user_id,
    recency_days,
    frequency,
    monetary,
    avg_order_value,
    last_order_at,
    first_order_at,
    r_score,
    f_score,
    m_score,
    rfm_score,
    segment,
    current_timestamp()     as dbt_updated_at
from segmented
