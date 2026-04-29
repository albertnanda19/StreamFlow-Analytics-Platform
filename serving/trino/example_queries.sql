-- ============================================================
-- StreamFlow Analytics — Trino Federated Query Examples
-- Demonstrates cross-source SQL across ClickHouse, PostgreSQL
-- ============================================================

-- Q1: 7-day revenue with enriched product & user data
-- Joins ClickHouse Gold with PostgreSQL reference tables
SELECT
    g.revenue_date,
    g.total_revenue,
    g.order_count,
    g.avg_order_value,
    u.country             AS top_user_country,
    g.dod_growth_pct
FROM clickhouse.gold.gold_daily_revenue g
CROSS JOIN (
    SELECT country, count(*) AS cnt
    FROM postgresql.public.users
    GROUP BY country
    ORDER BY cnt DESC
    LIMIT 1
) u
WHERE g.revenue_date >= current_date - interval '7' day
ORDER BY g.revenue_date DESC;


-- Q2: Cross-source conversion analysis — session data from ClickHouse, user profile from PostgreSQL
SELECT
    s.utm_source                              AS channel,
    count(DISTINCT s.session_id)             AS total_sessions,
    count(DISTINCT CASE WHEN s.has_converted THEN s.session_id END) AS converted_sessions,
    round(
        count(DISTINCT CASE WHEN s.has_converted THEN s.session_id END) * 100.0
        / nullif(count(DISTINCT s.session_id), 0),
    2)                                        AS conversion_rate_pct,
    count(DISTINCT u.user_id)               AS registered_users_in_sessions
FROM clickhouse.silver.silver_sessions s
LEFT JOIN postgresql.public.users u ON s.user_id = cast(u.user_id AS varchar)
WHERE s.session_start >= current_date - interval '30' day
GROUP BY s.utm_source
ORDER BY conversion_rate_pct DESC;


-- Q3: Real-time vs historical revenue comparison
-- Real-time from ClickHouse analytics schema vs Gold aggregations
SELECT
    'realtime_last_hour'                      AS period,
    sum(total_amount)                         AS revenue,
    count(*)                                  AS orders
FROM clickhouse.analytics.orders_realtime
WHERE event_timestamp >= now() - interval '1' hour
UNION ALL
SELECT
    'yesterday_same_hour'                     AS period,
    sum(total_amount)                         AS revenue,
    count(*)                                  AS orders
FROM clickhouse.analytics.orders_realtime
WHERE event_timestamp >= now() - interval '25' hour
  AND event_timestamp < now() - interval '23' hour;


-- Q4: RFM segment revenue contribution — user segments from Gold, product info from PostgreSQL
SELECT
    seg.segment,
    count(DISTINCT seg.user_id)              AS user_count,
    round(avg(seg.monetary), 0)              AS avg_lifetime_value,
    round(avg(seg.frequency), 1)             AS avg_order_frequency,
    round(avg(seg.recency_days), 0)          AS avg_recency_days
FROM clickhouse.gold.gold_user_segments seg
GROUP BY seg.segment
ORDER BY avg_lifetime_value DESC;


-- Q5: Product performance with live inventory — Gold metrics joined with PostgreSQL catalogue
SELECT
    gp.product_id,
    p.name                                    AS product_name,
    p.category,
    gp.revenue_30d,
    gp.units_sold_30d,
    gp.stock_status,
    gp.revenue_contribution_pct,
    gp.discount_rate,
    p.stock_quantity                          AS pg_stock_qty
FROM clickhouse.gold.gold_product_performance gp
LEFT JOIN postgresql.public.products p ON gp.product_id = cast(p.product_id AS varchar)
WHERE gp.revenue_30d > 0
ORDER BY gp.revenue_30d DESC
LIMIT 20;


-- Q6: Funnel stage drop-off by acquisition channel
SELECT
    cf.channel,
    cf.session_date,
    cf.sessions_total,
    cf.sessions_with_product_view,
    cf.sessions_with_cart,
    cf.sessions_with_checkout,
    cf.sessions_converted,
    round(cf.product_view_rate  * 100, 2)    AS product_view_pct,
    round(cf.cart_rate          * 100, 2)    AS cart_pct,
    round(cf.checkout_rate      * 100, 2)    AS checkout_pct,
    round(cf.purchase_rate      * 100, 2)    AS purchase_pct,
    round(cf.overall_conversion_rate * 100, 2) AS overall_cvr_pct
FROM clickhouse.gold.gold_conversion_funnel cf
WHERE cf.session_date >= current_date - interval '7' day
ORDER BY cf.session_date DESC, cf.sessions_total DESC;


-- Q7: End-to-end pipeline latency audit
-- Measures time from event generation to Bronze ingestion
SELECT
    toDate(event_timestamp)                   AS event_date,
    count(*)                                  AS total_records,
    round(avg(
        date_diff('second', event_timestamp, ingestion_timestamp)
    ), 1)                                     AS avg_latency_sec,
    max(
        date_diff('second', event_timestamp, ingestion_timestamp)
    )                                         AS max_latency_sec,
    sum(CASE WHEN
        date_diff('second', event_timestamp, ingestion_timestamp) > 30
        THEN 1 ELSE 0 END)                   AS records_exceeding_sla
FROM clickhouse.bronze.orders
WHERE ingestion_timestamp >= now() - interval '24' hour
GROUP BY toDate(event_timestamp)
ORDER BY event_date DESC;


-- Q8: Data quality cross-layer validation
-- Confirms row counts are consistent across Bronze → Silver → Gold
SELECT
    'bronze_orders'  AS layer,
    count(*)         AS row_count,
    count(DISTINCT order_id) AS unique_orders
FROM clickhouse.bronze.orders
UNION ALL
SELECT
    'silver_orders',
    count(*),
    count(DISTINCT order_id)
FROM clickhouse.silver.silver_orders
UNION ALL
SELECT
    'gold_revenue_rows',
    count(*),
    sum(order_count)
FROM clickhouse.gold.gold_daily_revenue;
