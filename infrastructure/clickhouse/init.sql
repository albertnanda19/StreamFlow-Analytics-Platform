CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.orders_realtime
(
    order_id        String,
    user_id         String,
    product_id      String,
    event_type      String,
    status          String,
    total_amount    Float64,
    quantity        Int32,
    unit_price      Float64,
    country         String,
    user_segment    String,
    category        String,
    event_timestamp DateTime,
    processing_time DateTime DEFAULT now(),
    _ingested_at    DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (event_timestamp, user_id, order_id)
TTL event_timestamp + INTERVAL 6 MONTH
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.daily_revenue
(
    date            Date,
    country         String,
    user_segment    String,
    category        String,
    total_revenue   Float64,
    total_orders    Int64,
    avg_order_value Float64,
    unique_users    Int64,
    _updated_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, country, user_segment, category)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.product_performance
(
    product_id      String,
    product_name    String,
    category        String,
    date            Date,
    units_sold      Int64,
    revenue         Float64,
    avg_unit_price  Float64,
    unique_buyers   Int64,
    _updated_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, product_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.user_behavior
(
    user_id             String,
    user_segment        String,
    country             String,
    date                Date,
    total_orders        Int64,
    total_spend         Float64,
    avg_order_value     Float64,
    cancelled_orders    Int64,
    conversion_rate     Float64,
    _updated_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, user_id)
SETTINGS index_granularity = 8192;
