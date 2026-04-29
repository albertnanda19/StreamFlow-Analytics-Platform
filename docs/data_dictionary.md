# StreamFlow Data Dictionary

## Silver Layer

### `silver.silver_orders`
Deduplicated and user-enriched order placement events. One row per unique order.

| Column | Type | Description | Example | PII |
|---|---|---|---|---|
| `order_id` | UUID | Primary key — unique order identifier | `a2f3e4b5-...` | No |
| `user_id` | UUID | Purchasing user identifier | `c3d4e5f6-...` | No |
| `session_id` | UUID | Browser session when order was placed | `b1c2d3e4-...` | No |
| `event_type` | String | Always `order_placed` | `order_placed` | No |
| `event_timestamp` | Timestamp | When the order was placed (WIB) | `2024-03-15 14:23:01` | No |
| `payment_method` | Enum | Payment channel used | `EWALLET` | No |
| `subtotal` | Float | Sum of item prices before shipping (IDR) | `450000.00` | No |
| `shipping_fee` | Float | Shipping cost (IDR) | `25000.00` | No |
| `total_amount` | Float | subtotal + shipping_fee (IDR) | `475000.00` | No |
| `gross_revenue` | Float | total_amount - shipping_fee (IDR) | `450000.00` | No |
| `items` | Array | Raw item array from Bronze | `[{product_id, quantity, ...}]` | No |
| `shipping_address` | JSON | Delivery address object | `{"city": "Jakarta Selatan", ...}` | Yes |
| `device_type` | Enum | Device used to place order | `MOBILE` | No |
| `device_category` | Enum | Simplified device group | `MOBILE` | No |
| `platform` | String | App or web | `android_app` | No |
| `coupon_code` | String | Discount code applied (nullable) | `SALE20` | No |
| `user_email` | String | Enriched from PostgreSQL | `user@example.com` | **Yes** |
| `user_country` | String | Enriched from PostgreSQL | `Indonesia` | No |
| `user_segment` | Enum | Marketing segment | `VIP` | No |
| `order_item_count` | Int | Number of distinct products | `3` | No |
| `has_discount` | Boolean | Any item had discount_pct > 0 | `true` | No |
| `is_high_value` | Boolean | total_amount > IDR 500,000 | `false` | No |
| `hour_of_day` | Int | Hour extracted from event_timestamp | `14` | No |
| `day_of_week` | Int | Day of week (0=Sunday) | `5` | No |
| `ingestion_timestamp` | Timestamp | When Spark wrote to Bronze | `2024-03-15 14:23:15` | No |
| `processing_date` | Date | Partitioning date | `2024-03-15` | No |
| `data_source` | String | Always `kafka_orders` | `kafka_orders` | No |
| `dbt_updated_at` | Timestamp | Last dbt processing time | `2024-03-15 15:00:45` | No |

---

### `silver.silver_order_items`
Exploded line items — one row per product per order.

| Column | Type | Description | Example | PII |
|---|---|---|---|---|
| `item_id` | String | Surrogate key MD5(order_id+product_id+idx) | `a1b2c3...` | No |
| `order_id` | UUID | Foreign key to silver_orders | `a2f3e4b5-...` | No |
| `user_id` | UUID | Purchasing user | `c3d4e5f6-...` | No |
| `item_index` | Int | Position in original items array | `0` | No |
| `product_id` | UUID | Product identifier | `d4e5f6a7-...` | No |
| `product_name` | String | Enriched from product catalogue | `iPhone 15 Pro` | No |
| `category` | String | Product category | `Electronics` | No |
| `base_price` | Float | Catalogue price (IDR) | `15999000.00` | No |
| `quantity` | Int | Units purchased | `1` | No |
| `unit_price` | Float | Actual selling price (IDR) | `14399000.00` | No |
| `discount_pct` | Float | Discount fraction (0.0 = no discount) | `0.10` | No |
| `item_revenue` | Float | quantity × unit_price × (1 - discount_pct) | `12959100.00` | No |
| `discount_amount` | Float | Revenue foregone from discount | `1439900.00` | No |
| `is_discounted` | Boolean | discount_pct > 0 | `true` | No |
| `event_timestamp` | Timestamp | Inherited from parent order | `2024-03-15 14:23:01` | No |
| `processing_date` | Date | Partitioning date | `2024-03-15` | No |
| `dbt_updated_at` | Timestamp | Last dbt processing time | `2024-03-15 15:00:45` | No |

---

### `silver.silver_pageviews`
Cleaned clickstream events with session sequencing.

| Column | Type | Description | Example | PII |
|---|---|---|---|---|
| `event_id` | UUID | Primary key | `e1f2a3b4-...` | No |
| `session_id` | UUID | Browser session | `f2a3b4c5-...` | No |
| `user_id` | UUID | Logged-in user (nullable for anonymous) | `c3d4e5f6-...` | No |
| `page_type` | Enum | Standardised page category | `PRODUCT_DETAIL` | No |
| `page_url` | String | Full URL path | `/products/iphone-15-pro` | No |
| `referrer_url` | String | Previous page URL | `https://google.com` | No |
| `referrer_type` | Enum | Traffic source type | `ORGANIC_SEARCH` | No |
| `product_id` | UUID | Product viewed (nullable) | `d4e5f6a7-...` | No |
| `search_query` | String | Search term (for SEARCH pages) | `iphone` | No |
| `device_type` | Enum | Device used | `DESKTOP` | No |
| `user_agent` | String | Browser user agent | `Mozilla/5.0...` | Yes |
| `ip_masked` | String | Last octet masked | `192.168.1.xxx` | Partial |
| `duration_seconds` | Int | Time on page (seconds) | `45` | No |
| `utm_source` | String | UTM source parameter | `google` | No |
| `utm_medium` | String | UTM medium parameter | `cpc` | No |
| `utm_campaign` | String | UTM campaign parameter | `spring_sale_2024` | No |
| `session_sequence_number` | Int | Rank within session (1 = entry page) | `1` | No |
| `is_entry_page` | Boolean | First page of the session | `true` | No |
| `is_bounce` | Boolean | Session had only this one pageview | `false` | No |
| `user_email` | String | Enriched from PostgreSQL | `user@example.com` | **Yes** |
| `user_segment` | Enum | Marketing segment | `REGULAR` | No |

---

### `silver.silver_sessions`
Session-level aggregations of pageview events.

| Column | Type | Description | Example | PII |
|---|---|---|---|---|
| `session_id` | UUID | Primary key | `f2a3b4c5-...` | No |
| `session_start` | Timestamp | First pageview time | `2024-03-15 14:10:00` | No |
| `session_end` | Timestamp | Last pageview time | `2024-03-15 14:23:00` | No |
| `session_duration_seconds` | Int | session_end - session_start | `780` | No |
| `page_count` | Int | Total pageviews in session | `8` | No |
| `unique_pages_visited` | Int | Distinct URLs visited | `6` | No |
| `is_bounce` | Boolean | Session had only 1 pageview | `false` | No |
| `pages_visited` | Array | Ordered list of page_type values | `[HOME, CATEGORY, PRODUCT_DETAIL, CART]` | No |
| `viewed_products` | Array | Product IDs viewed | `[uuid1, uuid2]` | No |
| `user_id` | UUID | Logged-in user (nullable) | `c3d4e5f6-...` | No |
| `device_type` | Enum | Mode device type for session | `MOBILE` | No |
| `utm_source` | String | Acquisition channel | `instagram` | No |
| `utm_campaign` | String | Campaign name | `ramadan_sale` | No |
| `has_converted` | Boolean | Session resulted in an order | `true` | No |

---

## Gold Layer

### `gold.gold_daily_revenue`
Pre-aggregated daily revenue metrics with rolling averages.

| Column | Type | Description | Example | PII |
|---|---|---|---|---|
| `revenue_date` | Date | Primary key | `2024-03-15` | No |
| `order_count` | Int | Total orders placed | `1243` | No |
| `total_revenue` | Float | Sum of total_amount (IDR) | `623500000.00` | No |
| `avg_order_value` | Float | Average order size (IDR) | `501609.81` | No |
| `gross_revenue` | Float | Revenue excluding shipping | `573500000.00` | No |
| `prev_day_revenue` | Float | Yesterday's total_revenue | `598000000.00` | No |
| `dod_growth_pct` | Float | Day-over-day growth (%) | `4.27` | No |
| `revenue_7d_avg` | Float | 7-day rolling average (IDR) | `611000000.00` | No |
| `revenue_30d_avg` | Float | 30-day rolling average (IDR) | `588000000.00` | No |
| `revenue_rank_90d` | Int | Revenue rank within last 90 days | `3` | No |
| `dbt_updated_at` | Timestamp | Last dbt processing time | `2024-03-16 01:45:00` | No |

### `gold.gold_user_segments`
RFM (Recency-Frequency-Monetary) segmentation for all active users.

| Column | Type | Description | Example | PII |
|---|---|---|---|---|
| `user_id` | UUID | Primary key | `c3d4e5f6-...` | No |
| `recency_days` | Int | Days since last order | `3` | No |
| `frequency` | Int | Total orders in lookback window | `12` | No |
| `monetary` | Float | Total spend in lookback window (IDR) | `4850000.00` | No |
| `avg_order_value` | Float | Average order size (IDR) | `404166.67` | No |
| `last_order_at` | Timestamp | Most recent order timestamp | `2024-03-12 18:30:00` | No |
| `first_order_at` | Timestamp | First-ever order timestamp | `2023-06-01 10:15:00` | No |
| `r_score` | Int | Recency score 1-5 (5 = most recent) | `5` | No |
| `f_score` | Int | Frequency score 1-5 (5 = most orders) | `4` | No |
| `m_score` | Int | Monetary score 1-5 (5 = highest spend) | `3` | No |
| `rfm_score` | String | Concatenated RFM scores | `543` | No |
| `segment` | Enum | Business segment label | `Champions` | No |
| `dbt_updated_at` | Timestamp | Last dbt processing time | `2024-03-16 01:45:00` | No |

**Segment definitions:**

| Segment | Criteria |
|---|---|
| Champions | R=5, F≥4, M≥4 |
| Loyal Customers | F≥3, M≥3 |
| At Risk | R≤2, F≥3 |
| Lost | R=1, F≤2 |
| New Customers | R=5, F=1 |
| Potential | R≥4, F≤2 |
| Others | All remaining |
