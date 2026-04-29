import asyncio
import logging
import os
from datetime import date, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional

import psycopg2
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CH_USER = os.getenv("CLICKHOUSE_USER", "streamflow")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "streamflow123")
CH_DB   = os.getenv("CLICKHOUSE_DB", "analytics")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_USER = os.getenv("POSTGRES_USER", "streamflow")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "streamflow123")
PG_DB   = os.getenv("POSTGRES_SOURCE_DB", "streamflow_source")

app = FastAPI(
    title="StreamFlow Analytics API",
    description="Serving layer for the StreamFlow real-time e-commerce analytics platform",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


def _ch_client():
    import clickhouse_driver
    return clickhouse_driver.Client(
        host=CH_HOST, port=CH_PORT,
        user=CH_USER, password=CH_PASS,
        database=CH_DB,
    )


def _pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASS,
        dbname=PG_DB,
    )


def _ch_query(sql: str) -> List[Dict]:
    client = _ch_client()
    rows   = client.execute(sql, with_column_types=True)
    data, cols = rows[0], rows[1]
    col_names = [c[0] for c in cols]
    return [dict(zip(col_names, row)) for row in data]


@app.get("/health", tags=["System"])
def health():
    checks: Dict[str, Any] = {"status": "healthy", "components": {}}
    try:
        _ch_client().execute("SELECT 1")
        checks["components"]["clickhouse"] = "running"
    except Exception as exc:
        checks["components"]["clickhouse"] = f"error: {exc}"
        checks["status"] = "degraded"

    try:
        conn = _pg_conn()
        conn.close()
        checks["components"]["postgresql"] = "running"
    except Exception as exc:
        checks["components"]["postgresql"] = f"error: {exc}"
        checks["status"] = "degraded"

    try:
        conn = _pg_conn()
        cur  = conn.cursor()
        cur.execute("SELECT recorded_at FROM pipeline_runs ORDER BY recorded_at DESC LIMIT 1")
        row = cur.fetchone()
        checks["last_pipeline_run"] = str(row[0]) if row else "unknown"
        conn.close()
    except Exception:
        checks["last_pipeline_run"] = "unknown"

    return checks


@app.get("/metrics/revenue", tags=["Metrics"])
def revenue(query_date: Optional[str] = Query(None, alias="date", description="YYYY-MM-DD")):
    target_date = query_date or str(date.today())
    try:
        date.fromisoformat(target_date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format — use YYYY-MM-DD")

    total = _ch_query(f"""
        SELECT
            sum(total_amount)  AS total_revenue,
            count()            AS order_count,
            round(avg(total_amount), 0) AS avg_order_value
        FROM analytics.orders_realtime
        WHERE toDate(event_timestamp) = '{target_date}'
    """)

    by_payment = _ch_query(f"""
        SELECT payment_method, sum(total_amount) AS revenue, count() AS orders
        FROM analytics.orders_realtime
        WHERE toDate(event_timestamp) = '{target_date}'
        GROUP BY payment_method ORDER BY revenue DESC
    """)

    by_category = _ch_query(f"""
        SELECT category, sum(item_revenue) AS revenue
        FROM silver.silver_order_items oi
        INNER JOIN silver.silver_orders so ON oi.order_id = so.order_id
        WHERE toDate(so.event_timestamp) = '{target_date}'
        GROUP BY category ORDER BY revenue DESC
    """)

    base = total[0] if total else {}
    return {
        "date":           target_date,
        "total_revenue":  base.get("total_revenue", 0),
        "order_count":    base.get("order_count", 0),
        "avg_order_value": base.get("avg_order_value", 0),
        "by_payment_method": {r["payment_method"]: r["revenue"] for r in by_payment},
        "by_category":    {r["category"]: r["revenue"] for r in by_category},
    }


@app.get("/metrics/products/top", tags=["Metrics"])
def top_products(limit: int = Query(10, ge=1, le=100), days: int = Query(7, ge=1, le=90)):
    rows = _ch_query(f"""
        SELECT
            oi.product_id,
            oi.product_name,
            oi.category,
            sum(oi.quantity)     AS units_sold,
            sum(oi.item_revenue) AS revenue,
            round(avg(oi.unit_price), 0) AS avg_price
        FROM silver.silver_order_items oi
        INNER JOIN silver.silver_orders so ON oi.order_id = so.order_id
        WHERE so.event_timestamp >= now() - INTERVAL {days} DAY
        GROUP BY oi.product_id, oi.product_name, oi.category
        ORDER BY revenue DESC
        LIMIT {limit}
    """)
    return {"days": days, "limit": limit, "products": rows}


@app.get("/metrics/funnel", tags=["Metrics"])
def funnel(
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date:   str = Query(..., description="YYYY-MM-DD"),
):
    try:
        date.fromisoformat(start_date)
        date.fromisoformat(end_date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format — use YYYY-MM-DD")

    rows = _ch_query(f"""
        SELECT
            sum(sessions_total)              AS sessions,
            sum(sessions_with_product_view)  AS product_views,
            sum(sessions_with_cart)          AS cart,
            sum(sessions_with_checkout)      AS checkout,
            sum(sessions_converted)          AS orders,
            round(avg(overall_conversion_rate) * 100, 2) AS avg_conversion_rate_pct
        FROM gold.gold_conversion_funnel
        WHERE session_date BETWEEN '{start_date}' AND '{end_date}'
    """)
    base = rows[0] if rows else {}
    return {"start_date": start_date, "end_date": end_date, "funnel": base}


@app.get("/metrics/users/segments", tags=["Metrics"])
def user_segments():
    rows = _ch_query("""
        SELECT
            segment,
            count()                           AS user_count,
            round(avg(monetary), 0)           AS avg_lifetime_value,
            round(avg(frequency), 1)          AS avg_order_frequency,
            round(avg(recency_days), 0)       AS avg_recency_days
        FROM gold.gold_user_segments
        GROUP BY segment
        ORDER BY avg_lifetime_value DESC
    """)
    total_users = sum(r.get("user_count", 0) for r in rows)
    for r in rows:
        r["pct_of_total"] = round(r["user_count"] / max(total_users, 1) * 100, 2)
    return {"total_users": total_users, "segments": rows}


@app.get("/pipeline/status", tags=["Operations"])
def pipeline_status():
    services: Dict[str, str] = {}

    try:
        _ch_client().execute("SELECT 1")
        services["clickhouse"] = "running"
    except Exception as exc:
        services["clickhouse"] = f"error: {exc}"

    try:
        import requests
        r = requests.get("http://kafka:9092", timeout=3)
        services["kafka"] = "running"
    except Exception:
        services["kafka"] = "unreachable"

    try:
        conn = _pg_conn()
        cur  = conn.cursor()
        cur.execute("SELECT status FROM data_quality_reports ORDER BY check_time DESC LIMIT 1")
        row = cur.fetchone()
        services["data_quality"] = row[0] if row else "no_data"
        conn.close()
    except Exception as exc:
        services["data_quality"] = f"error: {exc}"

    overall = "healthy" if all(v == "running" or v in ("PASS", "WARN") for v in services.values()) else "degraded"
    return {"overall_status": overall, "components": services}
