import logging
import os

import requests
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


class ClickHouseHook(BaseHook):
    conn_name_attr    = "clickhouse_conn_id"
    default_conn_name = "clickhouse_http"
    conn_type         = "http"
    hook_name         = "ClickHouse"

    def __init__(self, clickhouse_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.clickhouse_conn_id = clickhouse_conn_id
        self._conn = None

    def get_conn(self):
        if self._conn:
            return self._conn
        conn = self.get_connection(self.clickhouse_conn_id)
        try:
            import clickhouse_driver
            self._conn = clickhouse_driver.Client(
                host=conn.host or os.getenv("CLICKHOUSE_HOST", "clickhouse"),
                port=9000,
                user=conn.login or "streamflow",
                password=conn.password or "",
                database=conn.schema or "analytics",
            )
        except ImportError:
            self._conn = {
                "host":     conn.host,
                "port":     conn.port or 8123,
                "login":    conn.login,
                "password": conn.password,
            }
        return self._conn

    def execute_query(self, sql: str, params: dict | None = None):
        client = self.get_conn()
        if hasattr(client, "execute"):
            return client.execute(sql, params or {})
        conn_info = client
        resp = requests.get(
            f"http://{conn_info['host']}:{conn_info['port']}/",
            params={"query": sql, "user": conn_info["login"], "password": conn_info["password"]},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.text

    def insert_dataframe(self, table: str, df) -> int:
        client = self.get_conn()
        if not hasattr(client, "execute"):
            raise NotImplementedError("insert_dataframe requires clickhouse_driver to be installed")
        records = df.to_dict("records")
        if not records:
            return 0
        columns = list(records[0].keys())
        col_str = ", ".join(columns)
        client.execute(
            f"INSERT INTO {table} ({col_str}) VALUES",
            records,
        )
        logger.info("Inserted %d rows into %s", len(records), table)
        return len(records)
