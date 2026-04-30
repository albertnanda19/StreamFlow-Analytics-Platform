from __future__ import annotations

import io
import struct
from typing import Dict, Optional

import fastavro
import requests
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

_schema_cache: Dict[int, dict] = {}


def _fetch_schema(schema_registry_url: str, schema_id: int) -> dict:
    if schema_id in _schema_cache:
        return _schema_cache[schema_id]

    url = f"{schema_registry_url}/schemas/ids/{schema_id}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    schema_str = resp.json()["schema"]
    parsed = fastavro.parse_schema(
        __import__("json").loads(schema_str)
    )
    _schema_cache[schema_id] = parsed
    return parsed


def _avro_default(obj):
    import datetime
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    return str(obj)


def _deserialize_confluent(data: bytes, schema_registry_url: str) -> Optional[dict]:
    if not data or len(data) < 5:
        return None

    magic = data[0]
    if magic != 0:
        return None

    schema_id = struct.unpack(">I", data[1:5])[0]
    payload   = data[5:]

    schema = _fetch_schema(schema_registry_url, schema_id)
    reader = io.BytesIO(payload)
    return fastavro.schemaless_reader(reader, schema)


def make_avro_deserializer_udf(schema_registry_url: str):
    import json as _json

    def _deserialize(data: bytes) -> Optional[str]:
        try:
            record = _deserialize_confluent(data, schema_registry_url)
            return _json.dumps(record, default=_avro_default) if record else None
        except Exception:
            return None

    return udf(_deserialize, StringType())


def deserialize_confluent_bytes(data: bytes, schema_registry_url: str) -> Optional[dict]:
    return _deserialize_confluent(data, schema_registry_url)
