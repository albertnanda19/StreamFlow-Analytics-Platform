import io
import struct

import fastavro
import requests
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

_schema_cache: dict[int, dict] = {}


def _fetch_schema(schema_registry_url: str, schema_id: int) -> dict:
    if schema_id in _schema_cache:
        return _schema_cache[schema_id]

    url = f"{schema_registry_url}/schemas/ids/{schema_id}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    schema_str = resp.json()["schema"]
    parsed = fastavro.parse_schema(fastavro.schema.parse_schema(
        __import__("json").loads(schema_str)
    ))
    _schema_cache[schema_id] = parsed
    return parsed


def _deserialize_confluent(data: bytes, schema_registry_url: str) -> dict | None:
    if not data or len(data) < 5:
        return None

    magic = data[0]
    if magic != 0:
        return None

    schema_id = struct.unpack(">I", data[1:5])[0]
    payload   = data[5:]

    schema = _fetch_schema(schema_registry_url, schema_id)
    reader = io.BytesIO(payload)
    return fastavro.read.schemaless_reader(reader, schema)


def make_avro_deserializer_udf(schema_registry_url: str):
    import json as _json

    def _deserialize(data: bytes) -> str | None:
        try:
            record = _deserialize_confluent(data, schema_registry_url)
            return _json.dumps(record, default=str) if record else None
        except Exception:
            return None

    return udf(_deserialize, StringType())


def deserialize_confluent_bytes(data: bytes, schema_registry_url: str) -> dict | None:
    return _deserialize_confluent(data, schema_registry_url)
