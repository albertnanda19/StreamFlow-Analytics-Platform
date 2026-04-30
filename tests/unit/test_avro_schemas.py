import io
import json
import os
import pytest


SCHEMA_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "producer", "schemas"
)

SCHEMA_FILES = [
    "order_placed.avsc",
    "order_status_updated.avsc",
    "pageview.avsc",
    "inventory_update.avsc",
]


def _load_schema(filename: str):
    from fastavro.schema import parse_schema
    path = os.path.join(SCHEMA_DIR, filename)
    with open(path) as f:
        raw = json.load(f)
    return parse_schema(raw)


class TestAvroSchemaValidity:

    @pytest.mark.unit
    @pytest.mark.parametrize("schema_file", SCHEMA_FILES)
    def test_schema_file_is_valid_avro(self, schema_file):
        from fastavro.schema import parse_schema
        path = os.path.join(SCHEMA_DIR, schema_file)
        with open(path) as f:
            raw = json.load(f)
        parsed = parse_schema(raw)
        assert parsed is not None

    @pytest.mark.unit
    def test_order_placed_schema_has_required_fields(self):
        schema = _load_schema("order_placed.avsc")
        field_names = {f["name"] for f in schema["fields"]}
        required = {
            "event_id", "order_id", "user_id", "items",
            "total_amount", "payment_method", "event_timestamp",
            "event_type", "subtotal", "shipping_fee",
        }
        missing = required - field_names
        assert missing == set(), f"Schema missing fields: {missing}"

    @pytest.mark.unit
    def test_pageview_schema_has_required_fields(self):
        schema = _load_schema("pageview.avsc")
        field_names = {f["name"] for f in schema["fields"]}
        required = {
            "event_id", "event_type", "event_timestamp", "session_id",
            "page_url", "page_type", "device_type", "duration_seconds",
        }
        missing = required - field_names
        assert missing == set(), f"Pageview schema missing fields: {missing}"

    @pytest.mark.unit
    def test_inventory_schema_has_required_fields(self):
        schema = _load_schema("inventory_update.avsc")
        field_names = {f["name"] for f in schema["fields"]}
        required = {
            "event_id", "product_id", "warehouse_id",
            "previous_quantity", "new_quantity", "change_reason",
        }
        missing = required - field_names
        assert missing == set(), f"Inventory schema missing fields: {missing}"


class TestAvroSerializationRoundtrip:

    @pytest.mark.unit
    def test_order_event_roundtrip_via_schemaless(self):
        from fastavro import schemaless_writer, schemaless_reader
        from deepdiff import DeepDiff

        schema = _load_schema("order_placed.avsc")
        record = {
            "event_id": "880e8400-e29b-41d4-a716-446655440001",
            "event_type": "order_placed",
            "event_timestamp": 1705309200000,
            "order_id": "770e8400-e29b-41d4-a716-446655440001",
            "user_id": "550e8400-e29b-41d4-a716-446655440001",
            "session_id": "990e8400-e29b-41d4-a716-446655440001",
            "items": [
                {
                    "product_id": "660e8400-e29b-41d4-a716-446655440001",
                    "product_name": "Laptop Gaming ASUS ROG",
                    "category": "Electronics",
                    "quantity": 1,
                    "unit_price": 15_000_000.0,
                    "discount_pct": 0.0,
                }
            ],
            "subtotal": 15_000_000.0,
            "shipping_fee": 0.0,
            "total_amount": 15_000_000.0,
            "payment_method": "CREDIT_CARD",
            "shipping_address": {
                "city": "Jakarta Selatan",
                "province": "DKI Jakarta",
                "country": "ID",
                "postal_code": "12190",
            },
            "device_type": "DESKTOP",
            "platform": "WEB",
            "coupon_code": None,
            "metadata": {"source": "test-suite"},
        }
        buf = io.BytesIO()
        schemaless_writer(buf, schema, record)
        buf.seek(0)
        result = schemaless_reader(buf, schema)
        diff = DeepDiff(
            record,
            result,
            significant_digits=2,
            ignore_type_in_groups=[(type(None), str)],
            exclude_paths=["root['event_timestamp']"],
        )
        assert diff == {}, f"Roundtrip mismatch: {diff}"

    @pytest.mark.unit
    def test_null_union_field_accepts_none(self):
        from fastavro import schemaless_writer
        schema = _load_schema("order_placed.avsc")
        record = {
            "event_id": "880e8400-e29b-41d4-a716-446655440099",
            "event_type": "order_placed",
            "event_timestamp": 1705309200000,
            "order_id": "770e8400-e29b-41d4-a716-446655440099",
            "user_id": "550e8400-e29b-41d4-a716-446655440001",
            "session_id": "990e8400-e29b-41d4-a716-446655440099",
            "items": [
                {
                    "product_id": "660e8400-e29b-41d4-a716-446655440001",
                    "product_name": "Test",
                    "category": "Electronics",
                    "quantity": 1,
                    "unit_price": 100_000.0,
                    "discount_pct": 0.0,
                }
            ],
            "subtotal": 100_000.0,
            "shipping_fee": 0.0,
            "total_amount": 100_000.0,
            "payment_method": "CREDIT_CARD",
            "shipping_address": {
                "city": "Jakarta",
                "province": "DKI Jakarta",
                "country": "ID",
                "postal_code": "10110",
            },
            "device_type": "DESKTOP",
            "platform": "WEB",
            "coupon_code": None,
            "metadata": {},
        }
        buf = io.BytesIO()
        schemaless_writer(buf, schema, record)
        assert buf.tell() > 0

    @pytest.mark.unit
    def test_pageview_null_user_id_accepted(self):
        from fastavro import schemaless_writer
        schema = _load_schema("pageview.avsc")
        record = {
            "event_id": "880e8400-e29b-41d4-a716-446655441001",
            "event_type": "pageview",
            "event_timestamp": 1705309200000,
            "session_id": "990e8400-e29b-41d4-a716-446655441001",
            "user_id": None,
            "page_url": "https://streamflow.io/home",
            "page_type": "HOME",
            "referrer_url": None,
            "product_id": None,
            "search_query": None,
            "device_type": "MOBILE",
            "user_agent": "Mozilla/5.0",
            "ip_address": "103.10.20.30",
            "duration_seconds": 45,
            "metadata": {},
        }
        buf = io.BytesIO()
        schemaless_writer(buf, schema, record)
        assert buf.tell() > 0

    @pytest.mark.unit
    @pytest.mark.parametrize("schema_file,record_key,bad_value", [
        ("order_placed.avsc", "payment_method", "BITCOIN"),
        ("order_placed.avsc", "device_type", "SMARTWATCH"),
    ])
    def test_invalid_enum_value_raises_error(self, schema_file, record_key, bad_value):
        from fastavro import schemaless_writer

        schema = _load_schema(schema_file)
        record = {
            "event_id": "880e8400-e29b-41d4-a716-446655449999",
            "event_type": "order_placed",
            "event_timestamp": 1705309200000,
            "order_id": "770e8400-e29b-41d4-a716-446655449999",
            "user_id": "550e8400-e29b-41d4-a716-446655440001",
            "session_id": "990e8400-e29b-41d4-a716-446655449999",
            "items": [
                {
                    "product_id": "660e8400-e29b-41d4-a716-446655440001",
                    "product_name": "Test",
                    "category": "Electronics",
                    "quantity": 1,
                    "unit_price": 100_000.0,
                    "discount_pct": 0.0,
                }
            ],
            "subtotal": 100_000.0,
            "shipping_fee": 0.0,
            "total_amount": 100_000.0,
            "payment_method": "CREDIT_CARD",
            "shipping_address": {
                "city": "Jakarta",
                "province": "DKI Jakarta",
                "country": "ID",
                "postal_code": "10110",
            },
            "device_type": "DESKTOP",
            "platform": "WEB",
            "coupon_code": None,
            "metadata": {},
        }
        record[record_key] = bad_value
        buf = io.BytesIO()
        with pytest.raises((ValueError, Exception)):
            schemaless_writer(buf, schema, record)
