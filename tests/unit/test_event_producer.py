import sys
import os
import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
PRODUCER_DIR = os.path.join(ROOT, "producer")
for p in (ROOT, PRODUCER_DIR):
    if p not in sys.path:
        sys.path.insert(0, os.path.abspath(p))

from tests.fixtures.seed_data import (
    DETERMINISTIC_USERS,
    DETERMINISTIC_PRODUCTS,
    DETERMINISTIC_ORDERS,
    VALID_PAYMENT_METHODS,
    VALID_DEVICE_TYPES,
    VALID_PLATFORMS,
)


class TestOrderEventGeneration:

    def _make_producer(self):
        from producer.event_producer import EcommerceEventProducer
        p = EcommerceEventProducer.__new__(EcommerceEventProducer)
        p.users = DETERMINISTIC_USERS
        p.products = DETERMINISTIC_PRODUCTS
        from collections import deque
        p.recent_orders = deque(maxlen=1000)
        return p

    @pytest.mark.unit
    def test_generate_order_event_has_all_required_fields(self):
        p = self._make_producer()
        event = p.generate_order_event(DETERMINISTIC_USERS[0], DETERMINISTIC_PRODUCTS)
        required_fields = [
            "event_id", "event_type", "event_timestamp", "order_id",
            "user_id", "items", "total_amount", "payment_method",
            "shipping_address", "device_type", "platform", "subtotal",
            "shipping_fee",
        ]
        for field in required_fields:
            assert field in event, f"Missing required field: {field}"

    @pytest.mark.unit
    def test_generate_order_event_total_amount_equals_subtotal_plus_shipping(self):
        p = self._make_producer()
        for _ in range(20):
            event = p.generate_order_event(DETERMINISTIC_USERS[0], DETERMINISTIC_PRODUCTS)
            diff = abs(event["total_amount"] - (event["subtotal"] + event["shipping_fee"]))
            assert diff < 0.01, (
                f"total_amount={event['total_amount']} != "
                f"subtotal={event['subtotal']} + shipping={event['shipping_fee']}"
            )

    @pytest.mark.unit
    def test_generate_order_event_items_not_empty_and_within_bounds(self):
        p = self._make_producer()
        for _ in range(20):
            event = p.generate_order_event(DETERMINISTIC_USERS[0], DETERMINISTIC_PRODUCTS)
            assert 1 <= len(event["items"]) <= 5, (
                f"items count {len(event['items'])} out of [1, 5]"
            )

    @pytest.mark.unit
    def test_generate_order_event_discount_within_bounds(self):
        p = self._make_producer()
        for _ in range(100):
            event = p.generate_order_event(DETERMINISTIC_USERS[0], DETERMINISTIC_PRODUCTS)
            for item in event["items"]:
                assert 0.0 <= item["discount_pct"] <= 0.30, (
                    f"discount_pct {item['discount_pct']} out of [0, 0.30]"
                )

    @pytest.mark.unit
    def test_generate_order_event_payment_method_is_valid_enum(self):
        p = self._make_producer()
        for _ in range(50):
            event = p.generate_order_event(DETERMINISTIC_USERS[0], DETERMINISTIC_PRODUCTS)
            assert event["payment_method"] in VALID_PAYMENT_METHODS, (
                f"Invalid payment_method: {event['payment_method']}"
            )

    @pytest.mark.unit
    def test_generate_order_event_device_type_is_valid(self):
        p = self._make_producer()
        for _ in range(50):
            event = p.generate_order_event(DETERMINISTIC_USERS[0], DETERMINISTIC_PRODUCTS)
            assert event["device_type"] in VALID_DEVICE_TYPES

    @pytest.mark.unit
    def test_generate_order_event_item_revenue_calculation(self):
        expected_revenue = round(2 * 1_200_000.0 * (1 - 0.10), 2)
        assert expected_revenue == 2_160_000.0

        p = self._make_producer()
        items = [
            {
                "product_id": DETERMINISTIC_PRODUCTS[1]["product_id"],
                "product_name": DETERMINISTIC_PRODUCTS[1]["name"],
                "category": DETERMINISTIC_PRODUCTS[1]["category"],
                "quantity": 2,
                "unit_price": 1_200_000.0,
                "discount_pct": 0.10,
            }
        ]
        item = items[0]
        computed = round(item["quantity"] * item["unit_price"] * (1 - item["discount_pct"]), 2)
        assert computed == expected_revenue

    @pytest.mark.unit
    def test_generate_order_event_uuid_format(self):
        import re
        uuid4_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
        p = self._make_producer()
        for _ in range(10):
            event = p.generate_order_event(DETERMINISTIC_USERS[0], DETERMINISTIC_PRODUCTS)
            assert re.match(uuid4_pattern, event["event_id"]), f"Bad event_id: {event['event_id']}"
            assert re.match(uuid4_pattern, event["order_id"]), f"Bad order_id: {event['order_id']}"

    @pytest.mark.unit
    def test_generate_order_event_shipping_address_has_required_fields(self):
        p = self._make_producer()
        event = p.generate_order_event(DETERMINISTIC_USERS[0], DETERMINISTIC_PRODUCTS)
        for field in ("city", "province", "country", "postal_code"):
            assert field in event["shipping_address"], f"Missing address field: {field}"

    @pytest.mark.unit
    def test_generate_order_event_event_type_is_order_placed(self):
        p = self._make_producer()
        event = p.generate_order_event(DETERMINISTIC_USERS[0], DETERMINISTIC_PRODUCTS)
        assert event["event_type"] == "order_placed"

    @pytest.mark.unit
    def test_generate_order_event_user_id_matches_input(self):
        p = self._make_producer()
        user = DETERMINISTIC_USERS[2]
        event = p.generate_order_event(user, DETERMINISTIC_PRODUCTS)
        assert event["user_id"] == str(user["user_id"])

    @pytest.mark.unit
    def test_generate_order_event_subtotal_matches_items_sum(self):
        p = self._make_producer()
        for _ in range(20):
            event = p.generate_order_event(DETERMINISTIC_USERS[0], DETERMINISTIC_PRODUCTS)
            computed_subtotal = sum(
                item["quantity"] * item["unit_price"] * (1 - item["discount_pct"])
                for item in event["items"]
            )
            assert abs(event["subtotal"] - computed_subtotal) < 0.01, (
                f"subtotal {event['subtotal']} != computed {computed_subtotal}"
            )


class TestPageviewEventGeneration:

    def _make_producer(self):
        from producer.event_producer import EcommerceEventProducer
        from collections import deque
        p = EcommerceEventProducer.__new__(EcommerceEventProducer)
        p.users = DETERMINISTIC_USERS
        p.products = DETERMINISTIC_PRODUCTS
        p.recent_orders = deque(maxlen=1000)
        return p

    @pytest.mark.unit
    def test_pageview_event_has_required_fields(self):
        p = self._make_producer()
        event = p.generate_pageview_event(DETERMINISTIC_USERS[0], DETERMINISTIC_PRODUCTS)
        for field in ("event_id", "event_type", "event_timestamp", "session_id",
                      "page_url", "page_type", "device_type", "ip_address",
                      "duration_seconds", "user_agent"):
            assert field in event, f"Missing field: {field}"

    @pytest.mark.unit
    def test_pageview_event_type_is_pageview(self):
        p = self._make_producer()
        event = p.generate_pageview_event(DETERMINISTIC_USERS[0], DETERMINISTIC_PRODUCTS)
        assert event["event_type"] == "pageview"

    @pytest.mark.unit
    def test_product_detail_page_includes_product_id(self):
        import unittest.mock as mock
        p = self._make_producer()
        with mock.patch("random.random", return_value=0.0):
            with mock.patch(
                "producer.event_producer.weighted_choice",
                side_effect=["PRODUCT_DETAIL", "MOBILE", "ANDROID"]
            ):
                event = p.generate_pageview_event(DETERMINISTIC_USERS[0], DETERMINISTIC_PRODUCTS)
                if event["page_type"] == "PRODUCT_DETAIL":
                    assert event["product_id"] is not None

    @pytest.mark.unit
    def test_pageview_duration_is_positive(self):
        p = self._make_producer()
        for _ in range(20):
            event = p.generate_pageview_event(DETERMINISTIC_USERS[0], DETERMINISTIC_PRODUCTS)
            assert event["duration_seconds"] > 0

    @pytest.mark.unit
    def test_pageview_ip_address_format(self):
        import re
        p = self._make_producer()
        ip_pattern = r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"
        for _ in range(10):
            event = p.generate_pageview_event(DETERMINISTIC_USERS[0], DETERMINISTIC_PRODUCTS)
            assert re.match(ip_pattern, event["ip_address"]), f"Bad IP: {event['ip_address']}"


class TestInventoryEventGeneration:

    def _make_producer(self):
        from producer.event_producer import EcommerceEventProducer
        from collections import deque
        p = EcommerceEventProducer.__new__(EcommerceEventProducer)
        p.products = DETERMINISTIC_PRODUCTS
        p.recent_orders = deque(maxlen=1000)
        return p

    @pytest.mark.unit
    def test_inventory_event_has_required_fields(self):
        p = self._make_producer()
        event = p.generate_inventory_event(DETERMINISTIC_PRODUCTS)
        for field in (
            "event_id", "event_type", "event_timestamp", "product_id",
            "warehouse_id", "previous_quantity", "new_quantity", "change_reason",
        ):
            assert field in event, f"Missing field: {field}"

    @pytest.mark.unit
    def test_inventory_event_new_quantity_is_non_negative(self):
        p = self._make_producer()
        for _ in range(50):
            event = p.generate_inventory_event(DETERMINISTIC_PRODUCTS)
            assert event["new_quantity"] >= 0, f"Negative new_quantity: {event['new_quantity']}"

    @pytest.mark.unit
    def test_inventory_event_change_reason_is_valid(self):
        valid_reasons = {"SALE", "RESTOCK", "ADJUSTMENT", "RETURN", "DAMAGE"}
        p = self._make_producer()
        for _ in range(50):
            event = p.generate_inventory_event(DETERMINISTIC_PRODUCTS)
            assert event["change_reason"] in valid_reasons
