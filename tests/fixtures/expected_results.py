from tests.fixtures.seed_data import DETERMINISTIC_ORDERS

SILVER_ORDER_EXPECTED = {
    "770e8400-e29b-41d4-a716-446655440001": {
        "order_id": "770e8400-e29b-41d4-a716-446655440001",
        "user_email": "andi@test.streamflow.io",
        "gross_revenue": 15_000_000.0,
        "order_item_count": 1,
        "has_discount": False,
        "is_high_value": True,
        "payment_method": "CREDIT_CARD",
        "device_category": "NON-MOBILE",
    },
    "770e8400-e29b-41d4-a716-446655440002": {
        "order_id": "770e8400-e29b-41d4-a716-446655440002",
        "user_email": "budi@test.streamflow.io",
        "gross_revenue": 2_510_000.0,
        "order_item_count": 2,
        "has_discount": True,
        "is_high_value": True,
        "payment_method": "EWALLET",
        "device_category": "MOBILE",
    },
    "770e8400-e29b-41d4-a716-446655440003": {
        "order_id": "770e8400-e29b-41d4-a716-446655440003",
        "user_email": "citra@test.streamflow.io",
        "gross_revenue": 255_000.0,
        "order_item_count": 1,
        "has_discount": False,
        "is_high_value": False,
        "payment_method": "BANK_TRANSFER",
        "device_category": "MOBILE",
    },
    "770e8400-e29b-41d4-a716-446655440004": {
        "order_id": "770e8400-e29b-41d4-a716-446655440004",
        "user_email": "doni@test.streamflow.io",
        "gross_revenue": 2_125_000.0,
        "order_item_count": 1,
        "has_discount": True,
        "is_high_value": True,
        "payment_method": "COD",
        "device_category": "MOBILE",
    },
    "770e8400-e29b-41d4-a716-446655440005": {
        "order_id": "770e8400-e29b-41d4-a716-446655440005",
        "user_email": "eka@test.streamflow.io",
        "gross_revenue": 1_625_000.0,
        "order_item_count": 2,
        "has_discount": False,
        "is_high_value": True,
        "payment_method": "DEBIT_CARD",
        "device_category": "NON-MOBILE",
    },
}

SILVER_ORDER_ITEMS_EXPECTED = {
    ("770e8400-e29b-41d4-a716-446655440002", "660e8400-e29b-41d4-a716-446655440002"): 2_160_000.0,
    ("770e8400-e29b-41d4-a716-446655440002", "660e8400-e29b-41d4-a716-446655440003"): 350_000.0,
    ("770e8400-e29b-41d4-a716-446655440004", "660e8400-e29b-41d4-a716-446655440005"): 2_125_000.0,
    ("770e8400-e29b-41d4-a716-446655440005", "660e8400-e29b-41d4-a716-446655440002"): 1_200_000.0,
    ("770e8400-e29b-41d4-a716-446655440005", "660e8400-e29b-41d4-a716-446655440004"): 425_000.0,
}

GOLD_DAILY_REVENUE_EXPECTED = {
    "2024-01-15": {
        "revenue": sum(o["total_amount"] for o in DETERMINISTIC_ORDERS),
        "order_count": len(DETERMINISTIC_ORDERS),
    }
}

BRONZE_REQUIRED_COLUMNS = {
    "event_id", "order_id", "user_id", "total_amount", "payment_method",
    "event_timestamp", "ingestion_timestamp", "is_valid", "processing_date",
    "kafka_partition", "kafka_offset",
}

SILVER_ORDERS_REQUIRED_COLUMNS = {
    "order_id", "user_id", "total_amount", "payment_method",
    "event_timestamp", "is_valid", "gross_revenue", "is_high_value",
    "has_discount", "order_item_count",
}

GOLD_DAILY_REVENUE_REQUIRED_COLUMNS = {
    "order_date", "revenue", "order_count", "avg_order_value",
}
