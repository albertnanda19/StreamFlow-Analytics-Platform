from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ORDER_ITEM_SCHEMA = StructType([
    StructField("product_id",   StringType(),  True),
    StructField("product_name", StringType(),  True),
    StructField("category",     StringType(),  True),
    StructField("quantity",     IntegerType(), True),
    StructField("unit_price",   DoubleType(),  True),
    StructField("discount_pct", FloatType(),   True),
])

SHIPPING_ADDRESS_SCHEMA = StructType([
    StructField("city",        StringType(), True),
    StructField("province",    StringType(), True),
    StructField("country",     StringType(), True),
    StructField("postal_code", StringType(), True),
])

ORDER_PLACED_SCHEMA = StructType([
    StructField("event_id",          StringType(),               True),
    StructField("event_type",        StringType(),               True),
    StructField("event_timestamp",   LongType(),                 True),
    StructField("order_id",          StringType(),               True),
    StructField("user_id",           StringType(),               True),
    StructField("session_id",        StringType(),               True),
    StructField("items",             ArrayType(ORDER_ITEM_SCHEMA), True),
    StructField("subtotal",          DoubleType(),               True),
    StructField("shipping_fee",      DoubleType(),               True),
    StructField("total_amount",      DoubleType(),               True),
    StructField("payment_method",    StringType(),               True),
    StructField("shipping_address",  SHIPPING_ADDRESS_SCHEMA,   True),
    StructField("device_type",       StringType(),               True),
    StructField("platform",          StringType(),               True),
    StructField("coupon_code",       StringType(),               True),
    StructField("metadata",          MapType(StringType(), StringType()), True),
])

ORDER_STATUS_SCHEMA = StructType([
    StructField("event_id",          StringType(), True),
    StructField("event_type",        StringType(), True),
    StructField("event_timestamp",   LongType(),   True),
    StructField("order_id",          StringType(), True),
    StructField("user_id",           StringType(), True),
    StructField("previous_status",   StringType(), True),
    StructField("new_status",        StringType(), True),
    StructField("updated_by",        StringType(), True),
    StructField("reason",            StringType(), True),
    StructField("metadata",          MapType(StringType(), StringType()), True),
])

PAGEVIEW_SCHEMA = StructType([
    StructField("event_id",          StringType(),  True),
    StructField("event_type",        StringType(),  True),
    StructField("event_timestamp",   LongType(),    True),
    StructField("session_id",        StringType(),  True),
    StructField("user_id",           StringType(),  True),
    StructField("page_url",          StringType(),  True),
    StructField("page_type",         StringType(),  True),
    StructField("referrer_url",      StringType(),  True),
    StructField("product_id",        StringType(),  True),
    StructField("search_query",      StringType(),  True),
    StructField("device_type",       StringType(),  True),
    StructField("user_agent",        StringType(),  True),
    StructField("ip_address",        StringType(),  True),
    StructField("duration_seconds",  IntegerType(), True),
    StructField("metadata",          MapType(StringType(), StringType()), True),
])

INVENTORY_UPDATE_SCHEMA = StructType([
    StructField("event_id",          StringType(),  True),
    StructField("event_type",        StringType(),  True),
    StructField("event_timestamp",   LongType(),    True),
    StructField("product_id",        StringType(),  True),
    StructField("warehouse_id",      StringType(),  True),
    StructField("previous_quantity", IntegerType(), True),
    StructField("new_quantity",      IntegerType(), True),
    StructField("change_reason",     StringType(),  True),
    StructField("reference_id",      StringType(),  True),
    StructField("metadata",          MapType(StringType(), StringType()), True),
])

VALID_PAGE_TYPES = {
    "HOME", "CATEGORY", "PRODUCT_DETAIL",
    "CART", "CHECKOUT", "ORDER_CONFIRMATION",
    "SEARCH", "PROFILE",
}
