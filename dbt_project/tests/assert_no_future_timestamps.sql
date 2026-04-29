select order_id
from {{ ref('silver_orders') }}
where event_timestamp > current_timestamp()
