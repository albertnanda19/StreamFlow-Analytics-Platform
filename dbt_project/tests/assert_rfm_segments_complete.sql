select user_id
from {{ ref('silver_orders') }}

except

select user_id
from {{ ref('gold_user_segments') }}
