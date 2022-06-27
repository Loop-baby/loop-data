select
    member_id
    , sku
    , NULL AS subscription_id
    , avg_price AS gross_price
    , avg_price as net_price
    , 1 AS quantity
    , delivery_date as start_dt
    , pick_up_date as end_dt
    , NULL as status
from {{ ref('fact_loop') }}
