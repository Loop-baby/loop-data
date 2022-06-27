select
    order_id
    , discount_code
    , discount_amount
    , discount_type
from {{ref('fact_order')}}
where discount_code is not null
