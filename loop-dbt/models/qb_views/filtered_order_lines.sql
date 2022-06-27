with base as (
select 
    li.id
    , li.price
    , li.quantity
    , li.title
    , li.total_discount
    , li.variant_id
    , li.name
    , li.product_id
    , li.order_id
    , o.source_name
    , row_number() over (partition by json_extract_path_text(customer, 'id'), product_id order by o.created_at) as rn
from {{source('shopify', 'line_items')}} li 
join {{source('shopify', 'orders')}} o 
    on o.id  = li.order_id 
)
select 
    id
    , price
    , quantity
    , title
    , total_discount
    , variant_id
    , name
    , product_id
    , order_id
from base 
WHERE rn > 1 and source_name = 'subscription_contract'