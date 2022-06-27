WITH base as (
select
    orders.id
    , orders.order_number
    , orders.created_at::date AS Order_Date
    , json_extract_path_text(customer, 'id') as customer_id
    , REPLACE(orders.created_at::text, ' ', 'T') || 'Z' AS created_at
    , REPLACE(orders.updated_at::text, ' ', 'T') || 'Z' AS updated_at
    , orders.subtotal_price
    , orders.total_discounts
    , orders.financial_status
    , orders.source_name
    , ad.address_id
    , CASE ad.province
        WHEN 'California' THEN 1
        WHEN 'New York' THEN 2
        WHEN 'New Jersey' THEN 2
        WHEN 'Connecticut' THEN 2
    END AS raw_inventory_location
    , row_number() OVER (partition by customer_id, line_items.product_id order by orders.created_at) as rn
from {{source('shopify', 'orders')}} as orders
join {{source('shopify', 'line_items')}} line_items
    on orders.id  = line_items.order_id 
left join {{ ref('order_shipping_addresses') }} as ad
    on orders.id = ad.order_id
)
select distinct
    id
    , order_number
    , Order_Date
    , customer_id
    , created_at
    , updated_at
    , subtotal_price
    , total_discounts
    , financial_status
    , source_name
    , address_id AS raw_address_id
    , raw_inventory_location
from base
where rn = 1
    or source_name != 'subscription_contract'