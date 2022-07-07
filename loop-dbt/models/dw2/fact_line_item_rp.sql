select
    id as line_item_id
    , order_id
    , gift_card as is_gift_card
    , price
    , quantity
    , product_id as sku
    , variant_id
	, case when discount_allocations != '[]' then JSON_EXTRACT_PATH_TEXT(json_extract_array_element_text(discount_allocations, 0), 'amount')::decimal else 0.00 end as total_discount
    , 'shopify' as src
from {{ source('shopify', 'line_items') }}

union all 

select
    id as line_item_id
    , order_id
    , null as is_gift_card
    , base_price as price
    , quantity
    , product_id as sku
    , variant_id
	, case when applied_discounts != '[]' then JSON_EXTRACT_PATH_TEXT(json_extract_array_element_text(applied_discounts, 0), 'amount')::decimal else 0.00 end as total_discount
    , 'bigcommerce' as src
from {{ source('bigcommerce', 'line_items') }}