select
    id as line_item_id
    , order_id
    , gift_card as is_gift_card
    , price
    , quantity
    , vendor
    , product_id as sku
    , variant_id
	, case when discount_allocations != '[]' then JSON_EXTRACT_PATH_TEXT(json_extract_array_element_text(discount_allocations, 0), 'amount')::decimal else 0.00 end as total_discount
from {{ source('shopify', 'line_items') }}
