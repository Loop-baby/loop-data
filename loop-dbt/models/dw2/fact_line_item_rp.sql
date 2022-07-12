select
    id::text as line_item_id
    , order_id::text
    , null as is_gift_card
    , base_price as price
    , quantity
    , product_id::text
    , name as product_name
    , variant_id
	, case when applied_discounts != '[]' then JSON_EXTRACT_PATH_TEXT(json_extract_array_element_text(applied_discounts, 0), 'amount')::decimal else 0.00 end as total_discount
    , 'bigcommerce' as src
from {{ source('bigcommerce', 'line_items') }}

union all 

select
    ii.id as line_item_id
    , ii.invoice_id as order_id
    , null as is_gift_card
    , ii.amount::float/100.00 as price
    , ii.quantity
    , p.metadata_big_commerce_product_id as product_id
    , ii.description as product_name
    , null as variant_id
	, null as total_discount
    , 'stripe' as src
from {{ ref('recurring_subscription_invoice') }} rsi inner join
    {{ source('stripe', 'invoice_items') }} ii on rsi.id = ii.invoice_id left join
    stripe.products p on ii.description = p.name