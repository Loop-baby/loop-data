with discount_base as (
    select
        orders.id as order_id
        , json_extract_array_element_text(discount_codes, numbers.ordinal::INT, true) AS item
    from {{ source('shopify', 'orders')}}
    cross join {{ ref('numbers') }} as numbers
    where ordinal < json_array_length(discount_codes, true)
), discount_codes as (
select
    order_id
    , json_extract_path_text(item, 'code') as discount_code
    , json_extract_path_text(item, 'amount')::float as discount_amount
    , json_extract_path_text(item, 'type') as discount_type
from discount_base
)
select
    id as order_id
    , member_id_map.member_id
    , order_number
    , source_name as order_type
    , created_at as order_ts
    , processed_at as processed_ts
    , total_discounts as order_discounts
    , total_line_items_price as order_gross
    , total_tax as order_tax
    , total_price_usd as order_total
    , member_orders.order_id is not null AS is_membership_order
    , gift_card_orders.order_id is not null AS is_gift_card_order
    , financial_status
    , updated_at as updated_ts
    , discount_codes.discount_code
    , discount_codes.discount_amount
    , discount_codes.discount_type
from {{ source('shopify', 'orders') }}
left join {{ ref('member_id_map') }} AS member_id_map
    on  json_extract_path_text(customer, 'id') = member_id_map.src_id
    and member_id_map.src = 'shopify'
left join (
    SELECT DISTINCT order_id
    FROM {{ ref('membership_orders') }}
) as member_orders
    on orders.id = member_orders.order_id
left join (
    select distinct order_id
    from (
        select
            order_id
            , count(*) as num_items
            , sum(case when products.title ilike '%gift card%' or line_items.name ilike '%gift card%' then 1 else 0 end) as num_gcs
        from {{ source('shopify', 'line_items') }}
        left join {{ source('shopify', 'products') }}
            on line_items.product_id = products.id
        group by 1
    )
    where COALESCE(num_gcs, 0) > 0
        and num_items = num_gcs
) as gift_card_orders
    on orders.id = gift_card_orders.order_id
left join discount_codes
    on discount_codes.order_id = orders.id
