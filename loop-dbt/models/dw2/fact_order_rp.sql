with discount_base as (
    select
        orders.id as order_id
        , json_extract_array_element_text(applied_discounts, numbers.ordinal::INT, true) AS item
    from {{ source('bigcommerce', 'line_items') }}
    cross join {{ ref('numbers') }} as numbers
    where ordinal < json_array_length(discount_codes, true)
), 
discount_codes as (
    select
        order_id
        , json_extract_path_text(item, 'code') as discount_code
        , json_extract_path_text(item, 'amount')::float as discount_amount
        , json_extract_path_text(item, 'name') as discount_name
        , json_extract_path_text(item, 'target') as discount_target
        , ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY json_extract_path_text(item, 'amount')::float DESC) ranked_order
    from discount_base
)
select
    o.id as order_id
    , mim.member_id
    , 'web' as order_type
    , o.date_created as order_ts
    , o.discount_amount as order_discounts
    , o.total_ex_tax as order_gross
    , o.total_tax as order_tax
    , o.total_inc_tax as order_total
    , mo.order_id is not null AS is_membership_order
    , null AS is_gift_card_order
    , case when r.total_amount is null or r.total_amount = 0 then 'paid' 
        when o.total_ex_tax <= r.total_amount then 'partially_refunded'
        else 'refunded' as financial_status
    , o.date_modified as updated_ts
    , dc.discount_code
    , dc.discount_amount
    , dc.discount_name
    , dc.discount_target
from {{ source('bigcommerce', 'orders') }} o
left join {{ ref('member_id_map_rp') }} AS mim
    on  o.customer_id = mim.src_id
    and member_id_map.src = 'bigcommerce'
left join (
    SELECT DISTINCT order_id
    FROM {{ ref('membership_orders') }}
) as member_orders
    on orders.id = member_orders.order_id
left join discount_codes
    on discount_codes.order_id = orders.id

union all 

select
    in.id as order_id
    , mim.member_id
    , 'web' as order_type
    , o.date_created as order_ts
    , o.discount_amount as order_discounts
    , o.total_ex_tax as order_gross
    , o.total_tax as order_tax
    , o.total_inc_tax as order_total
    , mo.order_id is not null AS is_membership_order
    , null AS is_gift_card_order
    , case when r.total_amount is null or r.total_amount = 0 then 'paid' 
        when o.total_ex_tax <= r.total_amount then 'partially_refunded'
        else 'refunded' as financial_status
    , o.date_modified as updated_ts
    , dc.discount_code
    , dc.discount_amount
    , dc.discount_name
    , dc.discount_target
from {{ source('bigcommerce', 'orders') }} o
left join {{ ref('member_id_map_rp') }} AS mim
    on  json_extract_path_text(customer, 'id') = mim.src_id
    and member_id_map.src = 'bigcommerce'
left join (
    SELECT DISTINCT order_id
    FROM {{ ref('membership_orders') }}
) as member_orders
    on orders.id = member_orders.order_id
left join discount_codes
    on discount_codes.order_id = orders.id

