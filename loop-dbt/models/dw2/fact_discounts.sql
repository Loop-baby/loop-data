with discount_base as (
    select
        bcli.order_id,
        id as line_item_id
        , json_extract_array_element_text(applied_discounts, numbers.ordinal::INT, true) AS item
    from {{ source('bigcommerce', 'line_items') }} bcli
    cross join {{ ref('numbers') }} as numbers
    where ordinal < json_array_length(applied_discounts, true)
)
select
    line_item_id
    , order_id
    , json_extract_path_text(item, 'code') as discount_code
    , json_extract_path_text(item, 'amount')::float as discount_amount
    , json_extract_path_text(item, 'name') as discount_name
    , json_extract_path_text(item, 'target') as discount_target
from discount_base

