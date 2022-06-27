WITH numbers AS (
  SELECT
    (ROW_NUMBER() OVER () - 1)::int AS ordinal
  FROM
    stl_scan
  LIMIT 1000
)
, table_json_props AS (
  SELECT *
    , JSON_ARRAY_LENGTH(refund_line_items) AS arr_length
  FROM {{ref('fact_refund')}}
)
, unnested_array AS (
  SELECT *
    , NULLIF(json_extract_array_element_text(refund_line_items, numbers.ordinal), '') AS refund_line
  FROM table_json_props
  CROSS JOIN numbers
  WHERE numbers.ordinal < arr_length
)
select
    NULLIF(JSON_EXTRACT_PATH_TEXT(refund_line, 'id'), '') as refund_line_item_id
    , refund_id
    , order_id
    , NULLIF(JSON_EXTRACT_PATH_TEXT(refund_line, 'line_item_id'), '') as line_item_id
    , NULLIF(JSON_EXTRACT_PATH_TEXT(refund_line, 'quantity'), '')::INT as quantity
    , JSON_EXTRACT_PATH_TEXT(refund_line, 'restock_type') as restock_type
    , NULLIF(JSON_EXTRACT_PATH_TEXT(refund_line, 'subtotal'), '')::FLOAT as subtotal
    , NULLIF(JSON_EXTRACT_PATH_TEXT(refund_line, 'total_tax'), '')::FLOAT as total_tax
    , NULLIF(JSON_EXTRACT_PATH_TEXT(refund_line, 'total_discount'), '')::decimal as total_discount
    , JSON_EXTRACT_PATH_TEXT(refund_line, 'requires_shipping') as requires_shipping
    , NULLIF(JSON_EXTRACT_PATH_TEXT(refund_line, 'price'), '')::FLOAT as price
from unnested_array
