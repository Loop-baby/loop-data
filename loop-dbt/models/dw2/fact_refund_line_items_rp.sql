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
    NULLIF(JSON_EXTRACT_PATH_TEXT(refund_line, 'item_id'), '') as refund_line_item_id
    , refund_id
    , order_id
    , NULLIF(JSON_EXTRACT_PATH_TEXT(refund_line, 'line_item_id'), '') as line_item_id
    , NULLIF(JSON_EXTRACT_PATH_TEXT(refund_line, 'quantity'), '')::INT as quantity
    , NULLIF(JSON_EXTRACT_PATH_TEXT(refund_line, 'requested_amount'), '')::FLOAT as total
    , NULLIF(JSON_EXTRACT_PATH_TEXT(refund_line, 'reason'), '')::FLOAT as reason
from unnested_array
where NULLIF(JSON_EXTRACT_PATH_TEXT(refund_line, 'item_type'), '') = 'PRODUCT'

