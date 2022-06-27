WITH numbers AS (
  SELECT
    (ROW_NUMBER() OVER () - 1)::int AS ordinal
  FROM
    stl_scan
  LIMIT 1000
)
, table_json_props AS (
  SELECT *
    , JSON_ARRAY_LENGTH(refunds) AS arr_length
  FROM {{source('shopify', 'orders')}}
)
, unnested_array AS (
  SELECT *
    , NULLIF(json_extract_array_element_text(refunds, numbers.ordinal), '') AS refund_line
  FROM table_json_props
  CROSS JOIN numbers
  WHERE numbers.ordinal < arr_length
)
select
    JSON_EXTRACT_PATH_TEXT(refund_line, 'id') as refund_id
    , member_id_map.member_id
    , NULLIF(JSON_EXTRACT_PATH_TEXT(refund_line, 'order_id'), '') as order_id
    , NULLIF(JSON_EXTRACT_PATH_TEXT(refund_line, 'created_at'), '')::timestamp as created_ts
    , NULLIF(JSON_EXTRACT_PATH_TEXT(refund_line, 'processed_at'), '')::TIMESTAMP as processed_ts
    , case when JSON_EXTRACT_PATH_TEXT(refund_line, 'restock') = 'true' then true else FALSE end as restock
    , NULLIF(JSON_EXTRACT_PATH_TEXT(json_extract_array_element_text(
        JSON_EXTRACT_PATH_TEXT(refund_line, 'order_adjustments'), 0
    ), 'amount'), '')::FLOAT as order_adjustments
    , JSON_EXTRACT_PATH_TEXT(refund_line, 'transactions') as transactions
    , JSON_EXTRACT_PATH_TEXT(refund_line, 'refund_line_items') as refund_line_items
from unnested_array
left join {{ source('shopify', 'orders')}} 
    on NULLIF(JSON_EXTRACT_PATH_TEXT(refund_line, 'order_id'), '') = orders.id
left join {{ ref('member_id_map') }} AS member_id_map
    on  json_extract_path_text(orders.customer, 'id') = member_id_map.src_id
    and member_id_map.src = 'shopify'
