with base as (
select 
    li.id
    , li.price
    , li.quantity
    , li.title
    , li.total_discount
    , li.variant_id
    , li.name
    , li.product_id
    , li.order_id
    , o.source_name
    , row_number() over (partition by json_extract_path_text(customer, 'id'), product_id order by o.created_at) as rn
from {{source('shopify', 'line_items')}} li 
join {{source('shopify', 'orders')}} o 
    on o.id  = li.order_id 
), base_agg as (
select 
    id
    , price
    , quantity
    , title
    , total_discount
    , variant_id
    , name
    , product_id
    , order_id
    , id as raw_order_line_id
from base 
where rn = 1
    or source_name != 'subscription_contract'
), quant_group as (
    SELECT
        id
        , row_number() over(partition by id) as row_repeat
    from base_agg
    join (select row_number() over() as quantity from {{ref('dim_item')}}) rpt
    on base_agg.quantity>=rpt.quantity
), flattened_quants as (
SELECT
    CASE WHEN row_repeat > 1 THEN row_repeat::varchar || base_agg.id ELSE base_agg.id END as id
    , price
    , 1 as quantity
    , title
    , total_discount
    , variant_id
    , name
    , product_id
    , order_id
    , raw_order_line_id
    , CASE WHEN base_agg.quantity > 1 then TRUE else FALSE end as multiple_qty_flag
from base_agg
LEFT JOIN quant_group
    on base_agg.id = quant_group.id and row_repeat < 9
), split_bundle as (
    SELECT
        *
    FROM flattened_quants
    WHERE product_id in ('7266594881707', '6706707234987')
)

SELECT
    flattened_quants.*
    , CASE WHEN split_bundle.id is not null then TRUE else FALSE END as unbundled_flag
FROM flattened_quants
LEFT JOIN split_bundle
    on flattened_quants.id = split_bundle.id

UNION ALL

SELECT
    '9' || id as id
    , 0 as price
    , quantity
    , title
    , total_discount
    , variant_id
    , CASE
        WHEN product_id = '7266594881707' THEN 'Guava Bassinet'
        WHEN product_id = '6706707234987' THEN 'Lily & River Rockwall Accessory'
      END AS name
    , CASE 
        WHEN product_id = '7266594881707' THEN '6650337886379'
        WHEN product_id = '6706707234987' THEN '7304838873259'
    END as product_id
    , order_id
    , raw_order_line_id
    , multiple_qty_flag
    , TRUE as unbundled_flag
FROM split_bundle