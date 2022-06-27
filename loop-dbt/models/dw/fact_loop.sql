with loop_master as (
    select
        pick_deliveries.asset_tag_association_asset_tag as bin
        , member_id_map.member_id
        , SPLIT_PART(order_lines.id, '.', 1) AS line_item_id
        , order_lines.qb_item_sku as sku
        , SUBSTRING(order_lines.order_id,0, CHARINDEX('.',order_lines.order_id)) as order_id
        , NULLIF(schedule_delivery.scheduled_delivery_date, '')::DATE delivery_date
        , NULLIF(schedule_pickup.scheduled_pickup_date, '')::DATE AS pick_up_date
        -- , inventory_reporting_base.restocking_time as restocking_time
        , COALESCE(NULLIF(order_lines.price, '')::float, sku_avg_price.avg_price) AS avg_price
        , CASE 
            WHEN order_lines.order_id_qb_inventory_location_inventory_location_name ilike 'bay %' then 'SF'
            WHEN order_lines.order_id_qb_inventory_location_inventory_location_name ilike 'ny %' then 'NYC'
        END as ops_location
        FROM {{source('quickbase', 'order_lines')}}
        LEFT JOIN {{ ref('dim_product') }}
            ON order_lines.qb_item_sku = dim_product.sku
        LEFT JOIN 
            (   select
                    *
                    , row_number() OVER (partition by related_order_line order by date_modified desc) as rn
                FROM {{source('quickbase', 'schedule_delivery')}}
                WHERE schedule_delivery.delivery_status != 'Scheduled Delivery Cancelled'     
            ) schedule_delivery
            ON schedule_delivery.related_order_line_ref = order_lines.id
            AND schedule_delivery.rn = 1
        LEFT JOIN (
            SELECT DISTINCT
                asset_tag_association_asset_tag, scheduled_delivery_related_order_line
            FROM {{source('quickbase', 'pick_deliveries')}}
        ) pick_deliveries
            ON pick_deliveries.scheduled_delivery_related_order_line = schedule_delivery.related_order_line
        LEFT JOIN 
            (   select
                    *
                    , row_number() OVER (partition by delivery_performance_picking_scheduled_delivery_related_order_line_ref order by date_modified desc) as rn
                FROM {{source('quickbase', 'schedule_pickup')}}
                WHERE schedule_pickup.scheduled_pickup_status != 'Scheduled Pickup Cancelled'     
            ) schedule_pickup
            ON schedule_pickup.delivery_performance_picking_scheduled_delivery_related_order_line_ref = order_lines.id
            AND schedule_pickup.rn = 1
        LEFT JOIN {{source('quickbase', 'customers')}}
            ON SUBSTRING(order_lines.qb_order_related_customer,0, CHARINDEX('.',order_lines.qb_order_related_customer)) = customers.id::TEXT
        left join {{ ref('member_id_map') }} AS member_id_map
            on customers.hubspot_id = member_id_map.src_id
            and member_id_map.src = 'hubspot'
        left join {{ ref('sku_avg_price') }} AS sku_avg_price
            on order_lines.qb_item_sku = sku_avg_price.sku
        where order_lines.qb_ol_status not in (
          'Closed-Refunded',
          'Closed-Non-inventory',
          'Closed-Duplicate',
          'Closed-Registry',
          'Closed-Test',
          'Closed-Trial',
          'Closed'
        ) 
        AND product_name NOT ILIKE '% plan%'
        AND product_name NOT ILIKE '%membership%'
)  

select 
    *
from loop_master