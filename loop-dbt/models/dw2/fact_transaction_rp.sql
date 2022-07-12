select
	fo.order_id
    , fo.subscription_id
    , fo.invoices_id
    , fo.member_id
	, fli.line_item_id
	, fo.order_ts AS transaction_ts
	, fo.order_type
	, 'order' as sales_type
	, fo.order_type as sales_channel
	, dp.product_name
	, dp.sku
	, fli.quantity
	, fli.price * fli.quantity as gross_sales
	, coalesce(fli.total_discount, 0) as discounts
	, 0.00 as returns
	, (fli.price * COALESCE(fli.quantity, 1)) - fli.total_discount as net_sales
	, fo.order_tax::decimal as taxes
	, (fli.price * fli.quantity) - fli.total_discount as total_sales
from {{ref('fact_order_rp')}} fo
join {{ref('fact_line_item_rp')}} fli 
	using(order_id)
left join {{ref('dim_product_rp')}} dp 
	on fli.product_id = dp.sku

union all

select
	fr.order_id
	, fo.subscription_id
	, fo.invoices_id
    , fr.member_id
	, fli.line_item_id
	, fr.created_ts AS transaction_ts
	, fo.order_type
	, 'return' as sales_type
	, fo.order_type as sales_channel
	, dp.product_name
	, dp.sku
	, fli.quantity::int * -1 as quantity
	, 0 as gross_sales
	, coalesce(fli2.total_discount, 0) as discounts
	, coalesce(fli2.price::decimal * fli2.quantity * -1.00, fr.refund_amount::decimal) as returns
	, returns AS net_sales
	, null as taxes
	, returns as total_sales
from {{ref('fact_refund_rp')}}  fr
left join {{ref('fact_refund_line_items_rp')}}  fli 
	on fr.order_id = fli.order_id and fr.id = fli.refund_id 
left join {{ref('fact_order_rp')}}  fo
	on fr.order_id = fo.order_id
left join {{ref('fact_line_item_rp')}} fli2
	using(line_item_id)
left join {{ref('dim_product_rp')}}  dp 
	on fli2.product_id = dp.sku


union all

select
	fr.order_id
	, fo.subscription_id
	, fo.invoices_id
    , fr.member_id
	, NULL as line_item_id
	, fr.created_ts AS transaction_ts
	, fo.order_type
	, 'return' as sales_type
	, fo.order_type as sales_channel
	, NULL as product_name
	, NULL as sku
	, 0 as quantity
	, 0 as gross_sales
	, 0 as discounts
	, 0 as returns
	, 0 AS net_sales
	, 0 as taxes
	, 0 as total_sales
from {{ref('fact_refund_rp')}}  fr
left join {{ref('fact_order_rp')}}  fo
	using(order_id)
where refund_amount > 0