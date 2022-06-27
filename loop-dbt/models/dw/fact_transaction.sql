select
	fo.order_id
    , fo.member_id
	, fli.line_item_id
	, fo.order_ts AS transaction_ts
	, fo.order_number
	, fo.order_type
	, 'order' as sales_type
	, case
		when fo.order_type = 'shopify_draft_order' then 'Draft Orders'
		when fo.order_type = 'subscription_contract' then 'Bold Subscriptions'
		when fo.order_type = 'web' then 'Online Store'
		when fo.order_type = '3890849' then 'Shop'
		when fo.order_type = '580111' then 'Registry Storefront'
	end as sales_channel
	, dp.product_name
	, dp.sku
	, fli.quantity
	, fli.price * fli.quantity as gross_sales
	, coalesce(fli.total_discount, 0) as discounts
	, 0.00 as returns
	, (fli.price * COALESCE(fli.quantity, 1)) - fli.total_discount as net_sales
	, fo.order_tax::decimal as taxes
	, (fli.price * fli.quantity) - fli.total_discount as total_sales
from {{ref('fact_order')}} fo
join {{ref('fact_line_item')}} fli 
	using(order_id)
left join {{ref('dim_product')}} dp 
	using(sku)

union all

select
	fr.order_id
    , fr.member_id
	, fli.line_item_id
	, fr.created_ts AS transaction_ts
	, fo.order_number
	, fo.order_type
	, 'return' as sales_type
	, case
		when fo.order_type = 'shopify_draft_order' then 'Draft Orders'
		when fo.order_type = 'subscription_contract' then 'Bold Subscriptions'
		when fo.order_type = 'web' then 'Online Store'
		when fo.order_type = '3890849' then 'Shop'
		when fo.order_type = '580111' then 'Registry Storefront'
	end as sales_channel
	, dp.product_name
	, dp.sku
	, fli.quantity::int * -1 as quantity
	, 0 as gross_sales
	, coalesce(fli.total_discount, 0) as discounts
	, coalesce(fli.subtotal::decimal * -1.00, fr.order_adjustments::decimal) as returns
	, returns AS net_sales
	, fli.total_tax::decimal as taxes
	, returns as total_sales
from {{ref('fact_refund')}}  fr
left join {{ref('fact_refund_line_item')}}  fli 
	using(order_id, refund_id)
left join {{ref('fact_order')}}  fo
	using(order_id)
left join {{ref('fact_line_item')}} fli2
	using(line_item_id)
left join {{ref('dim_product')}}  dp 
	using(sku)


union all

select
	fr.order_id
    , fr.member_id
	, NULL as line_item_id
	, fr.created_ts AS transaction_ts
	, fo.order_number
	, fo.order_type
	, 'return' as sales_type
	, case
		when fo.order_type = 'shopify_draft_order' then 'Draft Orders'
		when fo.order_type = 'subscription_contract' then 'Bold Subscriptions'
		when fo.order_type = 'web' then 'Online Store'
		when fo.order_type = '3890849' then 'Shop'
		when fo.order_type = '580111' then 'Registry Storefront'
	end as sales_channel
	, NULL as product_name
	, NULL as sku
	, 0 as quantity
	, order_adjustments as gross_sales
	, 0 as discounts
	, 0 as returns
	, order_adjustments AS net_sales
	, 0 as taxes
	, order_adjustments as total_sales
from {{ref('fact_refund')}}  fr
left join {{ref('fact_order')}}  fo
	using(order_id)
where order_adjustments > 0