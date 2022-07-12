with discount_codes_rn as (
    select *
        , ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY discount_amount DESC) ranked_order
    from {{ ref('fact_discounts') }}
),
discount_codes as (
    select * from discount_codes_rn where ranked_order = 1
),
bc_refund_sum as (
    select order_id,
        sum(total_amount) as total_amount
    from {{ source('bigcommerce', 'refunds') }}
    group by 1
)
select
    o.id::text as order_id
    , oim.subscription_id
    , oim.invoices_id
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
        else 'refunded' end as financial_status
    , o.date_modified as updated_ts
    , dc.discount_code::text
    , dc.discount_amount
    , dc.discount_name::text
    , dc.discount_target::text
    , o.id as initial_bigcommerce_order_id
from {{ source('bigcommerce', 'orders') }} o
left join {{ ref('member_id_map_rp') }} AS mim
    on  o.customer_id = mim.src_id
    and mim.src = 'bigcommerce'
left join (
    SELECT DISTINCT order_id
    FROM {{ ref('membership_orders') }}
) as mo
    on o.id = mo.order_id
left join discount_codes dc
    on o.id = dc.order_id
left join bc_refund_sum r
    on o.id = r.order_id
left join {{ ref('order_ids_map') }} oim on
    o.id = oim.initial_bigcommerce_order_id and oim.initial_invoice = true

union all 

select
    rsi.id as order_id
    , rsi.subscription_id
    , rsi.id as invoices_id
    , mim.member_id
    , 'subscription' as order_type
    , rsi.received_at as order_ts
    , null as order_discounts
    , null as order_gross
    , null as order_tax
    , rsi.total as order_total
    , false AS is_membership_order
    , null AS is_gift_card_order
    , case when ch.refunded = true and ch.amount_refunded >= rsi.total then 'refunded'
        when ch.amount_refunded is not null and ch.amount_refunded != 0 and ch.amount_refunded < rsi.total then 'partially_refunded'
        when rsi.paid = false then 'unpaid'
        else 'paid' end as financial_status
    , null as updated_ts
    , null as discount_code
    , null as discount_amount
    , null as discount_name
    , null as discount_target
    , oim.initial_bigcommerce_order_id
from {{ ref('recurring_subscription_invoice') }} rsi
left join {{ source('stripe', 'charges') }} ch
    on  rsi.charge_id = ch.id
left join {{ ref('member_id_map_rp') }} mim 
    on rsi.customer_id = mim.src_id and
        mim.src = 'stripe' 
inner join {{ ref('order_ids_map') }} oim 
    on rsi.id = oim.invoices_id

