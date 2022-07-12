with bc_refunds as (
    select 
        r.id::text,
        fo.member_id,
        r.order_id::text,
        r.created::timestamp as created_ts,
        r.total_amount as refund_amount,
        r.payments as transactions,
        r.items as refund_line_item,
        'bigcommerce' as src_name
    from {{ source('bigcommerce', 'refunds') }} r inner join
        {{ ref('fact_order_rp') }} fo on r.order_id = fo.order_id
)

select 
    sr.id,
    mim.member_id,
    ch.invoice_id as order_id,
    sr.created as created_ts,
    sr.amount::float/100.00 as refund_amount,
    null as payments,
    null as items,
    'stripe' as src_name
from {{ source('stripe', 'refunds') }} sr left join
    {{ source('stripe', 'charges') }} ch on sr.charge_id = ch.id left join
    {{ ref('member_id_map_rp') }} mim on ch.customer_id = mim.src_id and mim.src_id = 'stripe' left join
    bc_refunds bcr on mim.member_id = bcr.member_id and datediff(day, bcr.created_ts::timestamp, sr.created) < 2
where bcr.id is null 

union all 

select *
from bc_refunds
