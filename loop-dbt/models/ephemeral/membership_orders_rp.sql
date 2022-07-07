with base as (
    select
        c.email
        , mim.member_id
        , o.id as order_id
        , o.created_at
        , p.title as membership_product_name
        , p.id as product_id
        , p.price as membership_price
    from bigcommerce.line_items li
    left join bigcommerce.orders o
        on li.order_id = o.id
    left join bigcommerce.products p
        on li.product_id = p.id
    left join bigcommerce.customers c
        on c.id = o.customer_id
    LEFT JOIN {{ ref('member_id_map_rp')}} as member_id_map
        on o.customer_id = mim.src_id
      and src='bigcommerce'
    where p.title ilike '%member%'
        or p.title ilike '%plan'

    union all

    select
        c.email
        , mim.member_id
        , o.id as order_id
        , o.date_created as created_at
        , p.title as membership_product_name
        , p.id as product_id
        , p.price as membership_price
    from bigcommerce.line_items li
    left join bigcommerce.orders o
        on li.order_id = o.id
    left join bigcommerce.products p
        on li.product_id = p.id
    left join bigcommerce.customers c
        on c.id = o.customer_id
    LEFT JOIN {{ ref('member_id_map_rp')}} as mim
        on o.customer_id = mim.src_id
      and src='bigcommerce'
    where p.title ilike '%member%'
        or p.title ilike '%plan'
),
base_with_rn as (
    select *,
        row_number() over(partition by member_id order by created_at) as rn
    from base
)

select
    email
    , member_id
    , created_at
    , membership_product_name
    , order_id
    , product_id
    , membership_price
from base_with_rn
where rn = 1
