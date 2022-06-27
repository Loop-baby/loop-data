with months as (
    select
        dateadd('month', ordinal, '2020-12-01'::date)::date as month
    from {{ ref('numbers') }}
    where month <= DATE_TRUNC('month', CURRENT_DATE)
)
, item_level_base as (
    select
        fact_loop.sku
        , fact_loop.delivery_date
        , fact_loop.pick_up_date
        , fact_loop.avg_price AS item_mrr

        , dim_members.member_id
        , dim_members.membership_type
        , dim_members.membership_start_dt
        , dim_members.youngest_child_dob
        , dim_members.number_of_children
        , dim_members.expecting_at_sign_up
        , case
            when dim_members.membership_type = 'annual'
                then 149.0/12
            when dim_members.membership_type = 'monthly'
                then 18.0
            else 0
          end as _membership_mrr
        , dim_members.is_loopshare

        , min(fact_loop.delivery_date) over(partition by dim_members.member_id) as first_delivery_date

    from {{ ref('fact_loop') }}
    inner join {{ ref('dim_members') }}
        using(member_id)
)
, item_month_expansion as (
    select *
    from item_level_base
    inner join months
        on item_level_base.delivery_date < dateadd('month', 1, months.month)
        and coalesce(item_level_base.pick_up_date, '9999-12-31') >= months.month
)
select
    month
    , item_month_expansion.member_id
    , date_trunc('month', membership_start_dt)::date as member_start_month
    , date_trunc('month', first_delivery_date)::date as first_delivery_month
    , is_loopshare
    , membership_type
    , youngest_child_dob
    , number_of_children
    , expecting_at_sign_up
    , item_mrr
    , case when row_number() over(partition by item_month_expansion.member_id, month order by sku) = 1 then _membership_mrr else 0 end as membership_mrr
    , item_mrr + membership_mrr as gross_mrr
    , case when row_number() over(partition by item_month_expansion.member_id, month order by sku) = 1 then NVL(loopshare_discount_hack.discount_mrr_offset, 0) else 0 end as discount_offset
    , gross_mrr + discount_offset as net_mrr
    , basket_bin
    , initial_basket_ct
    , initial_basekt_dollars
    , case when initial_basket_has_snoo = 1 then 'YES' else 'NO' end as initial_basket_has_snoo
from item_month_expansion
left join {{ ref('loopshare_discount_hack') }} AS loopshare_discount_hack
    on item_month_expansion.member_id = loopshare_discount_hack.member_id
    and loopshare_discount_hack.start_date < item_month_expansion.month
    and loopshare_discount_hack.end_date >= item_month_expansion.month
left join {{ ref('member_initial_baskets_loop') }} AS member_initial_baskets_loop
    on item_month_expansion.member_id = member_initial_baskets_loop.member_id
