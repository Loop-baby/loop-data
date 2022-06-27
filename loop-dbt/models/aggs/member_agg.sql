-- primarily interested in "snoo members", but could be arbitrary member grain aggregates
with fwa_stats AS (
    select
        fwa.member_id
        , channel_name
        , first_product_viewed
        , first_url_viewed
        , min(fwa.timestamp) as first_touch_ts
        , count(distinct case when fwa.timestamp < dim_members.membership_start_dt then session_id end) as num_sessions_prior_to_conversion
        , max(case when first_session_url ilike '%r0hbqpcfkciaqj69c4a7qe88gneh1c%' then 1 else 0 end) as is_direct_to_snoo
        , count(distinct case when fwa.timestamp < dim_members.membership_start_dt and src_tbl = 'product_viewed' then product_id end) as num_unique_products_viewed_prior_to_conversion
    from (
        select *
            , first_value(case when src_tbl = 'pages' then url end ignore nulls) over(partition by session_id order by tmp.timestamp rows between unbounded preceding and unbounded following ) as first_session_url
            , first_value(product_id ignore nulls) over(partition by master_user_id order by tmp.timestamp rows between unbounded preceding and unbounded following) as first_product_viewed
            , first_value(url ignore nulls) over(partition by master_user_id order by tmp.timestamp rows between unbounded preceding and unbounded following) as first_url_viewed
        from {{ ref('fact_web_activity') }} as tmp
    ) as fwa
    left join {{ ref('dim_channel') }} as dc
        on fwa.user_first_session_channel_id = dc.channel_source_id
    left join {{ ref('dim_members') }}
        using(member_id)
    group by 1, 2, 3, 4
)
, fact_loop_stats as (
    select
        member_id
        , count(*) as all_time_rented_products
        , sum(case when NVL(pick_up_date, '9999-12-31') >= CURRENT_DATE AND delivery_date <= CURRENT_DATE then 1 else 0 end) as active_products
        , sum(case when NVL(pick_up_date, '9999-12-31') >= CURRENT_DATE AND delivery_date <= CURRENT_DATE then avg_price else 0 end) as current_mrr
        , count(distinct delivery_date) as loop_count
        , max(case when sku = '6650335723691' AND NVL(pick_up_date, '9999-12-31') >= CURRENT_DATE AND delivery_date <= CURRENT_DATE then 1 else 0 end) as has_snoo_in_home
        , max(case when sku != '6650335723691' AND NVL(pick_up_date, '9999-12-31') >= CURRENT_DATE AND delivery_date <= CURRENT_DATE then 1 else 0 end) as has_non_snoo_in_home
        , SUM(case when sku = '6650335723691' AND delivery_date <= CURRENT_DATE THEN DATEDIFF('d', delivery_date, NVL(pick_up_date, CURRENT_DATE)) end) as snoo_duration
    from {{ ref('fact_loop') }}
    group by 1
)
, discount_codes AS (
    select
        fact_order.member_id
        , listagg(distinct discount_code, ',') as member_discount_codes
    from {{ ref('fact_order') }}
    where discount_code is not null
    group by 1
)
, member_cltv AS (
    select
        member_id
        , sum(net_sales) as total_revenue
    from {{ ref('fact_transaction') }}
    group by 1
)
, saw_snoo_web as (
	select
		member_id
		, session_start_ts
		, dense_rank() over (partition by member_id order by session_start_ts) as rk
		, max(case when src_tbl = 'product_viewed' and product_id = '6650335723691' then 1 else 0 end) as saw_snoo
	from {{ ref('fact_web_activity') }} fwa
	group by 1, 2
)
, member_delivery_dates AS (
    select distinct
        member_id
        , delivery_date
    from dw.fact_loop
    where member_id is not null
        and delivery_date is not null
)
, member_time_to_second AS (
    select
        member_id
        , date_diff('d'
            , delivery_date
            , lead(delivery_date) over(partition by member_id order by delivery_date)
        ) as days_to_next_order
        , row_number() over(
            partition by member_id
            order by delivery_date
        ) as rn
    from member_delivery_dates
)
select
    dim_members.member_id
    , fwa_stats.channel_name as first_touch_channel
    , fwa_stats.first_touch_ts
    , CASE WHEN COALESCE(member_initial_baskets_shopify.order_ts, '9999-12-31') <  COALESCE(member_initial_baskets_loop.delivery_date, '9999-12-31')
        THEN NVL(member_initial_baskets_shopify.initial_basket_has_snoo = 1, FALSE)
        ELSE NVL(member_initial_baskets_loop.initial_basket_has_snoo = 1, FALSE)
    END as is_snoo_customer
    , dim_members.membership_start_dt
    , dim_members.membership_end_dt
    , membership_type
    , CASE
        WHEN COALESCE(member_initial_baskets_shopify.order_ts, '9999-12-31') <  COALESCE(member_initial_baskets_loop.delivery_date, '9999-12-31')
        THEN member_initial_baskets_shopify.initial_basket_ct
        ELSE member_initial_baskets_loop.initial_basket_ct
      END AS initial_basket_ct
    , CASE
        WHEN COALESCE(member_initial_baskets_shopify.order_ts, '9999-12-31') <  COALESCE(member_initial_baskets_loop.delivery_date, '9999-12-31')
        THEN COALESCE(member_initial_baskets_shopify.initial_basekt_dollars, 0)
        ELSE COALESCE(member_initial_baskets_loop.initial_basekt_dollars, 0)
      END AS initial_basket_product_dollars_gross
    , CASE
        WHEN COALESCE(member_initial_baskets_shopify.order_ts, '9999-12-31') <  COALESCE(member_initial_baskets_loop.delivery_date, '9999-12-31')
        THEN COALESCE(member_initial_baskets_shopify.initial_membership_dollars, 0)
        ELSE COALESCE(member_initial_baskets_loop.initial_membership_dollars, 0)
      END AS initial_membership_total
    , CASE
        WHEN COALESCE(member_initial_baskets_shopify.order_ts, '9999-12-31') <  COALESCE(member_initial_baskets_loop.delivery_date, '9999-12-31')
        THEN COALESCE(member_initial_baskets_shopify.total_discounts, 0)
        ELSE COALESCE(member_initial_baskets_loop.total_discounts, 0)
      END AS initial_basket_discounts
    , initial_basket_product_dollars_gross + initial_membership_total AS initial_basket_total_dollars_gross
    , initial_basket_total_dollars_gross -  initial_basket_discounts AS initial_basket_total_dollars_net
    , CASE
        WHEN COALESCE(member_initial_baskets_shopify.order_ts, '9999-12-31') <  COALESCE(member_initial_baskets_loop.delivery_date, '9999-12-31')
        THEN member_initial_baskets_shopify.initial_basket_products
        ELSE member_initial_baskets_loop.initial_basket_products
      END AS initial_basket_products
    , CASE
        WHEN COALESCE(member_initial_baskets_shopify.order_ts, '9999-12-31') <  COALESCE(member_initial_baskets_loop.delivery_date, '9999-12-31')
        THEN member_second_baskets_shopify.initial_basket_products
        ELSE member_second_baskets_loop.initial_basket_products
      END AS second_basket_products
    , CASE
        WHEN COALESCE(member_initial_baskets_shopify.order_ts, '9999-12-31') <  COALESCE(member_initial_baskets_loop.delivery_date, '9999-12-31')
        THEN member_second_baskets_shopify.initial_basket_ct
        ELSE member_second_baskets_loop.initial_basket_ct
      END AS second_basket_ct
    , CASE
        WHEN COALESCE(member_initial_baskets_shopify.order_ts, '9999-12-31') <  COALESCE(member_initial_baskets_loop.delivery_date, '9999-12-31')
        THEN COALESCE(member_second_baskets_shopify.initial_basekt_dollars, 0)
        ELSE COALESCE(member_second_baskets_loop.initial_basekt_dollars, 0)
      END AS second_basket_product_dollars_gross
    , CASE
        WHEN COALESCE(member_initial_baskets_shopify.order_ts, '9999-12-31') <  COALESCE(member_initial_baskets_loop.delivery_date, '9999-12-31')
        THEN COALESCE(member_second_baskets_shopify.initial_membership_dollars, 0)
        ELSE COALESCE(member_second_baskets_loop.initial_membership_dollars, 0)
      END AS second_membership_total
    , CASE
        WHEN COALESCE(member_initial_baskets_shopify.order_ts, '9999-12-31') <  COALESCE(member_initial_baskets_loop.delivery_date, '9999-12-31')
        THEN COALESCE(member_second_baskets_shopify.total_discounts, 0)
        ELSE COALESCE(member_second_baskets_loop.total_discounts, 0)
      END AS second_basket_discounts
    , second_basket_product_dollars_gross + second_membership_total AS second_basket_total_dollars_gross
    , second_basket_total_dollars_gross -  second_basket_discounts AS second_basket_total_dollars_net
    , all_time_rented_products
    , active_products
    , current_mrr
    , loop_count
    , num_sessions_prior_to_conversion
    , is_direct_to_snoo
    , member_discount_codes
    , total_revenue
    , has_non_snoo_in_home
    , has_snoo_in_home
    , dim_members.number_of_children
    , dim_members.youngest_child_dob
    , expecting_at_sign_up
    , snoo_duration
    , num_unique_products_viewed_prior_to_conversion
    , first_product_viewed
    , first_url_viewed
    , days_to_next_order AS days_to_second_order
    , CASE WHEN COALESCE(member_initial_baskets_shopify.order_ts, '9999-12-31') <  COALESCE(member_initial_baskets_loop.delivery_date, '9999-12-31')
            THEN CASE WHEN member_initial_baskets_shopify.initial_basket_ct = 1 and is_snoo_customer then 1 else 0 end
        ELSE
            CASE WHEN member_initial_baskets_loop.initial_basket_ct = 1 and is_snoo_customer then 1 else 0 end
        END AS first_order_snoo_only
    , saw_snoo_web.saw_snoo
    , dim_members.ops_location
from {{ ref('dim_members') }}
left join fwa_stats
    using(member_id)
left join {{ ref('member_initial_baskets_loop') }} AS member_initial_baskets_loop
    using(member_id)
left join {{ ref('member_initial_baskets_shopify') }} AS member_initial_baskets_shopify
    using(member_id)
left join {{ ref('member_second_baskets_loop') }} AS member_second_baskets_loop
    using(member_id)
left join {{ ref('member_second_baskets_shopify') }} AS member_second_baskets_shopify
    using(member_id)
left join fact_loop_stats
    using(member_id)
left join discount_codes
    using(member_id)
left join member_cltv
    using(member_id)
left join member_time_to_second
    on dim_members.member_id = member_time_to_second.member_id
    and member_time_to_second.rn = 1
left join saw_snoo_web
    on saw_snoo_web.member_id = dim_members.member_id
    and saw_snoo_web.rk = 1
where membership_start_dt <= current_date

