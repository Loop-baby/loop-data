with member_views as (
	select 
		member_id 
		, count(1) page_views
	from {{ref('fact_web_activity')}}
	where src_tbl = 'pages'
	and member_id is not null
	group by member_id 
)
select 
	fwa.member_id
	, session_id 
	, product_id_master
	, created_at::date
	, case when referrer like '%loop%' then true else false end as loop_referred
	, member_views.page_views
from {{ref('fact_web_activity')}} fwa
left join member_views
	on member_views.member_id = fwa.member_id 
