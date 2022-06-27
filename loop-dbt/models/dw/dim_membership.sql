select
    member_id
    , membership_start_dt as start_dt
    , LEAST(membership_end_dt, '9999-12-31') as end_dt
    , membership_type
    , case
        when CURRENT_DATE >= membership_start_dt
            AND CURRENT_DATE <= membership_end_dt
        THEN 'active'
        ELSE 'inactive'
      END as status
    , case membership_type
        when 'annual' then 149
        when 'monthly' then 18
        else 0
      end as price
from {{ ref('dim_members') }}
where membership_start_dt <= CURRENT_DATE