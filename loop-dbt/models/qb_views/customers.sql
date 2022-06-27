select
    sc.*
    , b.hubspot_id
from {{source('shopify', 'customers')}} sc
left join (
    select 
        email
        , id as hubspot_id
        , row_number() over (partition by id order by updated_at desc) as rn
    from {{source('hubspot', 'contacts')}}
    where nullif(email, '') is not null
) b
    on sc.email = b.email
    and b.rn = 1
