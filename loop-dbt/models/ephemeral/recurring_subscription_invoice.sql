with base as (
    select i.id,
         row_number() over(partition by s.id order by i.date) as rn
    from {{ source('stripe', 'subscriptions') }} s inner join
        {{ source('stripe', 'invoices') }} i on s.id = i.subscription_id
)

select i.*
from base b inner join
    {{ source('stripe', 'invoices') }} i on b.id = i.id
where b.rn != 1