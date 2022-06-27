select
    address_id as id
    , name
    , first_name
    , last_name 
    , phone
    , address1 AS address_line_1
    , address2 AS address_line_2
    , city
    , province as state
    , province_code
    , zip
    , country
    , country_code
    , latitude as lat
    , longitude as long
    , company
from (
    select *
        , row_number() over(
            partition by address_id
            order by order_id desc
        ) as rn
    from {{ ref('order_shipping_addresses') }}
)
where rn = 1
and address_id is not null