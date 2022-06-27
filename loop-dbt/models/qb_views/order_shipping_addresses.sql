{% set address_fields  = [
    'name', 
    'first_name', 
    'last_name', 
    'phone',
    'address1', 
    'address2',
    'city', 
    'province', 
    'province_code',
    'zip', 
    'country', 
    'country_code',
    'latitude', 
    'longitude', 
    'company'
] %}

with address_base as(
    select
        orders.id as order_id
        {% for field in address_fields %}
        , json_extract_path_text(shipping_address, '{{ field }}', true) as {{ field }}
        {% endfor %}
    from {{source('shopify', 'orders')}} as orders
)
select
    order_id
    , CASE WHEN nullif(address1, '') is not null then MD5(
        LOWER(TRIM(latitude::text))
        || LOWER(TRIM(longitude::text))
        || LOWER(TRIM(longitude::text))
        || LOWER(TRIM(REGEXP_REPLACE(address1, '[^0-9a-zA-Z]', '')))
        || LOWER(TRIM(REGEXP_REPLACE(address2, '[^0-9a-zA-Z]', '')))
    )
    ELSE NULL END as address_id
    {% for field in address_fields %}
    , {{ field }}
    {% endfor %}
from address_base
