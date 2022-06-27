select
    li.*
from shopify.line_items li
left join shopify.orders o
    on li.order_id = o.id
where updated_at > '{{last_modified_at}}'