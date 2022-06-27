select
    li.*
from dw.fact_line_item li
left join dw.fact_order fo
    using(order_id)
where updated_ts > '{{last_modified_at}}'