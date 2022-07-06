select
    p.id as sku
    , p.title as product_name
    , p.created_at as product_created_ts
    , p.updated_at as product_updated_ts
    , p.published_at as product_published_ts
    , p.handle as product_page_handle
    , p.status as product_status
    , p.tags as product_tags
from {{ source('shopify', 'products') }} as p