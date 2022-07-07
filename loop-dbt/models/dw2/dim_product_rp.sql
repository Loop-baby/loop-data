select
    p.id as sku --maybe we should be using sku as sku
    , p.name as product_name
    , p.date_created as product_created_ts
    , p.date_updated as product_updated_ts
    , null as product_published_ts
    , json_extract_path_text(p.custom_url, 'url') as product_page_handle
    , p.availability as product_status --not sure this is accurate
    , null as product_tags --would need to pull in cateogries for this possibly
from {{ source('bigcommerce', 'products') }} as p