select h.id as hubspot_contact_id,
    h.email,
    md5(lower(trim(customers.email))) as email_key,
    dm.member_id,
    st.id as stripe_customer_id,
    bc.id as bigcommerce_customer_id,
    sh.id as shopify_id,
    case when d.email is null then false else true end as duplicate,
    d.member_id as duplicate_member_id
from {{ source('hubspot', 'contacts') }} h left join
    {{ ref('dim_members') }} dm on h.email = dm.email left join
    {{ source('stripe', 'customers') }} st on h.email = st.email left join
    {{ source('bigcommerce', 'customers') }} bc on h.email = bc.email left join
    {{ source('shopify', 'customers') }} s on h.email = s.email left join
    {{ ref('_duplicates') }} d on h.email = d.email