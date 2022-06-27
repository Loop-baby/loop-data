SELECT DISTINCT
  member_id_map.email_key,
  member_id_map.member_id
FROM {{ ref('member_id_map') }} as member_id_map