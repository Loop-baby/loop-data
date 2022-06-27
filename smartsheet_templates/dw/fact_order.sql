select * from dw.fact_order
where updated_ts > '{{last_modified_at}}'