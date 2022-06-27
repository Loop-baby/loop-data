# Loop Analytics

## Structure
### DBT
 - Place holder for more info
### (E)mpirical (R)oot ETL - ERetl
 - Place holder for more info

## Additional Notes

### Dealing with customers that have multiple accounts

*Consolidate the accounts under 1 member_id using* `dbt.data._duplicates.csv`

Under the loop-dbt/data/ directory exists a file called `_duplicates.csv` which is a hard coded table to identify customers who have more than one account. We use that to identify the preferred account by providing the preferred `member_id`

For each system where the customer has a duplicate record there should be an entry in the `_duplicates.csv` table. Choosing the shared/primary member_id of the preferred record.

The ephemeral table `ephemeral.member_id_map.sql` then uses that duplicates table to map users from any of the connected systems (e.g. hubspot, spotify, etc) to the *primary* `member_id`

All the other tables should be using the member_id_map to connect a customer to the primary `member_id`.

In the case that a system does not have access to one of the system's ids like the hubspot.contact_id, but does have access the the email, one can get the primary member_id by looking up the md5(lower(trim(*email*))) as the `email_key` in the `ephemeral.member_id_by_email` mapping. 

for example:
```
select 
    member_id_by_email.member_id
    , my_table.*
from 
    my_table
    left join {{ ref('member_id_by_email') }} as member_id_by_email 
        on md5(lower(trim(my_table.email_field_with_weird_name))) = member_id_by_email.email_key
```