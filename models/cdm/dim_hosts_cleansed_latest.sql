{{
    config(materialized='incremental' 
    , unique_key='host_id'
    , on_schema_change='sync_all_columns', schema='cdm', post_hook="truncate table raw.raw_hosts_toprocess"
     )
}}

select
row_id,
host_id,
host_name,
is_superhost,
created_at,
updated_at,region ,
current_timestamp as insert_ts
from {{ ref('vw_hosts_cleansed') }} where dq_check = true and rn  = 1

{% if is_incremental() %}
--in case case this condition is not required
  and  updated_at >= ( select NVL( max(updated_at) , '1900-01-01') from {{ this }})
{% endif %}