{{
    config(materialized='incremental' 
    , unique_key='host_id'
    , on_schema_change='sync_all_columns'
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
from {{ ref('vw_host_cleansed') }}

{% if is_incremental() %}
--in case case this condition is not required
  where updated_at >= ( select NVL( max(updated_at) , '1900-01-01') from {{ this }})
{% endif %}