{{
config(
materialized = 'incremental'
, on_schema_change='sync_all_columns'
, pre_hook='truncate table raw.raw_hosts_toprocess'
)
}}

SELECT  row_id                         
     ,id                            
     ,name                          
     ,is_superhost                  
     ,created_at                    
     ,updated_at                    
     , region    , current_timestamp as inset_ts            
     ,metadata$filename             
     ,metadata$file_row_number
from     {{ source('source_name_goibibo','hosts_stream')}}
where  METADATA$ACTION = 'INSERT'