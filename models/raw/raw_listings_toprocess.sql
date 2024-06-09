{{
config(
materialized = 'incremental'
, on_schema_change='sync_all_columns'
, pre_hook='truncate table raw.raw_listings_toprocess'
)
}}


SELECT   row_id                    
 ,id    as listing_id                    
 ,listing_url               
 ,name  as listing_name                    
 ,room_type                 
 ,minimum_nights            
 ,host_id                   
 ,price     as price_str                
 ,created_at                
 ,updated_at                
 ,insert_ts                
 ,metadata$filename         
 ,metadata$file_row_number  
from     {{ source('source_name_goibibo','listing_stream')}}
where  METADATA$ACTION = 'INSERT'