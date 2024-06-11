 {{
    config(materialized='incremental' 
    , on_schema_change='sync_all_columns',schema='control_audit' 
     )
}}

--no need of incremental condition  as these records set based on stream CDC data
 with reject_data_listing as 
 (
 select 
 'raw_listings_toprocess' table_name , OBJECT_CONSTRUCT_KEEP_NULL(*)::variant as json_records , 'reject for DQ check' Status 
 from {{ ref('vw_listings_cleansed') }}
 where dq_check = False 
 )
 ,reject_data_hosts as 
 (
 select 
 'raw_hosts_toprocess' table_name , OBJECT_CONSTRUCT_KEEP_NULL(*)::variant as json_records , 'reject for DQ check' Status 
 from {{ ref('vw_hosts_cleansed') }}
 where dq_check = False 
 )
 , reject_data_reviews as 
 (
 select 
 'raw_reviews_toprocess' table_name , OBJECT_CONSTRUCT_KEEP_NULL(*)::variant as json_records , 'reject for DQ check' Status 
 from {{ ref('vw_reviews_cleansed') }}
 where dq_check = False 
 )
 select * from reject_data_listing
 union all  select * from reject_data_hosts
 union all  select * from reject_data_reviews