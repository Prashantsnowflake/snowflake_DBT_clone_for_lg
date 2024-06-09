{{
config(
materialized = 'incremental'
, on_schema_change='sync_all_columns',schema='raw' 
)
}}

WITH raw_reviews AS 
 (
  SELECT
   row_id
  ,listing_id
  ,date
  ,reviewer_name
  ,comments 
  ,sentiment 
  ,current_timestamp() as insert_ts
  ,metadata$filename         
  ,metadata$file_row_number
  FROM {{ source('source_name_goibibo','reviews_stream')}} 
  where 1  = 1  
  and METADATA$ACTION = 'INSERT'
 )
SELECT * FROM raw_reviews

{% if is_incremental() %}
 where insert_ts > ( select NVL( max(insert_ts) , '1900-01-01') from {{ this }})
{% endif %}


