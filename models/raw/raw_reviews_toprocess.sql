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
  where  METADATA$ISUPDATE = 'FALSE' and METADATA$ACTION = 'INSERT'
 )
SELECT * FROM raw_reviews