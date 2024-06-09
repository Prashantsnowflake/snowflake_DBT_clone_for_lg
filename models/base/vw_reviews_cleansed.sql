{{ config(materialized='view') }}

WITH src_reviews AS 
 ( 
   SELECT 
     row_id
    ,try_cast(listing_id as int) as listing_id
    ,date AS review_date
    ,reviewer_name
    ,comments AS review_text
    ,sentiment AS review_sentiment
    ,current_timestamp() as insert_ts
    ,metadata$filename         
    ,metadata$file_row_number
    , case when review_text is  null 
           OR reviewer_name is null then False 
		   else True END dq_check
   FROM
     {{ ref('raw_reviews_toprocess') }}
)
select *
FROM
src_reviews