WITH src_reviews AS ( 
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
FROM
{{ ref('raw_reviews_toprocess') }}
)
select *
FROM
src_reviews