{{
config(
materialized = 'incremental'
, on_schema_change='sync_all_columns' ,schema='base' , post_hook="truncate table raw.raw_reviews_toprocess"
)
}}

WITH src_reviews AS (
SELECT * FROM {{ ref('vw_reviews_cleansed') }} 
)
SELECT 
{{ dbt_utils.generate_surrogate_key(['listing_id', 'review_date', 'reviewer_name', 'review_text']) }} AS review_id
,listing_id
,review_date
,reviewer_name
,review_text
,review_sentiment
,current_timestamp() as insert_ts
FROM src_reviews
WHERE review_text is not null and dq_check = true 
{% if is_incremental() %}
and  review_date > ( select NVL( max(review_date) , '1900-01-01') from {{ this }})
{% endif %}