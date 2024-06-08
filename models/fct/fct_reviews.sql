{{
config(
materialized = 'incremental',
on_schema_change='sync_all_columns'
)
}}

WITH src_reviews AS (
SELECT * FROM {{ ref('vw_reviews_cleansed') }} 
)
SELECT * FROM src_reviews
WHERE review_text is not null
{% if is_incremental() %}
AND review_date > ( select NVL( max(review_date) , '1900-01-01') from {{ this }})
{% endif %}