{{
config(
materialized = 'incremental'
, schema='cdm' , post_hook="truncate table raw.raw_listings_toprocess"
)
}}

WITH fct_reviews AS (
SELECT 
{{ dbt_utils.generate_surrogate_key(['fct.listing_id', 'review_id', 'host.host_id']) }} AS sr_key
,fct.review_id
  ,fct.listing_id
  ,list.LISTING_NAME
  ,host.HOST_ID
  ,host.host_name
  ,fct.review_date
  ,fct.reviewer_name
  ,fct.review_text
  ,fct.review_sentiment
 FROM {{ ref('fct_reviews') }} fct 
   left join {{ ref('dim_listings_cleansed_latest') }} list on fct.listing_id = list.listing_id
   left join {{ ref('dim_hosts_cleansed_latest') }}  host on list.HOST_ID = host.HOST_ID  
     WHERE 1 = 1 
  {% if is_incremental() %}
    AND review_date > (SELECT COALESCE(MAX(review_date), '1900-01-01') FROM {{ this }})
  {% endif %}

)
select * from fct_reviews