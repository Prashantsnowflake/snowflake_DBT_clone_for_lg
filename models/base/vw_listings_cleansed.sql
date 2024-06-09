{{ config(materialized='view' , schema='base')}}

WITH src_listings AS (
  SELECT
     try_cast(listing_id as int ) as listing_id
    ,listing_name
    ,room_type
    ,CASE WHEN minimum_nights = 0 THEN 1 ELSE minimum_nights::int END AS minimum_nights
    ,try_cast(host_id as int ) as host_id
    ,REPLACE(price_str,'$') :: NUMBER(10,2) AS price
    ,created_at
    ,updated_at
    ,current_timestamp() insert_ts
    ,ROW_NUMBER() OVER(PARTITION BY listing_id ORDER BY insert_ts DESC) AS RN 
    ,case when listing_name is null   
          OR  listing_id is null 
    	  OR host_id is null then FALSE
    else TRUE end as dq_check
   FROM
  {{ ref('raw_listings_toprocess') }}
)
SELECT
 * 
  FROM src_listings