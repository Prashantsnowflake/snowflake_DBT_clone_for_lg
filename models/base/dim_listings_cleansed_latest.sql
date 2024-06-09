 {{
    config(materialized='incremental' 
    , unique_key='listing_id'
    , on_schema_change='sync_all_columns' , schema='cdm'

     )
}}

 WITH src_listings AS 
 (
  SELECT
    listing_id,
    listing_name,
    room_type,
    minimum_nights,
    host_id,
    price,
    created_at,
    updated_at,
	current_timestamp() insert_ts
  FROM
    {{ ref('vw_listings_cleansed') }} where  dq_check = true

)
select * from src_listings

{% if is_incremental() %}
--in case case this condition is not required
  where updated_at >= ( select NVL( max(updated_at) , '1900-01-01') from {{ this }})
{% endif %}