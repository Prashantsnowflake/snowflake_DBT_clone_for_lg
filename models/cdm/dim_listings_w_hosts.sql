WITH
l AS (
SELECT
*
FROM
{{ ref('dim_listings_cleansed_latest') }}
),
h AS (
SELECT *
FROM {{ ref('dim_hosts_cleansed_latest') }}
)
SELECT
l.listing_id,
l.listing_name,
l.room_type,
l.minimum_nights,
l.price,
l.host_id,
h.host_name,
h.is_superhost as host_is_superhost,
l.created_at,
GREATEST(l.updated_at, h.updated_at) as updated_at,
h.region as region,
current_timestamp() insert_ts
FROM l
LEFT JOIN h ON (h.host_id = l.host_id)