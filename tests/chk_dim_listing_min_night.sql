SELECT
*
FROM
{{ ref('dim_listings_cleansed_latest') }}
WHERE minimum_nights < 1
LIMIT 10