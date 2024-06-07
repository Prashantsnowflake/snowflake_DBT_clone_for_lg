{{ config(materialized='view') }}

WITH src_hosts AS (
SELECT
*
FROM
{{ ref('raw_hosts_live_toprocess') }}
)
SELECT
row_id,
id as host_id,
NVL(
name,
'Anonymous'
) AS host_name,
is_superhost,
created_at,
updated_at,
current_timestamp as insert_ts
FROM
src_hosts