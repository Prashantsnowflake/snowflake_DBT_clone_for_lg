{{ config(materialized='view' , schema='base' )}}

WITH src_hosts AS (
  SELECT
    row_id
    ,try_cast(id as int ) as host_id
    ,NVL( name,'Anonymous') AS host_name
    ,is_superhost
    ,created_at
    ,updated_at
	,region
    ,current_timestamp as insert_ts
	,ROW_NUMBER() OVER(PARTITION BY host_id ORDER BY insert_ts DESC) AS RN 
	, case when host_name is null then false else true end as dq_check
  FROM
  {{ ref('raw_hosts_toprocess') }}
)
SELECT
 *
 FROM src_hosts