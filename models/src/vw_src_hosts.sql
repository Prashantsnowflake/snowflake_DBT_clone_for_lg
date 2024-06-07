{{ config(materialized='view') }}

WITH raw_hosts AS (

    SELECT * from {{ source('source_name_goibibo','hosts_stream')}}
)
SELECT
    {# id AS host_id,
    NAME AS host_name,
    is_superhost,
    created_at,
    updated_at ,* #} *
FROM
    raw_hosts


