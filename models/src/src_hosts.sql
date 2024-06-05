WITH raw_hosts AS (
SELECT
*
FROM
DB_VN252_PM.RAW.RAW_HOSTS
)
SELECT
id AS host_id,
NAME AS host_name,
is_superhost,
created_at,
updated_at
FROM
raw_hosts