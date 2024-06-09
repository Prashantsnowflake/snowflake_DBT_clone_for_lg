{% snapshot dim_hosts_history_scd %}
{{
config(
target_schema='BASE',
unique_key='host_id',
strategy='timestamp',
updated_at='updated_at',
invalidate_hard_deletes=True
)
}}

select * FROM {{ ref('dim_hosts_cleansed_latest') }}

{% endsnapshot %}