{% snapshot dim_listings_history_scd %}
{{
config(
target_schema='CDM',
unique_key='listing_id',
strategy='timestamp',
updated_at='updated_at',
invalidate_hard_deletes=True
)
}}

select * FROM {{ ref('dim_listings_cleansed_latest') }}

{% endsnapshot %}