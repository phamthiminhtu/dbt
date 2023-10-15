{% snapshot suburb_snapshot %}

{{ config(
  strategy="timestamp",
  updated_at="ingestion_date",
  unique_key="suburb_name"
) }}

SELECT
  *,
  DATE(ingestion_timestamp) AS ingestion_date
FROM {{ source('airbnb_raw', 'nsw_lga_suburb') }}
{% endsnapshot %}