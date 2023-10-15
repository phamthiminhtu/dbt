{% snapshot lga_snapshot %}

{{ config(
  strategy="timestamp",
  updated_at="ingestion_date",
  unique_key="lga_code"
) }}

SELECT
  *,
  DATE(ingestion_timestamp) AS ingestion_date
FROM {{ source('airbnb_raw', 'nsw_lga_code') }}
{% endsnapshot %}