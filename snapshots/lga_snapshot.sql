{% snapshot lga_snapshot %}

{{ config(
  strategy="timestamp",
  updated_at="ingestion_timestamp",
  unique_key="lga_code"
) }}

SELECT * FROM {{ ref('raw_lga') }}
{% endsnapshot %}