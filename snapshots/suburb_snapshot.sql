{% snapshot suburb_snapshot %}

{{ config(
  strategy="timestamp",
  updated_at="ingestion_timestamp",
  unique_key="suburb_name"
) }}

SELECT * FROM {{ ref('raw_suburb') }}
{% endsnapshot %}