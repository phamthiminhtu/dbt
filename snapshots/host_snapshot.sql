{% snapshot host_snapshot %}

{{ config(
  strategy="timestamp",
  updated_at="ingestion_timestamp",
  unique_key="host_id",
) }}

WITH
  source AS
    (SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY host_id ORDER BY scraped_date DESC) AS _row_number
    FROM {{ source('airbnb_raw', 'listings') }})

  SELECT
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    host_neighbourhood,
    ingestion_timestamp
  FROM source
  WHERE _row_number = 1
{% endsnapshot %}