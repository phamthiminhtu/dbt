{% snapshot host_snapshot %}

{{ config(
  strategy="timestamp",
  updated_at="ingestion_date",
  unique_key="host_id",
) }}

WITH
  source AS
    (SELECT
      *,
      DATE(ingestion_timestamp) AS ingestion_date,
      ROW_NUMBER() OVER(PARTITION BY host_id ORDER BY scraped_date DESC) AS _row_number
    FROM {{ source('airbnb_raw', 'listings') }})

  SELECT * FROM source
  WHERE _row_number = 1
{% endsnapshot %}