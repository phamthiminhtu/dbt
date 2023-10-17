{% snapshot property_snapshot %}

{{ config(
  strategy="timestamp",
  updated_at="ingestion_timestamp",
  unique_key="listing_id",
) }}

WITH
  source AS
    (SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY listing_id ORDER BY scraped_date DESC) AS _row_number
    FROM {{ source('airbnb_raw', 'listings') }}
    )

  SELECT
    listing_id,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    ingestion_timestamp
  FROM source
  WHERE _row_number = 1
{% endsnapshot %}