{% snapshot listings_snapshot %}

{{ config(
  strategy="timestamp",
  updated_at="updated_at",
  unique_key="listing_id",
) }}

WITH
  source AS
    (SELECT
      *,
      scraped_date AS updated_at
    FROM {{ source('airbnb_raw', 'listings') }})

  ,dedup AS
    (SELECT
      listing_id,
      listing_neighbourhood,
      property_type,
      room_type,
      accommodates,
      updated_at,
      ROW_NUMBER() OVER(PARTITION BY listing_id ORDER BY scraped_date DESC) AS _row_number
    FROM source)

  SELECT
    *
  FROM dedup
  WHERE _row_number = 1
{% endsnapshot %}