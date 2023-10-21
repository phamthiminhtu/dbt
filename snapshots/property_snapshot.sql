{% snapshot property_snapshot %}

{{ config(
  strategy="timestamp",
  updated_at="updated_at",
  unique_key="listing_id",
) }}

WITH
  source AS
    (SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY listing_id ORDER BY scraped_date DESC) AS _row_number
    FROM {{ ref('raw_property') }}
    )

  SELECT
    listing_id,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    scraped_date::TIMESTAMP AS updated_at
  FROM source
  WHERE _row_number = 1
{% endsnapshot %}