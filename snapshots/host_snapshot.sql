{% snapshot host_snapshot %}

{{ config(
  strategy="timestamp",
  updated_at="updated_at",
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
    scraped_date::TIMESTAMP AS updated_at
  FROM source
  WHERE _row_number = 1 -- get the most recent info of a listing_id in case of duplication
{% endsnapshot %}