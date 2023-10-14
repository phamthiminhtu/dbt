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
      scraped_date AS updated_at
    FROM {{ source('airbnb_raw', 'listings') }})

  ,dedup AS
    (SELECT
      host_id,
      host_name,
      TO_DATE(host_since, 'DD/MM/YYYY') AS host_since,
      host_is_superhost,
      host_neighbourhood,
      updated_at,
      ROW_NUMBER() OVER(PARTITION BY host_id ORDER BY scraped_date DESC) AS _row_number
    FROM source)

  SELECT
    *
  FROM dedup
  WHERE _row_number = 1
{% endsnapshot %}