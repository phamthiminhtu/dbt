SELECT
  host_id,
  host_name,
  host_since,
  host_is_superhost,
  host_neighbourhood,
  scraped_date
FROM {{ source('airbnb_raw', 'listings') }}