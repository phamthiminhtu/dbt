SELECT
  listing_id,
  listing_neighbourhood,
  property_type,
  room_type,
  accommodates,
  scraped_date
FROM {{ source('airbnb_raw', 'listings') }}