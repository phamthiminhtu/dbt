{%- set is_active_listing = "has_availability = 't'" -%}

WITH
  facts_listings AS
    (SELECT
      *,
      DATE_TRUNC('month', scraped_date)::DATE AS month_year,
      CASE WHEN {{ is_active_listing }} THEN listing_id END AS active_listing_id,
      CASE WHEN {{ is_active_listing }} THEN price END AS active_listing_price,
      30 - availability_30 AS number_of_stays
    FROM {{ ref('facts_listings') }})

  SELECT
    host_neighbourhood_lga,
    month_year,
    COUNT(DISTINCT host_id) AS distinct_host_count,
    SUM(
      CASE WHEN {{ is_active_listing }} THEN number_of_stays END
      * active_listing_price
    ) AS estimated_revenue,
    (
      SUM(
        CASE WHEN {{ is_active_listing }} THEN number_of_stays END
        * active_listing_price
      )
      / COUNT(DISTINCT host_id)
    ) AS avg_estimated_revenue_per_host
  FROM facts_listings
  GROUP BY
    host_neighbourhood_lga,
    month_year
