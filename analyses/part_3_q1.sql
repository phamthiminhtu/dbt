WITH
  facts_listings AS
    (SELECT
      *,
      CASE WHEN has_availability = 't' THEN listing_id END AS active_listing_id,
      CASE WHEN has_availability = 't' THEN price END AS active_listing_price,
      CASE WHEN has_availability = 't' THEN 30 - availability_30 END AS number_of_stays
    FROM "postgres"."warehouse"."facts_listings")

  ,agg AS
    (SELECT
      listing_neighbourhood,
      (
        SUM(active_listing_price * number_of_stays)
        / COUNT(DISTINCT active_listing_id)
      ) AS estimated_revenue_per_active_listings
    FROM facts_listings
    GROUP BY
      listing_neighbourhood)

  SELECT
    *,
    RANK() OVER(ORDER BY estimated_revenue_per_active_listings DESC) AS ranking
  FROM agg
  ORDER BY ranking