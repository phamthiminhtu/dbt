{%- set is_active_listing = "has_availability = 't'" -%}

WITH
  facts_listings AS
    (SELECT
      *,
      DATE_TRUNC('month', scraped_date)::DATE AS month_year,
      CASE WHEN {{ is_active_listing }} THEN listing_id END AS active_listing_id,
      CASE WHEN {{ is_active_listing }} THEN price END AS active_listing_price
    FROM {{ ref('facts_listings') }})

  ,agg_data AS
    (SELECT
      property_type,
      room_type,
      accommodates,
      month_year,
      COUNT(DISTINCT active_listing_id)*100/ COUNT(DISTINCT listing_id) AS active_listing_rate,
      MIN(active_listing_price) AS min_active_listing_price,
      MAX(active_listing_price) AS max_active_listing_price,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY active_listing_price) AS median_active_listing_price,
      AVG(active_listing_price) AS avg_active_listing_price,
      COUNT(DISTINCT host_id) AS distinct_host_count,
      COUNT(DISTINCT CASE WHEN host_is_superhost = 't' THEN host_id END)*100/COUNT(DISTINCT host_id) AS super_host_rate,
      AVG(CASE WHEN {{ is_active_listing }} THEN review_scores_rating END) AS active_listing_avg_review_scores_rating,
      SUM(CASE WHEN {{ is_active_listing }} THEN 30 - availability_30 END) AS total_number_of_stays,
      (
        SUM(
          CASE WHEN {{ is_active_listing }} THEN 30 - availability_30 END
          * active_listing_price
        )
        / COUNT(DISTINCT active_listing_id)
      ) AS avg_estimated_revenue_per_listing,
      COUNT(DISTINCT active_listing_id) AS unique_active_listing_count,
      COUNT(
        DISTINCT 
        CASE WHEN NOT COALESCE({{ is_active_listing }}, FALSE) THEN listing_id END
      ) AS unique_inactive_listing_count
    FROM facts_listings
    GROUP BY
      property_type,
      room_type,
      accommodates,
      month_year)

  ,final AS
    (SELECT
      *,
      LAG(unique_active_listing_count, 1) OVER(
        PARTITION BY
          property_type,
          room_type,
          accommodates
        ORDER BY month_year
      ) AS previous_month_unique_active_listing_count,
      LAG(unique_inactive_listing_count, 1) OVER(
        PARTITION BY
          property_type,
          room_type,
          accommodates
        ORDER BY month_year
      ) AS previous_month_unique_inactive_listing_count
    FROM agg_data)
  
  SELECT
    property_type,
    room_type,
    accommodates,
    month_year,
    active_listing_rate,
    min_active_listing_price,
    max_active_listing_price,
    median_active_listing_price,
    avg_active_listing_price,
    distinct_host_count,
    super_host_rate,
    active_listing_avg_review_scores_rating,
    (
      (unique_active_listing_count - previous_month_unique_active_listing_count)*100
      / NULLIF(previous_month_unique_active_listing_count, 0)
    ) AS active_listings_percentage_change,
    (
      (unique_inactive_listing_count - previous_month_unique_inactive_listing_count)*100
      / NULLIF(previous_month_unique_inactive_listing_count, 0)
    ) AS inactive_listings_percentage_change,
    total_number_of_stays,
    avg_estimated_revenue_per_listing
  FROM final
  ORDER BY
    property_type,
    room_type,
    accommodates,
    month_year