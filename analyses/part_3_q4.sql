WITH

  census AS
    (SELECT
      REPLACE(lga_code_2016, 'LGA', '') AS lga_code,
      median_mortgage_repay_monthly
    FROM "postgres"."airbnb_raw"."census_lga_g02")

  ,facts_listings AS
    (SELECT
      *,
      CASE WHEN has_availability = 't' THEN price END AS active_listing_price,
      CASE WHEN has_availability = 't' THEN 30 - availability_30 END AS number_of_stays
    FROM "postgres"."warehouse"."facts_listings")

  ,dim_lga AS
    (SELECT
      lga_code,
      UPPER(lga_name) AS lga_name_upper
    FROM "postgres"."warehouse"."dim_lga"
    WHERE dbt_valid_to IS NULL)
  
  ,dim_host AS
    (SELECT
      host_id,
      host_neighbourhood_upper AS current_host_neighbourhood_upper
    FROM "postgres"."warehouse"."dim_host"
    WHERE dbt_valid_to IS NULL)

  ,dim_suburb AS
    (SELECT
      lga_name,
      suburb_name
    FROM "postgres"."warehouse"."dim_suburb"
    WHERE dbt_valid_to IS NULL)

  ,get_host_listing AS
    (SELECT
      host_id,
      SUM(number_of_stays * active_listing_price) AS estimated_revenue,
      COUNT(DISTINCT listing_id) > 1 AS is_host_having_multiple_listing
    FROM facts_listings
    GROUP BY
      host_id)

  ,get_host_current_lga AS
    (SELECT
      dh.host_id,
      dl.lga_code
    FROM dim_host AS dh
    LEFT JOIN dim_suburb AS ds
    ON dh.current_host_neighbourhood_upper = ds.suburb_name
    LEFT JOIN dim_lga AS dl
    ON ds.lga_name = dl.lga_name_upper)

  ,get_host_with_one_listing_info AS
    (SELECT
      ghl.host_id,
      ghl.estimated_revenue,
      ghcl.lga_code,
      ghl.estimated_revenue > (c.median_mortgage_repay_monthly * 12) as has_median_mortgage_repay_annualised_covered
    FROM get_host_listing AS ghl
    LEFT JOIN get_host_current_lga AS ghcl
    ON ghl.host_id = ghcl.host_id
    LEFT JOIN census AS c
    ON ghcl.lga_code = c.lga_code
    WHERE
      NOT ghl.is_host_having_multiple_listing
      AND ghcl.lga_code IS NOT NULL)

  ,final AS
    (SELECT
      has_median_mortgage_repay_annualised_covered,
      COUNT(*) AS host_count
    FROM get_host_with_one_listing_info
    GROUP BY has_median_mortgage_repay_annualised_covered)
  
  SELECT
    *,
    (host_count*100/SUM(host_count) OVER())::FLOAT AS percent
  FROM final