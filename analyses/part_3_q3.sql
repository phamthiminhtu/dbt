WITH
  facts_listings AS
    (SELECT * FROM "postgres"."warehouse"."facts_listings")
  
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
      COUNT(DISTINCT listing_id) > 1 AS is_host_having_multiple_listing
    FROM facts_listings
    GROUP BY
      host_id)

  ,get_host_current_lga AS
    (SELECT
      dh.host_id,
      COALESCE(ds.lga_name, 'OUTSIDE_NSW') AS current_host_lga_name
    FROM dim_host AS dh
    LEFT JOIN dim_suburb AS ds
    ON dh.current_host_neighbourhood_upper = ds.suburb_name)

  ,get_host_listings_lga AS
    (SELECT DISTINCT
      host_id,
      UPPER(listing_neighbourhood) AS listing_neighbourhood_lga_name
    FROM facts_listings)

 ,get_host_listing_neighbourhood_info AS
  (SELECT
      ghll.host_id,
      MAX(
        CASE
          WHEN ghcl.current_host_lga_name = ghll.listing_neighbourhood_lga_name THEN 1
          ELSE 0
        END
      ) AS has_listing_in_neighbourhood
    FROM get_host_listings_lga AS ghll
    LEFT JOIN get_host_current_lga AS ghcl
    ON ghll.host_id = ghcl.host_id
    GROUP BY ghll.host_id)
  
  ,final AS
    (SELECT
      ghl.is_host_having_multiple_listing,
      COUNT(ghlni.host_id) AS host_count
    FROM get_host_listing_neighbourhood_info AS ghlni
    LEFT JOIN get_host_listing AS ghl
    ON ghlni.host_id = ghl.host_id
    WHERE ghlni.has_listing_in_neighbourhood = 1
    GROUP BY
      ghl.is_host_having_multiple_listing)

  SELECT
    is_host_having_multiple_listing,
    (host_count/ SUM(host_count) OVER ())::FLOAT*100 AS percent
  FROM final