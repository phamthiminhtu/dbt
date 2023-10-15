WITH
  lga_snapshot AS
    (SELECT * FROM {{ ref('lga_snapshot') }})

  ,get_min_dbt_valid_from AS
    (SELECT
      MIN(dbt_valid_from) AS min_dbt_valid_from
    FROM lga_snapshot)

SELECT
  lga_code,
  lga_name,
  dbt_scd_id,
  dbt_updated_at,
  -- handle cases when the snapshot is newly updated without updated_at column
  CASE WHEN dbt_valid_from = min_dbt_valid_from THEN '1111-01-01'::DATE ELSE dbt_valid_from END AS dbt_valid_from,
  dbt_valid_to
FROM lga_snapshot
CROSS JOIN get_min_dbt_valid_from