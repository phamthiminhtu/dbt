WITH
  suburb_snapshot AS
    (SELECT * FROM {{ ref('suburb_snapshot') }})

  ,get_min_dbt_valid_from AS
    (SELECT
      MIN(dbt_valid_from) AS min_dbt_valid_from
    FROM suburb_snapshot)

SELECT
  lga_name,
  suburb_name,
  dbt_scd_id,
  dbt_updated_at,
  -- handle cases when the snapshot is newly updated without updated_at column
  CASE WHEN dbt_valid_from = min_dbt_valid_from THEN '1111-01-01'::DATE ELSE dbt_valid_from END AS dbt_valid_from,
  dbt_valid_to
FROM suburb_snapshot
CROSS JOIN get_min_dbt_valid_from