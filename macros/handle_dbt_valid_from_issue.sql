{% macro handle_dbt_valid_from(source, columns_to_select) -%}
  WITH
    source_data AS
      (SELECT * FROM {{ source }})

    ,get_min_dbt_valid_from AS
      (SELECT
        MIN(dbt_valid_from) AS min_dbt_valid_from
      FROM source_data)

    SELECT
      {{ columns_to_select | join(',\n')}},
      dbt_scd_id,
      dbt_updated_at,
      -- handle cases when the the snapshot tables are newly created
      CASE WHEN dbt_valid_from = min_dbt_valid_from THEN '1111-01-01'::DATE ELSE dbt_valid_from END AS dbt_valid_from,
      dbt_valid_to
    FROM source_data
    CROSS JOIN get_min_dbt_valid_from

{%- endmacro %}