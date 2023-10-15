{#-
  This macro is used to handle cases when the snapshot tables are created 
  after the data was first generated.
  Modularize this to reduce duplicate code.
-#}

{% macro handle_dbt_valid_from(source, columns_to_select, unique_key) -%}
  WITH
    source_data AS
      (SELECT
        *,
        {{ unique_key }} AS unique_key
      FROM {{ source }})

    ,get_min_dbt_valid_from AS
      (SELECT
        unique_key,
        MIN(dbt_valid_from) AS min_dbt_valid_from
      FROM source_data
      GROUP BY unique_key)

    SELECT
      {{ columns_to_select | join(',\n')}},
      dbt_scd_id,
      dbt_updated_at,
      -- handle cases when snapshot tables are created after the data was first generated
      CASE WHEN dbt_valid_from = m.min_dbt_valid_from THEN '1111-01-01'::DATE ELSE dbt_valid_from END AS dbt_valid_from,
      dbt_valid_to
    FROM source_data AS s
    LEFT JOIN get_min_dbt_valid_from AS m
    ON s.unique_key = m.unique_key

{%- endmacro %}