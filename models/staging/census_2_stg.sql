{# refer to macros/handle_dbt_valid_from.sql for more details #}

{{
  handle_dbt_valid_from(
    source=ref('census_2_snapshot'),
    unique_key='lga_code_2016',
    columns_to_select=[
      "REPLACE(lga_code_2016, 'LGA', '') AS lga_code",
      'median_age_persons',
      'median_mortgage_repay_monthly'
    ])
}}