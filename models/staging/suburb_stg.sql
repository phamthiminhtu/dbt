{# refer to macros/handle_dbt_valid_from.sql for more details #}

{{
  handle_dbt_valid_from(
    source=ref('suburb_snapshot'),
    unique_key='suburb_name',
    columns_to_select=[
      'lga_name',
      'suburb_name'
    ])
}}