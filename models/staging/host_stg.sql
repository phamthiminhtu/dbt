{# refer to macros/handle_dbt_valid_from.sql for more details #}

{{ 
  handle_dbt_valid_from(
    source=ref('host_snapshot'),
    unique_key='host_id',
    columns_to_select=[
        'host_id',
        'host_name',
        'host_since',
        'host_is_superhost',
        'host_neighbourhood',
        'UPPER(TRIM(host_neighbourhood)) AS host_neighbourhood_upper',
        'updated_at'
    ])
}}