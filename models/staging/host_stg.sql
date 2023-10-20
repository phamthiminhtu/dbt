{# refer to macros/handle_dbt_valid_from.sql for more details #}

{{ 
  handle_dbt_valid_from(
    source=ref('host_snapshot'),
    unique_key='host_id',
    columns_to_select=[
        'host_id',
        'host_name',
        "TO_DATE(host_since, 'DD/MM/YYYY') AS host_since",
        'host_is_superhost',
        'UPPER(TRIM(host_neighbourhood)) AS host_neighbourhood',
        'updated_at'
    ])
}}