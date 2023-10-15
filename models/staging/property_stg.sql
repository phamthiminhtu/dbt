{# refer to macros/handle_dbt_valid_from.sql for more details #}

{{
  handle_dbt_valid_from(
    source=ref('property_snapshot'),
    unique_key='listing_id',
    columns_to_select=[
      'listing_id',
      'listing_neighbourhood',
      'property_type',
      'room_type',
      'accommodates',
      'updated_at'
    ])
}}