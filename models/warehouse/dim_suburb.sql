{{ config(
    unique_key="suburb_name"
) }}

SELECT * FROM {{ ref('suburb_snapshot') }}