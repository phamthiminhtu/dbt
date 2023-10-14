{{ config(
    unique_key="lga_code"
) }}

SELECT * FROM {{ ref('lga_snapshot') }}