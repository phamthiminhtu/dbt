{{ config(
    unique_key="listing_id"
) }}

SELECT * FROM {{ ref('property_snapshot') }}