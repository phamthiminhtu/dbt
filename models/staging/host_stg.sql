{{ config(
    unique_key="host_id"
) }}

SELECT * FROM {{ ref('host_snapshot') }}