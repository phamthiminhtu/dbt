{{ config(
    unique_key="listing_id"
) }}

WITH
	property_stg AS
		(SELECT * FROM {{ ref('property_stg') }})

	SELECT * FROM property_stg