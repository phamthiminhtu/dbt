{{ config(
    unique_key="listing_id"
) }}

WITH
	property_snapshot AS
		(SELECT * FROM {{ ref('property_snapshot') }})

	SELECT * FROM property_snapshot