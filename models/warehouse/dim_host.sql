{{ config(
    unique_key="host_id"
) }}

WITH
	host_stg AS
		(SELECT * FROM {{ ref('host_stg') }})

	SELECT * FROM host_stg