{{ config(
    unique_key="host_id"
) }}

WITH
	host_snapshot AS
		(SELECT * FROM {{ ref('host_snapshot') }})

	SELECT * FROM host_snapshot