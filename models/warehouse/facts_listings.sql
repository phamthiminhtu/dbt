{%- set run_date = var('run_date') -%}
{%- set interval = var('interval') -%}

{{ config(
	partition_by=['scraped_date'],
	unique_key='scraped_date',
	partition_type="date",
	materialized='incremental',
	incremental_strategy='delete+insert',
) 
}}

WITH
	listings_stg AS
		(SELECT * FROM {{ ref("listings_stg") }}
		{% if is_incremental() %}
			WHERE scraped_date BETWEEN ('{{ run_date }}'::DATE - INTERVAL '{{ interval }} DAY')::DATE AND ('{{ run_date }}')::DATE
		{% endif %}
		)

	SELECT * FROM listings_stg