{#- 
	This model uses the "delete+insert" strategy using the run_date variable:
		It will delete data with scraped_date in the [run_date - 2,  run_date] range
		and insert new data of that date range.
	A few ways to define run_date variable:
		1. Define the run_date in dbt_profiles.yml (default)
		2. Overrided run_date in dbt_profiles.yml by passing vars using cli `dbt run --vars '{"run_date": "2023-10-01"}'`
		3. Run with run_date=today by deleting the run_date in dbt_profiles.yml.
-#}
{%- set run_date = var('run_date', modules.datetime.datetime.today().strftime("%Y-%m-%d")) -%}
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
		-- insert overwrite 3 day data: from run_date - 2 to run_date
		{% if is_incremental() %}
			WHERE scraped_date BETWEEN ('{{ run_date }}'::DATE - INTERVAL '{{ interval }} DAY')::DATE AND ('{{ run_date }}')::DATE
		{% endif %}
		)

	SELECT * FROM listings_stg