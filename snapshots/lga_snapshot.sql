{% snapshot lga_snapshot %}

{{ config(
  strategy="check",
  unique_key="lga_code",
  check_cols='all'
) }}

SELECT * FROM {{ source('airbnb_raw', 'nsw_lga_code') }}
{% endsnapshot %}