-- merge incremental_strategy preferred due to stable unique_key
{{
  config(
    materialized = 'incremental',
    unique_key = 'RESTAURANT_ID',
    incremental_strategy = 'merge'
  )
}}

SELECT
    RESTAURANT_ID,
    DOMAIN,
    _UPDATED_AT,
    _CREATED_AT,
    STATUS,
    DATA
FROM {{ source('pc_fivetran_db', 'hex_case_gsc_export') }}

{% if is_incremental() %}
WHERE _UPDATED_AT > (SELECT MAX(_UPDATED_AT) FROM {{ this }})
{% endif %}
