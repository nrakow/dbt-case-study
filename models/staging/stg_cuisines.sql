{{ config(materialized = 'table') }}

SELECT
        RESTAURANT_ID,
        TRIM(f.value::STRING, '" ') AS cuisine
    FROM {{ ref('src_hex_brand_cuisine_export') }},
         LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(CUISINES)) f