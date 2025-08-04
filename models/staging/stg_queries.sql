{{ config(materialized = 'table') }}

    SELECT
        RESTAURANT_ID,
        DOMAIN,
        f.value:clicks::INT AS clicks,
        f.value:ctr::FLOAT AS ctr,
        f.value:impressions::INT AS impressions,
        f.value:position::FLOAT AS position,
        TRIM(f.value:keys[0]::STRING) AS query
    FROM {{ ref('src_hex_case_gsc_export') }},
    LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(DATA):rows) f