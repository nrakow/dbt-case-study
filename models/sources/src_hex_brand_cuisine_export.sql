{{ config(materialized = 'table') }}

SELECT
    RESTAURANT_ID,
    CUISINES
FROM {{ source('pc_fivetran_db', 'hex_brand_cuisine_export') }}