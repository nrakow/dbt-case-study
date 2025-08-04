{{ config(materialized = 'table') }}

select distinct cuisine from  {{ ref('stg_cuisines') }}