{{ config(materialized='table') }}

-- Cuisine-level search performance metrics
-- Pulls from the unified search metrics fact table at the cuisine grain.

SELECT
    cuisine,
    search_type,
    search_query_count,
    total_clicks,
    total_impressions,
    ctr,              -- Click-through rate
    avg_position,     -- Weighted average position
    pct_unique_search_queries,
    pct_clicks,
    pct_impressions
FROM {{ ref('fct_search_metrics_unified') }}
WHERE grain = 'cuisine'
ORDER BY cuisine, search_type
