{{ config(materialized='table') }}

-- This fact model summarizes search performance metrics per cuisine and search type (Branded vs Unbranded).
-- It includes total search queries, clicks, impressions, CTR, position, and share of total engagement.

with agg_metrics as (
    select
        c.cuisine,
        q.search_type,
        count(DISTINCT q.query_raw) as search_query_count,
        sum(q.clicks) as total_clicks,
        sum(q.impressions) as total_impressions,
        round(sum(q.clicks) * 100.0 / nullif(sum(q.impressions), 0), 2) as ctr,
        SUM(position * impressions) / NULLIF(SUM(impressions), 0) AS avg_position -- using weighted average
    from {{ ref('int_queries_classified') }} q
    join {{ ref('stg_cuisines') }} c using (restaurant_id)
    group by c.cuisine, q.search_type
),

cuisine_totals as (
    select
        cuisine,
        sum(search_query_count) as total_queries,
        sum(total_clicks) as total_clicks,
        sum(total_impressions) as total_impressions
    from agg_metrics
    group by cuisine
)

select
    a.cuisine,
    a.search_type,
    a.search_query_count,
    a.total_clicks,
    a.total_impressions,
    a.ctr,
    a.avg_position,
    round(100.0 * a.search_query_count / nullif(t.total_queries, 0), 2) as pct_unique_search_queries,
    round(100.0 * a.total_clicks / nullif(t.total_clicks, 0), 2) as pct_clicks,
    round(100.0 * a.total_impressions / nullif(t.total_impressions, 0), 2) as pct_impressions
from agg_metrics a
join cuisine_totals t using (cuisine)
order by a.cuisine, a.search_type
