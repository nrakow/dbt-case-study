{{ config(materialized='table') }}

-- This fact model summarizes search performance metrics per cuisine and search type (Branded vs Unbranded).
-- It includes total searches, clicks, impressions, CTR, position, and share of total engagement.

with queries as (
    select
        q.restaurant_id,
        c.cuisine,
        q.search_type,
        q.clicks,
        q.impressions,
        q.position
    from {{ ref('int_queries_classified') }} q
    join {{ ref('stg_cuisines') }} c using (restaurant_id)
),

agg_metrics as (
    select
        cuisine,
        search_type,
        count(*) as num_searches,
        sum(clicks) as total_clicks,
        sum(impressions) as total_impressions,
        round(sum(clicks) * 100.0 / nullif(sum(impressions), 0), 2) as ctr,
        round(avg(position), 2) as avg_position,
        median(position) as median_position
    from queries
    group by cuisine, search_type
),

cuisine_totals as (
    select
        cuisine,
        sum(num_searches) as total_searches,
        sum(total_clicks) as total_clicks,
        sum(total_impressions) as total_impressions
    from agg_metrics
    group by cuisine
)

select
    a.cuisine,
    a.search_type,
    a.num_searches,
    a.total_clicks,
    a.total_impressions,
    a.ctr,
    a.avg_position,
    a.median_position,
    round(100.0 * a.num_searches / nullif(t.total_searches, 0), 2) as pct_searches,
    round(100.0 * a.total_clicks / nullif(t.total_clicks, 0), 2) as pct_clicks,
    round(100.0 * a.total_impressions / nullif(t.total_impressions, 0), 2) as pct_impressions
from agg_metrics a
join cuisine_totals t using (cuisine)
order by a.cuisine, a.search_type
