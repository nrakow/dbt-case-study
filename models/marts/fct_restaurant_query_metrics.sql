

{{ config(materialized='table') }}

-- This fact model summarizes search performance metrics per restaurant and search type (Branded vs Unbranded).
-- It includes total search queries, clicks, impressions, CTR, position, and share of total engagement.

with agg_metrics as (
  select
    restaurant_id,
    search_type,
    count(DISTINCT query_raw) as search_query_count,
    sum(clicks) as total_clicks,
    sum(impressions) as total_impressions,
    round(100.0 * sum(clicks) / nullif(sum(impressions), 0), 2) as ctr,
    SUM(position * impressions) / NULLIF(SUM(impressions), 0) AS avg_position -- using weighted average
from {{ ref('int_queries_classified') }}
  group by restaurant_id, search_type
),

totals as (
  select
    restaurant_id,
    sum(search_query_count) as total_queries,
    sum(total_clicks) as total_clicks,
    sum(total_impressions) as total_impressions
  from agg_metrics
  group by restaurant_id
)

select
  a.restaurant_id,
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
join totals t using(restaurant_id)
order by a.restaurant_id, a.search_type
