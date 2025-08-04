

{{ config(materialized='table') }}

-- This fact model summarizes search performance metrics per restaurant and search type (Branded vs Unbranded).
-- It includes total searches, clicks, impressions, CTR, position, and share of total engagement.

with agg_metrics as (
  select
    restaurant_id,
    search_type,
    count(*) as num_searches,
    sum(clicks) as total_clicks,
    sum(impressions) as total_impressions,
    round(100.0 * sum(clicks) / nullif(sum(impressions), 0), 2) as ctr,
    round(avg(position), 2) as avg_position,
    MEDIAN(position) AS median_position
  from {{ ref('int_queries_classified') }}
  group by restaurant_id, search_type
),

totals as (
  select
    restaurant_id,
    sum(num_searches) as total_searches,
    sum(total_clicks) as total_clicks,
    sum(total_impressions) as total_impressions
  from agg_metrics
  group by restaurant_id
)

select
  a.restaurant_id,
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
join totals t on a.restaurant_id = t.restaurant_id
order by a.restaurant_id, a.search_type
