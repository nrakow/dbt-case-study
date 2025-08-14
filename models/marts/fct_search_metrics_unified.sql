-- grain and search_type are low-cardinality fields that are frequently filtered in queries,
-- so clustering helps reduce scanned partitions and improve query performance.
{{
  config(
    materialized='table',
    cluster_by=['grain', 'search_type']
  )
}}

-- ============================================================
-- Unified Search Metrics Fact Table
-- ------------------------------------------------------------
-- Grains:
--   restaurant: One row per restaurant_id + search_type
--   cuisine:    One row per cuisine + search_type
--   global:     One row per search_type
--
-- Metric definitions:
--   search_query_count  : Count of unique search queries at this grain.
--   total_clicks        : Sum of clicks for all queries in this group.
--   total_impressions   : Sum of impressions for all queries in this group.
--   ctr                 : Click-through rate = total_clicks / total_impressions * 100
--   avg_position        : Weighted average position = sum(position * impressions) / sum(impressions)
--   pct_unique_search_queries : Share of unique queries for this grain compared to its parent total.
--   pct_clicks          : Share of clicks for this grain compared to its parent total.
--   pct_impressions     : Share of impressions for this grain compared to its parent total.
-- ============================================================

-- 1) Base: one row per restaurant_id + query_raw + search_type
with base as (
    select
        restaurant_id,
        search_type,
        query_raw,
        clicks,
        impressions,
        position,
        domain
    from {{ ref('int_queries_classified') }}
),

-- 2) Restaurant-level aggregation
agg_restaurant as (
    select
        restaurant_id,
        domain,
        search_type,
        count(distinct query_raw) as search_query_count,
        sum(clicks) as total_clicks,
        sum(impressions) as total_impressions,
        sum(position * impressions) / nullif(sum(impressions), 0) as avg_position
    from base
    group by restaurant_id, domain, search_type
),

restaurant_with_pct as (
    select
        'restaurant' as grain,
        cast(restaurant_id as string) as restaurant_id,
        null as cuisine,
        search_type,
        domain,
        search_query_count,
        total_clicks,
        total_impressions,
        round(100.0 * total_clicks / nullif(total_impressions, 0), 2) as ctr,
        avg_position,
        round(100.0 * search_query_count /
              nullif(sum(search_query_count) over (partition by restaurant_id), 0), 2) as pct_unique_search_queries,
        round(100.0 * total_clicks /
              nullif(sum(total_clicks) over (partition by restaurant_id), 0), 2) as pct_clicks,
        round(100.0 * total_impressions /
              nullif(sum(total_impressions) over (partition by restaurant_id), 0), 2) as pct_impressions
    from agg_restaurant
),

-- 3) Cuisine-level aggregation (full attribution)
expanded_for_cuisine as (
    select
        c.cuisine,
        r.search_type,
        r.search_query_count,
        r.total_clicks,
        r.total_impressions,
        r.avg_position
    from agg_restaurant r
    join {{ ref('stg_cuisines') }} c using (restaurant_id)
),

agg_cuisine as (
    select
        cuisine,
        search_type,
        sum(search_query_count) as search_query_count,
        sum(total_clicks) as total_clicks,
        sum(total_impressions) as total_impressions,
        sum(avg_position * total_impressions) / nullif(sum(total_impressions), 0) as avg_position
    from expanded_for_cuisine
    group by cuisine, search_type
),

cuisine_with_pct as (
    select
        'cuisine' as grain,
        null as restaurant_id,
        cuisine,
        search_type,
        null as domain,
        search_query_count,
        total_clicks,
        total_impressions,
        round(100.0 * total_clicks / nullif(total_impressions, 0), 2) as ctr,
        avg_position,
        round(100.0 * search_query_count /
              nullif(sum(search_query_count) over (partition by cuisine), 0), 2) as pct_unique_search_queries,
        round(100.0 * total_clicks /
              nullif(sum(total_clicks) over (partition by cuisine), 0), 2) as pct_clicks,
        round(100.0 * total_impressions /
              nullif(sum(total_impressions) over (partition by cuisine), 0), 2) as pct_impressions
    from agg_cuisine
),

-- 4) Global-level aggregation (avoid multi-cuisine double-counting)
agg_global as (
    select
        search_type,
        sum(search_query_count) as search_query_count,
        sum(total_clicks) as total_clicks,
        sum(total_impressions) as total_impressions,
        sum(avg_position * total_impressions) / nullif(sum(total_impressions), 0) as avg_position
    from agg_restaurant
    group by search_type
),

global_with_pct as (
    select
        'global' as grain,
        null as restaurant_id,
        null as cuisine,
        search_type,
        null as domain,
        search_query_count,
        total_clicks,
        total_impressions,
        round(100.0 * total_clicks / nullif(total_impressions, 0), 2) as ctr,
        avg_position,
        round(100.0 * search_query_count /
              nullif(sum(search_query_count) over (), 0), 2) as pct_unique_search_queries,
        round(100.0 * total_clicks /
              nullif(sum(total_clicks) over (), 0), 2) as pct_clicks,
        round(100.0 * total_impressions /
              nullif(sum(total_impressions) over (), 0), 2) as pct_impressions
    from agg_global
)

-- 5) Final unified output
select * from restaurant_with_pct
union all
select * from cuisine_with_pct
union all
select * from global_with_pct
