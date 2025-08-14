
# 📦 Project Overview

This dbt project analyzes Google Search Console (GSC) data for Owner.com restaurants to measure SEO performance across branded and unbranded searches at multiple aggregation levels.

The transformation pipeline:

1. Flatten and clean raw JSON from GSC exports.
2. Classify search queries as branded or unbranded.
3. Aggregate metrics at the restaurant, cuisine, and global levels.
4. Calculate the share of queries, clicks, and impressions by search type.
5. Produce additional views (restaurant-level and cuisine-level) sourced from a unified aggregated model to avoid duplication.
6. Identify unbranded CTR opportunity using position-based benchmarks and quantify potential incremental clicks.

---

## 🗂️ DBT Project Structure

```text
models/
├── src/ # Source models from Snowflake
│ ├── src_hex_case_gsc_export.sql # Raw GSC export (JSON array)
│ └── src_hex_brand_cuisine_export.sql # Cuisine mappings (JSON array)
│
├── stg/ # Staging models
│ ├── stg_queries.sql # Flatten & extract query-level metrics
│ ├── stg_cuisines.sql # Flatten cuisines per restaurant
│ └── stg_metadata_cuisine_names.sql # Reference list of cuisine names
│
├── int/ # Intermediate logic
│ └── int_queries_classified.sql # Branded vs. unbranded classification
│
└── fct/ # Final aggregated outputs
├── fct_search_metrics_unified.sql # Aggregated metrics with grain dimension
├── fct_restaurant_query_metrics.sql # Restaurant-level metrics (sourced from unified)
├── fct_cuisine_query_metrics.sql # Cuisine-level metrics (sourced from unified)
```

---

## 🔍 Key Model: fct_search_metrics_unified

**Grain dimension:**
- **restaurant** → one row per `restaurant_id` + `search_type` (“Branded”, “Unbranded”)
- **cuisine** → one row per `cuisine` + `search_type`
- **global** → one row per `search_type` (computed to avoid double-counting multi-cuisine restaurants)

> Restaurants can have multiple cuisines; this model assigns **full attribution** to each cuisine a restaurant has.

**Metrics:**
- **search_query_count** – Distinct queries for the restaurant/cuisine and `search_type`
- **total_clicks** – Sum of clicks
- **total_impressions** – Sum of impressions
- **ctr** – Weighted CTR = `SUM(clicks) / SUM(impressions)`
- **avg_position** – Weighted average position = `SUM(position * impressions) / SUM(impressions)`
- **pct_unique_search_queries** – Share of unique queries per `search_type` within the restaurant/cuisine
- **pct_clicks** – Share of clicks per `search_type` within the restaurant/cuisine
- **pct_impressions** – Share of impressions per `search_type` within the restaurant/cuisine

**Metric rationale:**
- Summations preserve total volume for clicks and impressions
- Weighted averages for CTR and position prevent low-volume queries from distorting results
- Percent shares show each search type’s contribution within its parent group

---

## 🧠 Branded vs. Unbranded Classification

Implemented in **`int_queries_classified.sql`**.

A query is **Branded** if it meets **any** of these criteria:
- **String similarity**: Jaro-Winkler score between cleaned query and domain name ≥ 78.
- **Substring match**: Domain name appears in query (or vice versa).
- **Word overlap**: At least 2 of the first 3 words in the query appear in the domain name.
- **Manual overrides**: Specific regex or text matches for known brand variations and false negatives.

**False positive prevention:**
- Single-letter queries excluded.
- Queries matching single-word cuisine names (e.g., `thai`, `pizza`) excluded.
- Manually excluded patterns (e.g., `"delivery pizza%"` for certain domains).

**Example cleaning steps** (from `stg_queries.sql`):
```sql
-- Remove common extraneous terms
REGEXP_REPLACE(query, '\\b(near me|menu|buffet|open now|...)\\b', '')

-- Remove prices & symbols
REGEXP_REPLACE(..., '\\$[0-9]+|&', '')

-- Remove non-alphanumeric characters
REGEXP_REPLACE(..., '[^a-z0-9 ]', '')

-- Lowercase and remove trailing "s"
LOWER(...),
REGEXP_REPLACE(..., 's$', '')
```
---
## 🧾 Slowly Changing Dimensions (SCD): Cuisine Changes

In production, cuisine classification changes are tracked using **Type 2 SCD**:
- Insert a new row for each change with `effective_start_date` and `effective_end_date`.
- Join metrics to the correct cuisine record based on the query’s event date.

**Dataset constraint:** The provided queries lack timestamps, so Type 2 joining is not possible here.

**Practical alternative: Snapshot-based approach**
- Build a `dim_restaurant_cuisine_snapshot` on a regular cadence (e.g., daily or weekly).
- Join metrics to the snapshot in effect for the analysis period (controlled via dashboard filter).
- Attributes all queries to the cuisine active during the selected window.

**In dbt:**
- Create `dim_restaurant_cuisine_snapshot`.
- Use `is_incremental()` to track changes.
- Apply date filters in the reporting layer to select a snapshot.

While this method does not provide full record-level precision, it enables time-aware attribution of SEO performance in a way that is practical given the structure of the data. 
A true Type 2 SCD with timestamps at the query level would be my recomendation.

---

## ✅ Data Quality Checks

Currently implemented in **dbt tests**:

### Not null constraints
- `restaurant_id`, `domain`, `clicks`, `ctr`, `impressions`, `position`, and `query` in staging models.  
- `cuisine` in cuisine-level models.

### Accepted values enforcement
- `search_type` must be `"Branded"` or `"Unbranded"`.
- `grain` must be `"restaurant"`, `"cuisine"`, or `"global"` in `fct_search_metrics_unified`.

### Conditional null tests
- `restaurant_id` must be populated when `grain = 'restaurant'`.
- `cuisine` must be populated when `grain = 'cuisine'`.

### Uniqueness check
- `cuisine` values in `stg_metadata_cuisine_names` must be unique.
