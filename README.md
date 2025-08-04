
# 📦 Project Overview

The purpose of this dbt project is to analyze Google Search Console (GSC) data across restaurants and cuisines. Specifically, the project tracks:

- Total impressions, clicks, CTR, and position
- Classification of queries as branded vs. unbranded
- Share of branded/unbranded searches at both the restaurant and cuisine level

---

## 🗂️ DBT Project Structure

```text
.
├── models/
│   ├── src/           # Raw source tables from Snowflake
│   ├── stg/           # Staging: flattened & cleaned inputs
│   ├── int/           # Intermediate logic: query classification
│   └── fct/           # Final models: aggregated performance
```

---

## 🔍 Key Models and Metrics

### 1. `fct_restaurant_query_metrics`
- **Grain**: `restaurant_id + search_type`
- Metrics:
  - Clicks, impressions, CTR
  - Weighted average position
  - Share of branded vs. unbranded queries, clicks, impressions

### 2. `fct_cuisine_query_metrics`
- **Grain**: `cuisine + search_type`
- Same metrics as above, rolled up by cuisine

---

## 🧠 Branded vs. Unbranded Classification

Queries are classified as branded if they show high lexical or semantic similarity to the restaurant's domain name. 

This includes:

- Jaro-Winkler similarity score of cleaned query and domain name above 80
- Substring matches between the cleaned query and domain name
- Shared words (e.g., at least 2 of the first 3 words in the query appear in the domain)

To reduce false positives:
- All single-letter queries excluded
- Single-word cuisine terms like "thai" or "pizza" are excluded
- Manual exceptions for known edge cases, which can be identified by reviewing of 'Branded' queries with low similarity score

All remaining queries are classified as unbranded.

Logic lives in: `int_queries_classified.sql`

---

## 🧾 Slowly Changing Dimensions (SCD) Approach

In a production setting, the preferred strategy for handling changes to a restaurant’s cuisine classification would be a Type 2 Slowly Changing Dimension (SCD). This approach preserves historical accuracy by storing a new row each time a restaurant’s cuisine changes, along with `effective_start_date` and `effective_end_date`. This allows query-level facts to be joined to the appropriate version of the dimension based on when the event occurred.

However, the provided dataset does not include timestamps on individual queries, so it is not possible to determine when each event took place. Without that temporal context, a true Type 2 SCD implementation at the query level is not feasible.

As a practical alternative, I would implement a snapshot-based approach that captures the state of each restaurant’s cuisine on a regular cadence, such as daily or weekly. This snapshot can be joined to query metrics at the `restaurant_id` level, with the analysis period controlled through a dashboard filter. All queries would then be attributed to the cuisine active during the selected time window.

In dbt, this could be implemented by:
- Creating a `dim_restaurant_cuisine_snapshot` model
- Using `is_incremental()` logic to track changes
- Applying date filters at the reporting layer to scope analysis to a given snapshot

While this method does not provide full record-level precision, it enables time-aware attribution of SEO performance in a way that is practical given the structure of the data.
