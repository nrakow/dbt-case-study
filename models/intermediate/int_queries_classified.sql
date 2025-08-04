{{ config(materialized = 'table') }}

with queries_cleaned as (
     SELECT
        RESTAURANT_ID,
        DOMAIN,
        clicks,
        ctr,
        impressions,
        position,
        query as query_raw,
        TRIM(LOWER(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  query,
                  '\\b(near me| a | around me|menu|''|buffet|open now|food|deliver(y|s)?|gift cards?( (online|balance))?)\\b',
                  ''
                ),
                '\\$[0-9]+|&', ''
              ),
              '[^a-z0-9 ]', ''
            ),
            '\\s+', ' '
          )
        )) AS query_cleaned, -- removing extraneous words for later classification
        -- Simplified brand_name derivation: remove domain extension and replace hyphens/underscores with spaces
        REGEXP_REPLACE(
            REGEXP_REPLACE(DOMAIN, '\.[a-z]+$', ''),  -- Remove .com/.net etc.
            '[-_]', ' '  -- Replace hyphens/underscores with spaces
        ) AS domain_name_cleaned
     FROM {{ ref('stg_queries') }}
),

classified_queries AS (
SELECT
    RESTAURANT_ID,
    DOMAIN,
    clicks,
    ctr,
    impressions,
    position,
    query_raw,
    query_cleaned,
    domain_name_cleaned,
    JAROWINKLER_SIMILARITY(query_cleaned, domain_name_cleaned) as jarrow_similarity,
    -- Classification using JAROWINKLER + manual heuristics
    -- The code is ugly but it works fairly well in most cases
    CASE
        WHEN
            LENGTH(query_cleaned) > 1 -- considering all single letters unbranded
            AND (JAROWINKLER_SIMILARITY(query_cleaned, domain_name_cleaned) > 80
            OR ( -- Fallback for partial/substring matches.
                REPLACE(query_cleaned,' ','') LIKE '%' || domain_name_cleaned || '%'
                OR domain_name_cleaned LIKE '%' || REPLACE(query_cleaned,' ','') || '%')
            OR (  -- Match if at least 2 of the first 3 words in the query appear in the domain name.
                  -- Example: 'pizza dados' matches 'dadospizzaok'
                  (
                      CASE WHEN ARRAY_SIZE(SPLIT(query_cleaned, ' ')) > 0 -- non null
                                AND LENGTH(SPLIT(query_cleaned, ' ')[0]) > 1 -- greater than 1 character
                                AND POSITION(SPLIT(query_cleaned, ' ')[0] IN domain_name_cleaned) > 0 THEN 1 ELSE 0 END
                      +
                      CASE WHEN ARRAY_SIZE(SPLIT(query_cleaned, ' ')) > 1
                                AND LENGTH(SPLIT(query_cleaned, ' ')[1]) > 1
                                AND POSITION(SPLIT(query_cleaned, ' ')[1] IN domain_name_cleaned) > 0 THEN 1 ELSE 0 END
                      +
                      CASE WHEN ARRAY_SIZE(SPLIT(query_cleaned, ' ')) > 2
                                AND LENGTH(SPLIT(query_cleaned, ' ')[2]) > 1
                                AND POSITION(SPLIT(query_cleaned, ' ')[2] IN domain_name_cleaned) > 0 THEN 1 ELSE 0 END
                    ) >= 2
                  AND query_raw NOT LIKE '%near me%' -- avoids many false positives
                  AND (query_raw NOT LIKE '%india% in%' OR query_raw NOT LIKE '%in% india%') -- any query with 'indian in' would match any name with 'india'
                )
            )
            --Excluding common causes of false positives
            AND (c.cuisine IS NULL -- eliminates many false positives for queries that are cuisine names ex. 'thai', 'indian' etc. which are common in restaurant names
            AND query_cleaned NOT IN ('tandor','viet','pasta','latino','egg','india','bahn','cuisine','waffles','pizza','restaurant','express','bar','grill','bakery','spice','best delivery pizza chicago','pastery','fries','salad','bagel', 'bistro','restaurant','burger', 'shawarma', 'taqueria', 'hibachi', 'chicken','taco', 'teriyaki', 'noodles', 'burrito','vietnam')
            AND NOT (domain_name_cleaned = 'flavorsofindia' and query_raw like 'flavorful%')
            AND query_cleaned not like 'delivery pizza%'
            AND NOT (domain_name_cleaned = 'smashsd' and query_cleaned like 'smash%burger%')
            AND NOT (domain_name_cleaned = 'andalelatinogrill' and query_cleaned = 'latin american grill')
                )
                OR (
                    (domain_name_cleaned like 'mediterranean%' and query_cleaned = 'mediterranean')
                    OR (domain_name_cleaned like '%aandr%' and query_cleaned like 'a&r') -- special char stripped in cleaning caused misclassification due to 'and' in domain
                    )
        THEN 'Branded'
        ELSE 'Unbranded'
END AS search_type,
FROM queries_cleaned q
LEFT JOIN {{ ref('stg_metadata_cuisine_names') }} c on c.cuisine = q.query_cleaned
)

-- Ideally, we also have a reference table with more granular brand terms, ex.
-- brand_terms AS (
--   SELECT RESTAURANT_ID, 'mcdonalds' AS brand_term
--   UNION ALL
--   SELECT RESTAURANT_ID, 'mc donalds'
--   UNION ALL
--   SELECT RESTAURANT_ID, 'mickey d'
-- ),

select * from classified_queries
