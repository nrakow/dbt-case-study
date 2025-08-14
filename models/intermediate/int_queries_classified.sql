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
        TRIM(
          REGEXP_REPLACE(  -- Step 5: remove trailing "s" if present
            LOWER(         -- Step 4: convert to lowercase
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      query,
                      -- Step 1: remove common extraneous terms for classification
                      '\\b(near me| a | around me|menu|''|buffet|open now|food|deliver(y|s)?|gift cards?( (online|balance))?)\\b',
                      ''
                    ),
                    -- Step 2: remove price amounts (e.g. "$20") and "&"
                    '\\$[0-9]+|&',
                    ''
                  ),
                  -- Step 3: remove any remaining non-alphanumeric characters except spaces
                  '[^a-z0-9 ]',
                  ''
                ),
                -- Step 4a: collapse multiple spaces into a single space
                '\\s+',
                ' '
              )
            ),
            's$',           -- matches lowercase "s" at end of string
            ''
          )
        ) AS query_cleaned,
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
            AND (JAROWINKLER_SIMILARITY(query_cleaned, domain_name_cleaned) >= 78
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
            OR domain_name_cleaned ILIKE SPLIT(query_cleaned, ' ')[0] || '%' -- first word in the query is the start of the domain Ex. ovn bellingham -> ovnwoodfiredpizza
            )
            --Excluding common causes of false positives
            AND (c.cuisine IS NULL -- eliminates many false positives for queries that are cuisine names ex. 'thai', 'indian' etc. which are common in restaurant names
                AND query_cleaned NOT IN ('tandor','viet','pasta','latino','egg','india','bahn','cuisine','waffles','pizza','restaurant','express','bar','grill','bakery','spice','best delivery pizza chicago','pastery','fries','salad','bagel', 'bistro','restaurant','burger', 'shawarma', 'taqueria', 'hibachi', 'chicken','taco', 'teriyaki', 'noodles', 'burrito','vietnam','pastry')
                AND NOT (domain_name_cleaned = 'flavorsofindia' and query_raw like 'flavorful%')
                AND query_cleaned not like 'delivery pizza%'
                AND NOT (domain_name_cleaned = 'smashsd' and query_cleaned like 'smash%burger%')
                )
                OR (
                    -- Setting particular queries to 'Branded'
                    -- These can be identified by looking at high click-volume queries classified as 'Unbranded'
                    (domain_name_cleaned like 'mediterranean%' and query_cleaned = 'mediterranean')
                    OR (domain_name_cleaned like '%aandr%' and REGEXP_LIKE(query_raw, 'a\s*&\s*r')) -- special char stripped in cleaning caused misclassification due to 'and' in domain
                    OR (query_cleaned like '%my pie%' and domain_name_cleaned = 'pizzayourway') --restaurant name not in domain
                    OR (query_cleaned like '%tacos and beer%' and domain_name_cleaned = 'tbmx1')
                    OR (query_cleaned like '%henry higgins%' and domain_name_cleaned = 'hhboiledbagels')
                    OR (query_cleaned like '%goodfellas%' and domain_name_cleaned = 'tharealgoodfellas')
                    OR (query_cleaned like '%saigon 8%' and domain_name_cleaned = 'saigon8vegas')
                    OR (query_cleaned like '%the modern vegan%' and domain_name_cleaned = 'tmvrestaurants')
                    OR (query_cleaned like 'the delaware%' and domain_name_cleaned = 'delawarepubandgrill')
                    OR (query_cleaned like '%sombrero%' and domain_name_cleaned = 'thesombrero')
                    OR (query_cleaned like '%chubbfather%' and domain_name_cleaned = 'thechubbfather')
                    OR (query_cleaned like '%aarus%' and domain_name_cleaned = 'aaruz')
                    OR (REGEXP_LIKE(query_cleaned, '.*pep[sz].*', 'i') AND domain_name_cleaned = 'stadiumpepz')
                    OR (REGEXP_LIKE(query_cleaned, '.*threes?.*|.*three\\s+happiness.*', 'i') AND domain_name_cleaned = '3happinessomaha')
                    OR (REGEXP_LIKE(query_cleaned, '.*dialog.*|.*dialogue.*', 'i') AND domain_name_cleaned = 'dialogcafe')
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
