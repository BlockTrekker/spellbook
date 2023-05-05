{{ config(alias='lens_poster_frequencies'

    ,post_hook='{{ expose_spells(\'["polygon"]\',
                                    "sector",
                                    "labels",
                                    \'["scoffie"]\') }}')
}}

WITH lens_addresses AS (SELECT to AS `address`
       ,handle AS `name`
       ,CAST (profileId AS string) AS `profileId`
FROM {{ source('lens_polygon','LensHub_evt_ProfileCreated') }}



),

post_data AS (
SELECT
    output_0 AS post_id,
    CAST(get_json_object(vars, '$.profileId') AS string) AS `profileid`

FROM {{ source('lens_polygon','LensHub_call_post') }}


WHERE
    1 = 1
    AND call_success = true


UNION ALL


SELECT
    output_0 AS post_id,
    CAST(get_json_object(vars, '$.profileId') AS string) AS `profileid`
FROM {{ source('lens_polygon','LensHub_call_postWithSig') }}


WHERE
    1 = 1
    AND call_success = true
),

post_count AS (
SELECT DISTINCT
    profileid,
    COUNT(post_id) AS `posts_count`
FROM post_data
GROUP BY 1
),

percentile AS (
SELECT
    approx_percentile(posts_count, 0.99) AS p99,
    approx_percentile(posts_count, 0.95) AS p95,
    approx_percentile(posts_count, 0.90) AS p90,
    approx_percentile(posts_count, 0.80) AS p80,
    approx_percentile(posts_count, 0.5) AS `p50`
FROM post_count
)

SELECT
'polygon' AS blockchain,
address,
CASE
    WHEN posts_count >= (SELECT p99 FROM percentile) THEN 'top 1% lens poster'
    WHEN posts_count >= (SELECT p95 FROM percentile) THEN 'top 5% lens poster'
    WHEN posts_count >= (SELECT p90 FROM percentile) THEN 'top 10% lens poster'
    WHEN posts_count >= (SELECT p80 FROM percentile) THEN 'top 20% lens poster'
    WHEN posts_count <= (SELECT p50 FROM percentile) THEN 'bottom 50% lens poster'
    ELSE 'between 50% to 80% lens posters'
END AS name,
'social' AS category,
'scoffie' AS contributor,
'query' AS source,
timestamp('2023-03-17') AS created_at,
now() AS updated_at,
'lens_poster_frequencies' AS model_name,
'usage' AS `label_type`
FROM (
SELECT
    address, --make distinct
    sum(posts_count) AS `posts_count`
FROM lens_addresses
INNER JOIN post_count
    ON la.profileid = pc.profileid
GROUP BY 1
)
