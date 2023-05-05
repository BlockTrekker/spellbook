{{ config(alias='nft_users_platforms') }}

WITH nft_trades AS (
    SELECT
        blockchain,
        project,
        buyer AS `address`
    FROM {{ ref('nft_trades') }}
    UNION ALL
SELECT
    blockchain,
    project,
    seller AS `address`
FROM {{ ref('nft_trades') }}
)

SELECT
    blockchain AS blockchain,
    address,
    array_join(collect_set(concat(upper(substring(project, 1, 1)), substring(project, 2))), ', ') || ' User' AS name,
    'nft' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    timestamp('2022-09-03') AS created_at,
    now() AS updated_at,
    'nft_users_platforms' AS model_name,
    'persona' AS `label_type`
FROM nft_trades
WHERE address IS NOT NULL
GROUP BY address, blockchain
