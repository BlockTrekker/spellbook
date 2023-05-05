{{ config(alias='nft_traders_transactions_current') }}

WITH nft_trades AS (
    SELECT
        blockchain,
        tx_hash,
        buyer AS `address`
    FROM {{ ref('nft_trades') }}
    WHERE block_time > NOW() - INTERVAL '14' DAY

UNION ALL

SELECT
    blockchain,
    tx_hash,
    seller AS `address`
FROM {{ ref('nft_trades') }}
WHERE block_time > NOW() - interval '14' day
),

total AS (
    SELECT
        address,
        COUNT(tx_hash) AS `total_count`
    FROM nft_trades
    GROUP BY 1
)

SELECT * FROM (
    SELECT
        nft_trades.blockchain AS blockchain,
        nft_trades.address,
        CASE
            WHEN
                ((ROW_NUMBER() OVER (ORDER BY COUNT(tx_hash) DESC)) / total_count * 100) <= 10
                AND ((ROW_NUMBER() OVER (ORDER BY COUNT(tx_hash) DESC)) / total_count * 100) > 5
                THEN 'Current Top 10% NFT Trader (Transactions)'
            WHEN
                ((ROW_NUMBER() OVER (ORDER BY COUNT(tx_hash) DESC)) / total_count * 100) <= 5
                AND ((ROW_NUMBER() OVER (ORDER BY COUNT(tx_hash) DESC)) / total_count * 100) > 1
                THEN 'Current Top 5% NFT Trader (Transactions)'
            WHEN ((ROW_NUMBER() OVER (ORDER BY COUNT(tx_hash) DESC)) / total_count * 100) <= 1
                THEN 'Current Top 1% NFT Trader (Transactions)'
        END AS name,
        'nft' AS category,
        'hildobby' AS contributor,
        'query' AS source,
        timestamp('2022-08-24') AS created_at,
        now() AS updated_at,
        'nft_traders_transactions_current' AS model_name,
        'usage' AS `label_type`
    FROM nft_trades
    INNER JOIN total ON total.address = nft_trades.address
    WHERE nft_trades.address IS NOT NULL
    GROUP BY nft_trades.address, total_count, nft_trades.blockchain
)
WHERE name IS NOT NULL
