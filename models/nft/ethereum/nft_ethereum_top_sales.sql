{{ config(
     alias = 'top_sales',
     materialized='table',
     post_hook='{{ expose_spells(\'["ethereum"]\',
                                 "sector",
                                 "nft",
                                 \'["Henrystats"]\') }}'
     )
 }}

WITH src AS (
    SELECT
        nft_contract_address,
        token_id AS nft_token_id,
        seller,
        amount_original AS price,
        tx_hash,
        ROW_NUMBER() OVER (PARTITION BY nft_contract_address ORDER BY amount_original DESC) AS rn
    FROM
        {{ ref('nft_trades') }}
    WHERE
        blockchain = 'ethereum'
        AND currency_symbol IN ('ETH', 'WETH')
        AND amount_original IS NOT NULL
        AND number_of_items = 1
)

SELECT *
FROM
    src
WHERE
    rn <= 50
