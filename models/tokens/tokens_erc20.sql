{{ config( alias='erc20',
        tags=['static'],
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","optimism", "gnosis", "fantom"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xManny","hildobby","soispoke","dot2dotseurat","mtitus6"]\') }}') }}

SELECT
    'arbitrum' AS blockchain,
    *
FROM {{ ref('tokens_arbitrum_erc20') }}
UNION ALL
SELECT
    'avalanche_c' AS blockchain,
    *
FROM {{ ref('tokens_avalanche_c_erc20') }}
UNION ALL
SELECT
    'bnb' AS blockchain,
    *
FROM {{ ref('tokens_bnb_bep20') }}
UNION ALL
SELECT
    'ethereum' AS blockchain,
    *
FROM {{ ref('tokens_ethereum_erc20') }}
UNION ALL
SELECT
    'gnosis' AS blockchain,
    *
FROM {{ ref('tokens_gnosis_erc20') }}
UNION ALL
SELECT
    'optimism' AS blockchain,
    contract_address,
    symbol,
    decimals
FROM {{ ref('tokens_optimism_erc20') }}
UNION ALL
SELECT
    'polygon' AS blockchain,
    *
FROM {{ ref('tokens_polygon_erc20') }}
UNION ALL
SELECT
    'fantom' AS blockchain,
    *
FROM {{ ref('tokens_fantom_erc20') }}
