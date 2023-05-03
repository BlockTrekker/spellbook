{{
    config(
        schema='balancer_v2_ethereum',
        alias='pools_tokens_weights',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['pool_id', 'token_address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["metacrypto", "jacektrocinski"]\') }}'
    )
}}

--
-- Balancer v2 Pools Tokens Weights
--
WITH registered AS (
    SELECT poolId, evt_block_time
    FROM {{ source('balancer_v2_ethereum', 'Vault_evt_PoolRegistered') }}
),

call_create_1 AS (
    SELECT output_0, tokens, weights
    FROM {{ source('balancer_v2_ethereum', 'WeightedPoolFactory_call_create') }}
),

call_create_2 AS (
    SELECT output_0, tokens, weights
    FROM {{ source('balancer_v2_ethereum', 'WeightedPool2TokensFactory_call_create') }}
),

call_create_3 AS (
    SELECT output_0, tokens, normalizedWeights AS weights
    FROM {{ source('balancer_v2_ethereum', 'WeightedPoolV2Factory_call_create') }}
)

SELECT
    registered.poolId AS pool_id,
    tokens.token_address,
    weights.normalized_weight / POW(10, 18) AS normalized_weight
FROM registered
LEFT JOIN call_create_1
    ON CAST(call_create_1.output_0 AS STRING) = CAST(SUBSTR(registered.poolId, 0, 43) AS STRING)
LEFT JOIN UNNEST(call_create_1.tokens) AS tokens WITH OFFSET AS pos
LEFT JOIN UNNEST(call_create_1.weights) AS weights WITH OFFSET AS pos
    ON tokens.pos = weights.pos
WHERE registered.evt_block_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK)
UNION ALL
SELECT
    registered.poolId AS pool_id,
    tokens.token_address,
    weights.normalized_weight / POW(10, 18) AS normalized_weight
FROM registered
LEFT JOIN call_create_2
    ON call_create_2.output_0 = SUBSTR(registered.poolId, 0, 43)
LEFT JOIN UNNEST(call_create_2.tokens) AS tokens WITH OFFSET AS pos
LEFT JOIN UNNEST(call_create_2.weights) AS weights WITH OFFSET AS pos
    ON tokens.pos = weights.pos
WHERE registered.evt_block_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK)
UNION ALL
SELECT
    registered.poolId AS pool_id,
    tokens.token_address,
    weights.normalized_weight / POW(10, 18) AS normalized_weight
FROM registered
LEFT JOIN call_create_3
    ON call_create_3.output_0 = SUBSTR(registered.poolId, 0, 43)
LEFT JOIN UNNEST(call_create_3.tokens) AS tokens WITH OFFSET AS pos
LEFT JOIN UNNEST(call_create_3.weights) AS weights WITH OFFSET AS pos
    ON tokens.pos = weights.pos
WHERE registered.evt_block_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK)

