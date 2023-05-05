{{ config(
    alias='balancer_v2_pools_arbitrum',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['address'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                     "sector",
                                    "labels",
                                    \'["balancerlabs"]\') }}'
    )
}}

WITH pools AS (
    SELECT
        pool_id,
        zip.tokens AS token_address,
        zip.weights / pow(10, 18) AS normalized_weight,
        symbol,
        pool_type
    FROM (
        SELECT
            c.poolid AS pool_id,
            explode(arrays_zip(cc.tokens, cc.weights)) AS zip,
            cc.symbol,
            'WP' AS `pool_type`
        FROM {{ source('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }}
        INNER JOIN {{ source('balancer_v2_arbitrum', 'WeightedPoolFactory_call_create') }}
            ON
                c.evt_tx_hash = cc.call_tx_hash
                AND SUBSTRING(c.poolid, 0, 42) = cc.output_0
        {% if is_incremental() %}
            WHERE
                c.evt_block_time >= date_trunc('day', now() - interval '1 week')
                AND cc.call_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
    )

    UNION ALL

    SELECT
        pool_id,
        zip.tokens AS token_address,
        zip.normalizedweights / pow(10, 18) AS normalized_weight,
        symbol,
        pool_type
    FROM (
        SELECT
            c.poolid AS pool_id,
            explode(arrays_zip(cc.tokens, cc.normalizedweights)) AS zip,
            cc.symbol,
            'WP' AS `pool_type`
        FROM {{ source('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }}
        INNER JOIN {{ source('balancer_v2_arbitrum', 'WeightedPoolV2Factory_call_create') }}
            ON
                c.evt_tx_hash = cc.call_tx_hash
                AND SUBSTRING(c.poolid, 0, 42) = cc.output_0
        {% if is_incremental() %}
            WHERE
                c.evt_block_time >= date_trunc('day', now() - interval '1 week')
                AND cc.call_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
    )

    UNION ALL

    SELECT
        pool_id,
        zip.tokens AS token_address,
        zip.weights / pow(10, 18) AS normalized_weight,
        symbol,
        pool_type
    FROM (
        SELECT
            c.poolid AS pool_id,
            explode(arrays_zip(cc.tokens, cc.weights)) AS zip,
            cc.symbol,
            'IP' AS `pool_type`
        FROM {{ source('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }}
        INNER JOIN {{ source('balancer_v2_arbitrum', 'InvestmentPoolFactory_call_create') }}
            ON
                c.evt_tx_hash = cc.call_tx_hash
                AND SUBSTRING(c.poolid, 0, 42) = cc.output_0
        {% if is_incremental() %}
            WHERE
                c.evt_block_time >= date_trunc('day', now() - interval '1 week')
                AND cc.call_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
    )

    UNION ALL

    SELECT
        pool_id,
        zip.tokens AS token_address,
        zip.weights / pow(10, 18) AS normalized_weight,
        symbol,
        pool_type
    FROM (
        SELECT
            c.poolid AS pool_id,
            explode(arrays_zip(cc.tokens, cc.weights)) AS zip,
            cc.symbol,
            'WP2T' AS `pool_type`
        FROM {{ source('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }}
        INNER JOIN {{ source('balancer_v2_arbitrum', 'WeightedPool2TokensFactory_call_create') }}
            ON
                c.evt_tx_hash = cc.call_tx_hash
                AND SUBSTRING(c.poolid, 0, 42) = cc.output_0
        {% if is_incremental() %}
            WHERE
                c.evt_block_time >= date_trunc('day', now() - interval '1 week')
                AND cc.call_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
    )

    UNION ALL

    SELECT
        c.poolid AS pool_id,
        explode(cc.tokens) AS token_address,
        CAST(NULL AS double) AS normalized_weight,
        cc.symbol,
        'SP' AS `pool_type`
    FROM {{ source('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }}
    INNER JOIN {{ source('balancer_v2_arbitrum', 'StablePoolFactory_call_create') }}
        ON
            c.evt_tx_hash = cc.call_tx_hash
            AND SUBSTRING(c.poolid, 0, 42) = cc.output_0
    {% if is_incremental() %}
        WHERE
            c.evt_block_time >= date_trunc('day', now() - interval '1 week')
            AND cc.call_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT
        c.poolid AS pool_id,
        explode(cc.tokens) AS token_address,
        CAST(NULL AS double) AS normalized_weight,
        cc.symbol,
        'SP' AS `pool_type`
    FROM {{ source('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }}
    INNER JOIN {{ source('balancer_v2_arbitrum', 'MetaStablePoolFactory_call_create') }}
        ON
            c.evt_tx_hash = cc.call_tx_hash
            AND SUBSTRING(c.poolid, 0, 42) = cc.output_0
    {% if is_incremental() %}
        WHERE
            c.evt_block_time >= date_trunc('day', now() - interval '1 week')
            AND cc.call_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT
        c.poolid AS pool_id,
        explode(cc.tokens) AS token_address,
        0 AS normalized_weight,
        cc.symbol,
        'LBP' AS `pool_type`
    FROM {{ source('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }}
    INNER JOIN {{ source('balancer_v2_arbitrum', 'LiquidityBootstrappingPoolFactory_call_create') }}
        ON
            c.evt_tx_hash = cc.call_tx_hash
            AND SUBSTRING(c.poolid, 0, 42) = cc.output_0
    {% if is_incremental() %}
        WHERE
            c.evt_block_time >= date_trunc('day', now() - interval '1 week')
            AND cc.call_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT
        c.poolid AS pool_id,
        explode(cc.tokens) AS token_address,
        0 AS normalized_weight,
        cc.symbol,
        'LBP' AS `pool_type`
    FROM {{ source('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }}
    INNER JOIN {{ source('balancer_v2_arbitrum', 'NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create') }}
        ON
            c.evt_tx_hash = cc.call_tx_hash
            AND SUBSTRING(c.poolid, 0, 42) = cc.output_0
    {% if is_incremental() %}
        WHERE
            c.evt_block_time >= date_trunc('day', now() - interval '1 week')
            AND cc.call_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT
        c.poolid AS pool_id,
        explode(cc.tokens) AS token_address,
        CAST(NULL AS double) AS normalized_weight,
        cc.symbol,
        'SP' AS `pool_type`
    FROM {{ source('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }}
    INNER JOIN {{ source('balancer_v2_arbitrum', 'ComposableStablePoolFactory_call_create') }}
        ON
            c.evt_tx_hash = cc.call_tx_hash
            AND SUBSTRING(c.poolid, 0, 42) = cc.output_0
    {% if is_incremental() %}
        WHERE
            c.evt_block_time >= date_trunc('day', now() - interval '1 week')
            AND cc.call_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT
        c.poolid AS pool_id,
        explode(array(cc.mainToken, cc.wrappedToken)) AS zip,
        CAST(NULL AS double) AS normalized_weight,
        cc.symbol,
        'LP' AS `pool_type`
    FROM {{ source('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }}
    INNER JOIN {{ source('balancer_v2_arbitrum', 'AaveLinearPoolFactory_call_create') }}
        ON
            c.evt_tx_hash = cc.call_tx_hash
            AND SUBSTRING(c.poolid, 0, 42) = cc.output_0
    {% if is_incremental() %}
        WHERE
            c.evt_block_time >= date_trunc('day', now() - interval '1 week')
            AND cc.call_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT
        c.poolid AS pool_id,
        explode(array (cc.mainToken, cc.wrappedToken)) AS zip,
        CAST(NULL AS double) AS normalized_weight,
        cc.symbol,
        'LP' AS `pool_type`
    FROM {{ source('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }}
    INNER JOIN {{ source('balancer_v2_arbitrum', 'ERC4626LinearPoolFactory_call_create') }}
        ON
            c.evt_tx_hash = cc.call_tx_hash
            AND SUBSTRING(c.poolid, 0, 42) = cc.output_0
    {% if is_incremental() %}
        WHERE
            c.evt_block_time >= date_trunc('day', now() - interval '1 week')
            AND cc.call_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

settings AS (
    SELECT
        pool_id,
        coalesce(t.symbol, '?') AS token_symbol,
        normalized_weight,
        p.symbol AS pool_symbol,
        p.pool_type
    FROM pools
    LEFT JOIN tokens.erc20 ON p.token_address = t.contract_address
)

SELECT
    'arbitrum' AS blockchain,
    SUBSTRING(pool_id, 0, 42) AS address,
    CASE
        WHEN array_contains(array('SP', 'LP', 'LBP'), pool_type)
            THEN lower(pool_symbol)
        ELSE lower(concat(array_join(array_sort(collect_list(token_symbol)), '/'), ' ', array_join(collect_list(cast(norm_weight AS string)), '/')))
    END AS name,
    'balancer_v2_pool' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    timestamp('2022-12-23') AS created_at,
    now() AS updated_at,
    'balancer_v2_pools_arbitrum' AS model_name,
    'identifier' AS `label_type`
FROM (
    SELECT
        s1.pool_id,
        token_symbol,
        pool_symbol,
        cast(100 * normalized_weight AS integer) AS norm_weight,
        pool_type
    FROM settings
    ORDER BY pool_id ASC, pool_symbol DESC, token_symbol ASC
) AS `s`
GROUP BY pool_id, pool_symbol, pool_type
ORDER BY 1
