{{
    config(
        schema='balancer_v2_optimism',
        alias='liquidity',
        post_hook='{{ expose_spells_hide_trino(\'["optimism"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["stefenon"]\') }}'
    )
}}

WITH pool_labels AS (
    SELECT
        address AS pool_id,
        name AS pool_symbol
    FROM {{ ref('labels_balancer_v2_pools_optimism') }}
),

prices AS (
    SELECT
        date_trunc('day', MINUTE) AS day,
        contract_address AS token,
        AVG(price) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'optimism'
    GROUP BY 1, 2
),

dex_prices_1 AS (
    SELECT
        date_trunc('day', HOUR) AS day,
        contract_address AS token,
        percentile(median_price, 0.5) AS price,
        sum(sample_size) AS sample_size
    FROM {{ ref('dex_prices') }}
    GROUP BY 1, 2
    HAVING sum(sample_size) > 3
),

dex_prices AS (
    SELECT
        *,
        LEAD(DAY, 1, NOW()) OVER (
            PARTITION BY token
            ORDER BY
                day
        ) AS day_of_next_change
    FROM
        dex_prices_1
),

swaps_changes AS (
    SELECT
        day,
        pool_id,
        token,
        SUM(COALESCE(delta, 0)) AS delta
    FROM
        (
            SELECT
                date_trunc('day', evt_block_time) AS day,
                poolid AS pool_id,
                tokenin AS token,
                amountin AS delta
            FROM
                {{ source('balancer_v2_optimism', 'Vault_evt_Swap') }}
            UNION
            ALL
            SELECT
                date_trunc('day', evt_block_time) AS day,
                poolid AS pool_id,
                tokenout AS token,
                -amountout AS delta
            FROM
                {{ source('balancer_v2_optimism', 'Vault_evt_Swap') }}
        ) AS swaps
    GROUP BY 1, 2, 3
),

zipped_balance_changes AS (
    SELECT
        date_trunc('day', evt_block_time) AS day,
        poolid AS pool_id,
        explode(arrays_zip(tokens, deltas, protocolfeeamounts)) AS zipped
    FROM {{ source('balancer_v2_optimism', 'Vault_evt_PoolBalanceChanged') }}
),

balances_changes AS (
    SELECT
        day,
        pool_id,
        zipped.tokens AS token,
        zipped.deltas - zipped.protocolfeeamounts AS delta
    FROM zipped_balance_changes
    ORDER BY 1, 2, 3
),

managed_changes AS (
    SELECT
        date_trunc('day', evt_block_time) AS day,
        poolid AS pool_id,
        token,
        cashdelta + manageddelta AS delta
    FROM {{ source('balancer_v2_optimism', 'Vault_evt_PoolBalanceManaged') }}
),

daily_delta_balance AS (
    SELECT
        day,
        pool_id,
        token,
        SUM(COALESCE(amount, 0)) AS amount
    FROM
        (
            SELECT
                day,
                pool_id,
                token,
                SUM(COALESCE(delta, 0)) AS amount
            FROM
                balances_changes
            GROUP BY 1, 2, 3
            UNION ALL
            SELECT
                day,
                pool_id,
                token,
                delta AS amount
            FROM
                swaps_changes
            UNION ALL
            SELECT
                day,
                pool_id,
                token,
                delta AS amount
            FROM
                managed_changes
        ) AS balance
    GROUP BY 1, 2, 3
),

cumulative_balance AS (
    SELECT
        day,
        pool_id,
        token,
        LEAD(DAY, 1, NOW()) OVER (PARTITION BY token, pool_id ORDER BY day) AS day_of_next_change,
        SUM(amount) OVER (PARTITION BY pool_id, token ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM daily_delta_balance
),

calendar AS (
    SELECT explode(sequence(to_date('2021-04-21'), CURRENT_DATE, INTERVAL 1 DAY)) AS day
),

cumulative_usd_balance AS (
    SELECT
        c.day,
        b.pool_id,
        b.token,
        symbol AS token_symbol,
        cumulative_amount / POWER(10, t.decimals) * COALESCE(p1.price, p2.price, 0) AS amount_usd
    FROM calendar AS c
    LEFT JOIN cumulative_balance AS b
        ON
            b.day <= c.day
            AND c.day < b.day_of_next_change
    LEFT JOIN {{ ref('tokens_erc20') }} AS t
        ON
            t.contract_address = b.token
            AND blockchain = 'optimism'
    LEFT JOIN prices AS p1
        ON
            p1.day = b.day
            AND p1.token = b.token
    LEFT JOIN dex_prices AS p2
        ON
            p2.day <= c.day
            AND c.day < p2.day_of_next_change
            AND p2.token = b.token
    WHERE b.token != SUBSTRING(b.pool_id, 0, 42)
),

pool_liquidity_estimates AS (
    SELECT
        b.day,
        b.pool_id,
        SUM(b.amount_usd) / COALESCE(SUM(w.normalized_weight), 1) AS liquidity
    FROM cumulative_usd_balance AS b
    LEFT JOIN {{ ref('balancer_v2_optimism_pools_tokens_weights') }} AS w
        ON
            b.pool_id = w.pool_id
            AND b.token = w.token_address
    GROUP BY 1, 2
)

SELECT
    b.day,
    b.pool_id,
    p.pool_symbol,
    token AS token_address,
    token_symbol,
    coalesce(amount_usd, liquidity * normalized_weight) AS usd_amount
FROM pool_liquidity_estimates AS b
LEFT JOIN cumulative_usd_balance AS c
    ON
        c.day = b.day
        AND c.pool_id = b.pool_id
LEFT JOIN {{ ref('balancer_v2_optimism_pools_tokens_weights') }} AS w
    ON
        b.pool_id = w.pool_id
        AND w.token_address = c.token
LEFT JOIN pool_labels AS p ON p.pool_id = SUBSTRING(b.pool_id, 0, 42)
