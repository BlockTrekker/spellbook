{{ config(
    schema = 'aztec_v2_ethereum',
    alias = 'daily_estimated_rollup_tvl',
    post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                "project",
                                "aztec_v2",
                                \'["Henrystats"]\') }}'
    )
}}

{% set first_transfer_date = '2022-06-06' %}

WITH

rollup_balance_changes AS (
    SELECT
        date_trunc('day', t.evt_block_time) AS date,
        t.symbol,
        t.contract_address AS token_address,
        sum(CASE WHEN t.from_type = 'Rollup' THEN -1 * value_norm WHEN t.to_type = 'Rollup' THEN value_norm ELSE 0 END) AS net_value_norm
    FROM {{ ref('aztec_v2_ethereum_rollupbridge_transfers') }} AS t
    WHERE t.from_type = 'Rollup' OR t.to_type = 'Rollup'
    GROUP BY 1, 2, 3
),

token_balances AS (
    SELECT
        date,
        symbol,
        token_address,
        sum(net_value_norm) OVER (PARTITION BY symbol, token_address ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS balance,
        lead(date, 1) OVER (PARTITION BY token_address ORDER BY date) AS next_date
    FROM rollup_balance_changes
),

day_series AS (
    SELECT explode(sequence(CAST('2022-06-06' AS date), CAST(NOW() AS date), interval '1 Day')) AS date
),

token_balances_filled AS (
    SELECT
        d.date,
        b.symbol,
        b.token_address,
        b.balance
    FROM day_series AS d
    INNER JOIN token_balances AS b
        ON
            d.date >= b.date
            AND d.date < coalesce(b.next_date, CAST(NOW() AS date) + 1) -- if it's missing that means it's the last entry in the series
),

token_addresses AS (
    SELECT DISTINCT token_address AS token_address
    FROM rollup_balance_changes
),

token_prices_token AS (
    SELECT
        date_trunc('day', p.minute) AS day,
        p.contract_address AS token_address,
        p.symbol,
        AVG(p.price) AS price
    FROM
        {{ source('prices', 'usd') }} AS p
    WHERE
        p.minute >= '{{ first_transfer_date }}'
        AND p.contract_address IN (SELECT token_address FROM token_addresses)
        AND p.blockchain = 'ethereum'
    GROUP BY 1, 2, 3
),

token_prices_eth AS (
    SELECT
        date_trunc('day', p.minute) AS day,
        AVG(p.price) AS price,
        1 AS price_eth
    FROM
        {{ source('prices', 'usd') }} AS p
    WHERE
        p.minute >= '{{ first_transfer_date }}'
        AND p.blockchain = 'ethereum'
        AND p.symbol = 'WETH'
    GROUP BY 1, 3
),

token_prices AS (
    SELECT
        tt.day,
        tt.token_address,
        tt.symbol,
        tt.price AS price_usd,
        tt.price / te.price AS price_eth,
        te.price AS eth_price
    FROM
        token_prices_token AS tt
    INNER JOIN
        token_prices_eth AS te
        ON tt.day = te.day
),

token_tvls AS (
    SELECT
        b.date,
        b.symbol,
        b.token_address,
        b.balance,
        b.balance * COALESCE(p.price_usd, bb.price) AS tvl_usd,
        b.balance * COALESCE(p.price_eth, bb.price_eth) AS tvl_eth
    FROM token_balances_filled AS b
    LEFT JOIN token_prices AS p ON b.date = p.day AND b.token_address = p.token_address AND b.token_address != '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
    LEFT JOIN token_prices_eth AS bb ON b.date = bb.day AND b.token_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' -- using this to get price for missing ETH token 

)

SELECT * FROM token_tvls;
