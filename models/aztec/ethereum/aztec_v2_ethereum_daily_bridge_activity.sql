{{ config(
    schema = 'aztec_v2_ethereum',
    alias = 'daily_bridge_activity',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "aztec_v2",
                                \'["Henrystats"]\') }}'
    )
}}

{% set first_transfer_date = '2022-06-06' %} -- first tx date 

WITH

daily_transfers AS (
    SELECT
        date_trunc('day', evt_block_time) AS date,
        bridge_protocol,
        bridge_address,
        contract_address AS token_address,
        count(*) AS num_tfers, -- number of transfers
        count(DISTINCT evt_tx_hash) AS num_rollups, -- number of rollups
        sum(CASE WHEN spec_txn_type IN ('Bridge to Protocol', 'Protocol to Bridge') THEN value_norm ELSE 0 END) AS abs_value_norm,
        sum(CASE WHEN spec_txn_type = 'Bridge to Protocol' THEN value_norm ELSE 0 END) AS input_value_norm,
        sum(CASE WHEN spec_txn_type = 'Protocol to Bridge' THEN value_norm ELSE 0 END) AS output_value_norm
    FROM {{ ref('aztec_v2_ethereum_rollupbridge_transfers') }}
    WHERE bridge_protocol != '' -- exclude all txns that don't interact with the bridges
    GROUP BY 1, 2, 3, 4
),

token_addresses AS (
    SELECT DISTINCT token_address AS token_address
    FROM daily_transfers
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
        p.minute >= CAST('{{ first_transfer_date }}' AS TIMESTAMP)
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
        p.minute >= CAST('{{ first_transfer_date }}' AS TIMESTAMP)
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
        te.price AS eth_price -- to be used later 
    FROM
        token_prices_token AS tt
    INNER JOIN
        token_prices_eth AS te
        ON tt.day = te.day
)

SELECT
    dt.date,
    dt.bridge_protocol,
    dt.bridge_address,
    dt.token_address,
    er.symbol,
    dt.num_rollups,
    dt.num_tfers,
    dt.abs_value_norm,
    dt.abs_value_norm * COALESCE(p.price_usd, b.price) AS abs_volume_usd,
    dt.abs_value_norm * COALESCE(p.price_eth, b.price_eth) AS abs_volume_eth,
    dt.input_value_norm * COALESCE(p.price_usd, b.price) AS input_volume_usd,
    dt.input_value_norm * COALESCE(p.price_eth, b.price_eth) AS input_volume_eth,
    dt.output_value_norm * COALESCE(p.price_usd, b.price) AS output_volume_usd,
    dt.output_value_norm * COALESCE(p.price_eth, b.price_eth) AS output_volume_eth
FROM daily_transfers AS dt
LEFT JOIN {{ ref('tokens_erc20') }} AS er ON dt.token_address = er.contract_address AND er.blockchain = 'ethereum'
LEFT JOIN token_prices AS p ON dt.date = p.day AND dt.token_address = p.token_address AND dt.token_address != '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
LEFT JOIN token_prices_eth AS b ON dt.date = b.day AND dt.token_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'; -- using this to get price for missing ETH token 
