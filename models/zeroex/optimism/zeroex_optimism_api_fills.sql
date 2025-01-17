{{ config(
        alias='api_fills',
        materialized='incremental',                                      
        partition_by = ['block_date'],
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                "project", 
                                "zeroex",
                                \'["rantum", "danning.sui", "bakabhai993"]\') }}'
    )
}} 
{% set zeroex_v3_start_date = '2019-12-01' %}
{% set zeroex_v4_start_date = '2021-01-06' %}

-- Test Query here: https://dune.com/queries/1685501

WITH zeroex_tx AS (
    SELECT
        tx_hash,
        max(affiliate_address) AS affiliate_address
    FROM (
        SELECT
            tr.tx_hash,
            '0x' || CASE
                WHEN POSITION('869584cd' IN input) != 0
                    THEN SUBSTRING(
                        input
                        FROM (position('869584cd' IN input) + 32)
                        FOR 40
                    )
                WHEN POSITION('fbc019a7' IN input) != 0
                    THEN SUBSTRING(
                        input
                        FROM (position('fbc019a7' IN input) + 32)
                        FOR 40
                    )
            END AS affiliate_address
        FROM {{ source('optimism', 'traces') }} AS tr
        WHERE
            tr.to IN (
                -- exchange contract
                '0x61935cbdd02287b511119ddb11aeb42f1593b7ef',
                -- forwarder addresses
                '0x6958f5e95332d93d21af0d7b9ca85b8212fee0a5',
                '0x4aa817c6f383c8e8ae77301d18ce48efb16fd2be',
                '0x4ef40d1bf0983899892946830abf99eca2dbc5ce',
                -- exchange proxy
                '0xdef1abe32c034e558cdd535791643c58a13acc10'
            )
            AND (
                POSITION('869584cd' IN input) != 0
                OR POSITION('fbc019a7' IN input) != 0
            )

            {% if is_incremental() %}
                AND block_time >= date_trunc('day', now() - INTERVAL '1 week')
            {% endif %}
        {% if not is_incremental() %}
                AND block_time >= '{{ zeroex_v3_start_date }}'
                {% endif %}
    ) AS temp
    GROUP BY tx_hash

),

v4_rfq_fills_no_bridge AS (
    SELECT
        fills.evt_tx_hash AS tx_hash,
        fills.evt_index,
        fills.contract_address,
        fills.evt_block_time AS block_time,
        fills.maker AS maker,
        fills.taker AS taker,
        fills.takertoken AS taker_token,
        fills.makertoken AS maker_token,
        fills.takertokenfilledamount AS taker_token_amount_raw,
        fills.makertokenfilledamount AS maker_token_amount_raw,
        'RfqOrderFilled' AS type,
        zeroex_tx.affiliate_address AS affiliate_address,
        (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag,
        FALSE AS matcha_limit_order_flag
    FROM {{ source('zeroex_optimism', 'ExchangeProxy_evt_RfqOrderFilled') }} AS fills
    INNER JOIN zeroex_tx ON zeroex_tx.tx_hash = fills.evt_tx_hash
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
    {% if not is_incremental() %}
    WHERE evt_block_time >= '{{ zeroex_v4_start_date }}'
    {% endif %}
),

v4_limit_fills_no_bridge AS (
    SELECT
        fills.evt_tx_hash AS tx_hash,
        fills.evt_index,
        fills.contract_address,
        fills.evt_block_time AS block_time,
        fills.maker AS maker,
        fills.taker AS taker,
        fills.takertoken AS taker_token,
        fills.makertoken AS maker_token,
        fills.takertokenfilledamount AS taker_token_amount_raw,
        fills.makertokenfilledamount AS maker_token_amount_raw,
        'LimitOrderFilled' AS type,
        COALESCE(zeroex_tx.affiliate_address, fills.feerecipient) AS affiliate_address,
        (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag,
        (
            fills.feerecipient IN
            ('0x9b858be6e3047d88820f439b240deac2418a2551', '0x86003b044f70dac0abc80ac8957305b6370893ed', '0x5bc2419a087666148bfbe1361ae6c06d240c6131'))
            AS matcha_limit_order_flag
    FROM {{ source('zeroex_optimism', 'ExchangeProxy_evt_LimitOrderFilled') }} AS fills

    INNER JOIN zeroex_tx ON zeroex_tx.tx_hash = fills.evt_tx_hash


    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
    {% if not is_incremental() %}
    WHERE evt_block_time >= '{{ zeroex_v4_start_date }}'
    {% endif %}
),



otc_fills AS (
    SELECT
        fills.evt_tx_hash AS tx_hash,
        fills.evt_index,
        fills.contract_address,
        fills.evt_block_time AS block_time,
        fills.maker AS maker,
        fills.taker AS taker,
        fills.takertoken AS taker_token,
        fills.makertoken AS maker_token,
        fills.takertokenfilledamount AS taker_token_amount_raw,
        fills.makertokenfilledamount AS maker_token_amount_raw,
        'OtcOrderFilled' AS type,
        zeroex_tx.affiliate_address AS affiliate_address,
        (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag,
        FALSE AS matcha_limit_order_flag
    FROM {{ source('zeroex_optimism', 'ExchangeProxy_evt_OtcOrderFilled') }} AS fills

    INNER JOIN zeroex_tx ON zeroex_tx.tx_hash = fills.evt_tx_hash


    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
    {% if not is_incremental() %}
    WHERE evt_block_time >= '{{ zeroex_v4_start_date }}'
    {% endif %}

),


erc20bridgetransfer AS (
    SELECT
        logs.tx_hash,
        index AS evt_index,
        logs.contract_address,
        block_time AS block_time,
        '0x' || substring(data, 283, 40) AS maker,
        '0x' || substring(data, 347, 40) AS taker,
        '0x' || substring(data, 27, 40) AS taker_token,
        '0x' || substring(data, 91, 40) AS maker_token,
        bytea2numeric(substring(data, 155, 40)) AS taker_token_amount_raw,
        bytea2numeric(substring(data, 219, 40)) AS maker_token_amount_raw,
        'ERC20BridgeTransfer' AS type,
        zeroex_tx.affiliate_address AS affiliate_address,
        TRUE AS swap_flag,
        FALSE AS matcha_limit_order_flag
    FROM {{ source('optimism', 'logs') }} AS logs
    INNER JOIN zeroex_tx ON zeroex_tx.tx_hash = logs.tx_hash
    WHERE
        topic1 = '0x349fc08071558d8e3aa92dec9396e4e9f2dfecd6bb9065759d1932e7da43b8a9'

        {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - INTERVAL '1 week')
        {% endif %}
    {% if not is_incremental() %}
  --  AND block_time >= '{{ zeroex_v3_start_date }}'
    {% endif %}


),
/*
BridgeFill AS (
    SELECT

            logs.tx_hash,
            INDEX                                           AS evt_index,
            logs.contract_address,
            block_time                                      AS block_time,
            '0x' || substring(DATA, 27, 40)                 AS maker,
            '0xdef1abe32c034e558cdd535791643c58a13acc10'    AS taker,
            '0x' || substring(DATA, 91, 40)                 AS taker_token,
            '0x' || substring(DATA, 155, 40)                AS maker_token,
            bytea2numeric('0x' || substring(DATA, 219, 40)) AS taker_token_amount_raw,
            bytea2numeric('0x' || substring(DATA, 283, 40)) AS maker_token_amount_raw,
            'BridgeFill'                                    AS type,
            zeroex_tx.affiliate_address                     AS affiliate_address,
            TRUE                                            AS swap_flag,
            FALSE                                           AS matcha_limit_order_flag
    FROM {{ source('optimism', 'logs') }} logs
    INNER JOIN zeroex_tx ON zeroex_tx.tx_hash = logs.tx_hash
    WHERE topic1 = '0xff3bc5e46464411f331d1b093e1587d2d1aa667f5618f98a95afc4132709d3a9'
        AND contract_address = '0xa3128d9b7cca7d5af29780a56abeec12b05a6740'

{% if is_incremental() %}
AND block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}
{% if not is_incremental() %}
  --      AND block_time >= '{{ zeroex_v4_start_date }}'
        {% endif %}

),
*/
newbridgefill AS (
    SELECT

        logs.tx_hash AS tx_hash,
        index AS evt_index,
        logs.contract_address,
        block_time AS block_time,
        '0x' || substring(data, 27, 40) AS maker,
        '0xdef1abe32c034e558cdd535791643c58a13acc10' AS taker,
        '0x' || substring(data, 91, 40) AS taker_token,
        '0x' || substring(data, 155, 40) AS maker_token,
        bytea2numeric('0x' || substring(data, 219, 40)) AS taker_token_amount_raw,
        bytea2numeric('0x' || substring(data, 283, 40)) AS maker_token_amount_raw,
        'BridgeFill' AS type,
        zeroex_tx.affiliate_address AS affiliate_address,
        TRUE AS swap_flag,
        FALSE AS matcha_limit_order_flag
    FROM {{ source('optimism' ,'logs') }} AS logs
    INNER JOIN zeroex_tx ON zeroex_tx.tx_hash = logs.tx_hash
    WHERE
        topic1 = '0xe59e71a14fe90157eedc866c4f8c767d3943d6b6b2e8cd64dddcc92ab4c55af8'
        AND contract_address = '0xa3128d9b7cca7d5af29780a56abeec12b05a6740'

        {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - INTERVAL '1 week')
        {% endif %}
    {% if not is_incremental() %}
        AND block_time >= '{{ zeroex_v4_start_date }}'
        {% endif %}
),
/*
direct_PLP AS (

    SELECT

            plp.evt_tx_hash,
            plp.evt_index               AS evt_index,
            plp.contract_address,
            plp.evt_block_time          AS block_time,f
            provider                    AS maker,
            recipient                   AS taker,
            inputToken                  AS taker_token,
            outputToken                 AS maker_token,
            inputTokenAmount            AS taker_token_amount_raw,
            outputTokenAmount           AS maker_token_amount_raw,
            'LiquidityProviderSwap'     AS type,
            zeroex_tx.affiliate_address AS affiliate_address,
            TRUE                        AS swap_flag,
            FALSE                       AS matcha_limit_order_flag
    FROM {{ source('zeroex_optimism', 'ExchangeProxy_evt_LiquidityProviderSwap') }} plp
    INNER JOIN zeroex_tx ON zeroex_tx.tx_hash = plp.evt_tx_hash

{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}
{% if not is_incremental() %}
    WHERE evt_block_time >= '{{ zeroex_v3_start_date }}'
    {% endif %}

),
direct_uniswapv3 AS (
    SELECT

            swap.evt_tx_hash                                                                        AS tx_hash,
            swap.evt_index,
            swap.contract_address,
            swap.evt_block_time                                                                     AS block_time,
            swap.contract_address                                                                   AS maker,
            LAST_VALUE(swap.recipient) OVER (PARTITION BY swap.evt_tx_hash ORDER BY swap.evt_index) AS taker,
            CASE WHEN amount0 < '0' THEN pair.token1 ELSE pair.token0 END                           AS taker_token,
            CASE WHEN amount0 < '0' THEN pair.token0 ELSE pair.token1 END                           AS maker_token,
            CASE
                WHEN amount0 < '0' THEN abs(swap.amount1)
                ELSE abs(swap.amount0) END                                                          AS taker_token_amount_raw,
            CASE
                WHEN amount0 < '0' THEN abs(swap.amount0)
                ELSE abs(swap.amount1) END                                                          AS maker_token_amount_raw,
            'Uniswap V3 Direct'                                                                     AS type,
            zeroex_tx.affiliate_address                                                             AS affiliate_address,
            TRUE                                                                                    AS swap_flag,
            FALSE                                                                                   AS matcha_limit_order_flag
    FROM {{ source('uniswap_v3_optimism', 'Pair_evt_Swap') }} swap
   LEFT JOIN {{ source('uniswap_v3_optimism', 'factory_evt_poolcreated') }} pair ON pair.pool = swap.contract_address
   INNER JOIN zeroex_tx ON zeroex_tx.tx_hash = swap.evt_tx_hash
   WHERE sender = '0xdef1abe32c034e558cdd535791643c58a13acc10'

{% if is_incremental() %}
AND swap.evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}
{% if not is_incremental() %}
      --  AND swap.evt_block_time >= '{{ zeroex_v4_start_date }}'
        {% endif %}

), */
all_tx AS (
    /*
    SELECT *
    FROM direct_uniswapv3
    UNION ALL
    SELECT *
    FROM direct_PLP
    UNION ALL

    SELECT *

    FROM BridgeFill
    UNION ALL */

    SELECT *
    FROM newbridgefill
    UNION ALL
    SELECT *
    FROM erc20bridgetransfer
    UNION ALL
    SELECT *
    FROM v4_rfq_fills_no_bridge
    UNION ALL
    SELECT *
    FROM v4_limit_fills_no_bridge

    UNION ALL
    SELECT *
    FROM otc_fills


)

SELECT
    all_tx.tx_hash,
    tx.block_number,
    all_tx.evt_index,
    all_tx.contract_address,
    all_tx.block_time,
    try_cast(date_trunc('day', all_tx.block_time) AS DATE) AS block_date,
    maker,
    CASE
        WHEN taker = '0xdef1abe32c034e558cdd535791643c58a13acc10' THEN tx.from
        ELSE taker
    END AS taker, -- fix the user masked by ProxyContract issue
    taker_token,
    ts.symbol AS taker_symbol,
    maker_token,
    ms.symbol AS maker_symbol,
    CASE WHEN lower(ts.symbol) > lower(ms.symbol) THEN concat(ms.symbol, '-', ts.symbol) ELSE concat(ts.symbol, '-', ms.symbol) END AS token_pair,
    taker_token_amount_raw / pow(10, tp.decimals) AS taker_token_amount,
    taker_token_amount_raw,
    maker_token_amount_raw / pow(10, mp.decimals) AS maker_token_amount,
    maker_token_amount_raw,
    all_tx.type,
    affiliate_address,
    swap_flag,
    matcha_limit_order_flag,
    --COALESCE((all_tx.maker_token_amount_raw / pow(10, mp.decimals)) * mp.price, (all_tx.taker_token_amount_raw / pow(10, tp.decimals)) * tp.price) AS volume_usd
    CASE
        WHEN
            maker_token IN (
                '0x7f5c764cbc14f9669b88837ca1490cca17c31607', '0x4200000000000000000000000000000000000006', '0xda10009cbd5d07dd0cecc66161fc93d7c9000da1',
                '0x4200000000000000000000000000000000000042', '0x94b008aa00579c1307b0ef2c499ad98a8ce58e58', '0x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9'
            ) AND mp.price IS NOT NULL
            THEN (all_tx.maker_token_amount_raw / pow(10, mp.decimals)) * mp.price
        WHEN
            taker_token IN (
                '0x7f5c764cbc14f9669b88837ca1490cca17c31607', '0x4200000000000000000000000000000000000006', '0xda10009cbd5d07dd0cecc66161fc93d7c9000da1',
                '0x4200000000000000000000000000000000000042', '0x94b008aa00579c1307b0ef2c499ad98a8ce58e58', '0x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9'
            ) AND tp.price IS NOT NULL
            THEN (all_tx.taker_token_amount_raw / pow(10, tp.decimals)) * tp.price
        ELSE COALESCE((all_tx.maker_token_amount_raw / pow(10, mp.decimals)) * mp.price, (all_tx.taker_token_amount_raw / pow(10, tp.decimals)) * tp.price)
    END AS volume_usd,
    tx.from AS tx_from,
    tx.to AS tx_to,
    'optimism' AS blockchain
FROM all_tx
INNER JOIN {{ source('optimism', 'transactions') }} AS tx
    ON
        all_tx.tx_hash = tx.hash
        {% if is_incremental() %}
            AND tx.block_time >= date_trunc('day', now() - INTERVAL '1 week')
        {% endif %}
{% if not is_incremental() %}
AND tx.block_time >= '{{ zeroex_v3_start_date }}'
{% endif %}

LEFT JOIN {{ source('prices', 'usd') }} AS tp
    ON
        date_trunc('minute', all_tx.block_time) = tp.minute
        AND CASE
            WHEN all_tx.taker_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE all_tx.taker_token
        END = tp.contract_address
        AND tp.blockchain = 'optimism'

        {% if is_incremental() %}
            AND tp.minute >= date_trunc('day', now() - INTERVAL '1 week')
        {% endif %}
{% if not is_incremental() %}
AND tp.minute >= '{{ zeroex_v3_start_date }}'
{% endif %}

LEFT JOIN {{ source('prices', 'usd') }} AS mp
    ON
        DATE_TRUNC('minute', all_tx.block_time) = mp.minute
        AND CASE
            WHEN all_tx.maker_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE all_tx.maker_token
        END = mp.contract_address
        AND mp.blockchain = 'optimism'

        {% if is_incremental() %}
            AND mp.minute >= date_trunc('day', now() - INTERVAL '1 week')
        {% endif %}
{% if not is_incremental() %}
AND mp.minute >= '{{ zeroex_v3_start_date }}'
{% endif %}

LEFT OUTER JOIN {{ ref('tokens_erc20') }} AS ts ON ts.contract_address = taker_token AND ts.blockchain = 'optimism'
LEFT OUTER JOIN {{ ref('tokens_erc20') }} AS ms ON ms.contract_address = maker_token AND ms.blockchain = 'optimism';
