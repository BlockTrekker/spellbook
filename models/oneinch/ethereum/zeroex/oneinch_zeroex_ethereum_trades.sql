{{ config(
        schema='oneinch_zeroex_ethereum',
        alias='trades',
        partition_by = ['block_date'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        materialized='incremental',
        incremental_strategy='merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    )
}}

{% set project_start_date = '2018-09-05' %} --for testing, use small subset of data
{% set burn_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %} --according to etherscan label
{% set blockchain = 'ethereum' %}
{% set blockchain_symbol = 'ETH' %}

WITH zeroex AS (
    SELECT
        evt_block_number,
        feerecipientaddress,
        takerassetdata,
        makerassetdata,
        makeraddress,
        takeraddress,
        makerassetfilledamount,
        takerassetfilledamount,
        contract_address,
        evt_block_time,
        evt_tx_hash,
        evt_index
    FROM
        {{ source('zeroex_v2_ethereum', 'Exchange2_0_evt_Fill') }}
    WHERE
        feerecipientaddress IN
        (
            '0x910bf2d50fa5e014fd06666f456182d4ab7c8bd2',
            '0x68a17b587caf4f9329f0e372e3a78d23a46de6b5'
        )
        {% if is_incremental() %}
            AND evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% else %}
        AND evt_block_time >= '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
        evt_block_number,
        feerecipientaddress,
        takerassetdata,
        makerassetdata,
        makeraddress,
        takeraddress,
        makerassetfilledamount,
        takerassetfilledamount,
        contract_address,
        evt_block_time,
        evt_tx_hash,
        evt_index
    FROM
        {{ source('zeroex_v2_ethereum', 'Exchange2_1_evt_Fill') }}
    WHERE
        feerecipientaddress IN
        (
            '0x910bf2d50fa5e014fd06666f456182d4ab7c8bd2',
            '0x68a17b587caf4f9329f0e372e3a78d23a46de6b5'
        )
        {% if is_incremental() %}
            AND evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% else %}
        AND evt_block_time >= '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
        evt_block_number,
        feerecipientaddress,
        takerassetdata,
        makerassetdata,
        makeraddress,
        takeraddress,
        makerassetfilledamount,
        takerassetfilledamount,
        contract_address,
        evt_block_time,
        evt_tx_hash,
        evt_index
    FROM
        {{ source('zeroex_v3_ethereum', 'Exchange_evt_Fill') }}
    WHERE
        feerecipientaddress IN
        (
            '0x910bf2d50fa5e014fd06666f456182d4ab7c8bd2',
            '0x68a17b587caf4f9329f0e372e3a78d23a46de6b5'
        )
        {% if is_incremental() %}
            AND evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% else %}
        AND evt_block_time >= '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
        evt_block_number,
        feerecipient,
        takertoken,
        makertoken,
        maker,
        taker,
        makertokenfilledamount,
        takertokenfilledamount,
        contract_address,
        evt_block_time,
        evt_tx_hash,
        evt_index
    FROM
        {{ source('zeroex_ethereum', 'ExchangeProxy_evt_LimitOrderFilled') }}
    WHERE
        feerecipient IN
        (
            '0x910bf2d50fa5e014fd06666f456182d4ab7c8bd2',
            '0x68a17b587caf4f9329f0e372e3a78d23a46de6b5'
        )
        {% if is_incremental() %}
            AND evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% else %}
        AND evt_block_time >= '{{ project_start_date }}'
        {% endif %}
),

oneinch AS (
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        '1inch' AS project,
        'ZRX' AS version,
        takeraddress AS taker,
        makeraddress AS maker,
        takerassetfilledamount AS token_bought_amount_raw,
        makerassetfilledamount AS token_sold_amount_raw,
        CAST(NULL AS double) AS amount_usd,
        CONCAT('0x', substring(takerassetdata, 35, 40)) AS token_bought_address,
        CONCAT('0x', substring(makerassetdata, 35, 40)) AS token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        CAST(ARRAY() as array<bigint>) AS trace_address,
        evt_index
    FROM zeroex
)

SELECT
    '{{ blockchain }}' AS blockchain,
    src.project,
    src.version,
    date_trunc('day', src.block_time) AS block_date,
    src.block_time,
    src.block_number,
    token_bought.symbol AS token_bought_symbol,
    token_sold.symbol AS token_sold_symbol,
    CASE
        WHEN lower(token_bought.symbol) > lower(token_sold.symbol) THEN concat(token_sold.symbol, '-', token_bought.symbol)
        ELSE concat(token_bought.symbol, '-', token_sold.symbol)
    END AS token_pair,
    src.token_bought_amount_raw / power(10, token_bought.decimals) AS token_bought_amount,
    src.token_sold_amount_raw / power(10, token_sold.decimals) AS token_sold_amount,
    CAST(src.token_bought_amount_raw AS decimal(38, 0)) AS token_bought_amount_raw,
    CAST(src.token_sold_amount_raw AS decimal(38, 0)) AS token_sold_amount_raw,
    coalesce(
        src.amount_usd,
        (
            src.token_bought_amount_raw / power(
                10,
                CASE
                    WHEN token_bought_address = '{{ burn_address }}'
                        THEN 18
                    ELSE prices_bought.decimals
                END
            )
        )
        *
        (
            CASE
                WHEN token_bought_address = '{{ burn_address }}'
                    THEN prices_eth.price
                ELSE prices_bought.price
            END
        ),
        (
            src.token_sold_amount_raw / power(
                10,
                CASE
                    WHEN token_sold_address = '{{ burn_address }}'
                        THEN 18
                    ELSE prices_sold.decimals
                END
            )
        )
        *
        (
            CASE
                WHEN token_sold_address = '{{ burn_address }}'
                    THEN prices_eth.price
                ELSE prices_sold.price
            END
        )
    ) AS amount_usd,
    src.token_bought_address,
    src.token_sold_address,
    coalesce(src.taker, tx.from) AS taker,
    src.maker,
    src.project_contract_address,
    src.tx_hash,
    tx.from AS tx_from,
    tx.to AS tx_to,
    CAST(src.trace_address AS ARRAY<long>) AS trace_address,
    src.evt_index
FROM
    oneinch
INNER JOIN {{ source('ethereum', 'transactions') }}
    ON
        src.tx_hash = tx.hash
        AND src.block_number = tx.block_number
        {% if is_incremental() %}
            AND tx.block_time >= date_trunc('day', now() - interval '1 week')
        {% else %}
    AND tx.block_time >= '{{ project_start_date }}'
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }}
    ON
        token_bought.contract_address = src.token_bought_address
        AND token_bought.blockchain = '{{ blockchain }}'
LEFT JOIN {{ ref('tokens_erc20') }}
    ON
        token_sold.contract_address = src.token_sold_address
        AND token_sold.blockchain = '{{ blockchain }}'
LEFT JOIN {{ source('prices', 'usd') }}
    ON
        prices_bought.minute = date_trunc('minute', src.block_time)
        AND prices_bought.contract_address = src.token_bought_address
        AND prices_bought.blockchain = '{{ blockchain }}'
        {% if is_incremental() %}
            AND prices_bought.minute >= date_trunc('day', now() - interval '1 week')
        {% else %}
    AND prices_bought.minute >= '{{ project_start_date }}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }}
    ON
        prices_sold.minute = date_trunc('minute', src.block_time)
        AND prices_sold.contract_address = src.token_sold_address
        AND prices_sold.blockchain = '{{ blockchain }}'
        {% if is_incremental() %}
            AND prices_sold.minute >= date_trunc('day', now() - interval '1 week')
        {% else %}
    AND prices_sold.minute >= '{{ project_start_date }}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }}
    ON
        prices_eth.minute = date_trunc('minute', src.block_time)
        AND prices_eth.blockchain IS NULL
        AND prices_eth.symbol = '{{ blockchain_symbol }}'
        {% if is_incremental() %}
            AND prices_eth.minute >= date_trunc('day', now() - interval '1 week')
        {% else %}
    AND prices_eth.minute >= '{{ project_start_date }}'
    {% endif %}
