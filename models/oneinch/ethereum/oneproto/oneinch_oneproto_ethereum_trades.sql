{{ config(
        schema='oneinch_oneproto_ethereum',
        alias='trades',
        partition_by = ['block_date'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        materialized='incremental',
        incremental_strategy='merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    )
}}

{% set project_start_date = '2020-07-21' %} --for testing, use small subset of data
{% set generic_null_address = '0x0000000000000000000000000000000000000000' %} --according to etherscan label
{% set burn_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %} --according to etherscan label
{% set blockchain = 'ethereum' %}
{% set blockchain_symbol = 'ETH' %}

WITH oneproto AS (
    SELECT
        evt_block_number AS block_number,
        CAST(NULL AS string) AS taker,
        fromtoken AS from_token,
        desttoken AS to_token,
        fromtokenamount AS from_amount,
        desttokenamount AS to_amount,
        evt_tx_hash AS tx_hash,
        evt_block_time AS block_time,
        contract_address,
        evt_index
    FROM
        {{ source('oneproto_ethereum', 'OneSplitAudit_evt_Swapped') }}
    {% if is_incremental() %}
        WHERE
            evt_block_time >= date_trunc("day", now() - interval "1 week")
    {% else %}
    WHERE
        evt_block_time >= '{{ project_start_date }}'
    {% endif %}
),

oneinch AS (
    SELECT
        block_number,
        block_time,
        "1inch" AS project,
        "1proto" AS version,
        taker,
        CAST(NULL AS string) AS maker,
        to_amount AS token_bought_amount_raw,
        from_amount AS token_sold_amount_raw,
        CAST(NULL AS double) AS amount_usd,
        CASE
            WHEN to_token = '{{ generic_null_address }}'
                THEN '{{ burn_address }}'
            ELSE to_token
        END AS token_bought_address,
        CASE
            WHEN from_token = '{{ generic_null_address }}'
                THEN '{{ burn_address }}'
            ELSE from_token
        END AS token_sold_address,
        contract_address AS project_contract_address,
        tx_hash,
        CAST(ARRAY() as array<bigint>) AS trace_address,
        evt_index
    FROM oneproto
)

SELECT
    '{{ blockchain }}' AS blockchain,
    src.project,
    src.version,
    date_trunc("day", src.block_time) AS block_date,
    src.block_time,
    src.block_number,
    token_bought.symbol AS token_bought_symbol,
    token_sold.symbol AS token_sold_symbol,
    CASE
        WHEN lower(token_bought.symbol) > lower(token_sold.symbol) THEN concat(token_sold.symbol, "-", token_bought.symbol)
        ELSE concat(token_bought.symbol, "-", token_sold.symbol)
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
LEFT ANTI JOIN --where tx_hash isn't already pulled from oneinch version spells
    (
        SELECT DISTINCT
            tx_hash
            , block_number
        FROM
            {{ ref('oneinch_v1_ethereum_trades') }}
        UNION ALL
        SELECT DISTINCT
            tx_hash
            , block_number
        FROM
            {{ ref('oneinch_v2_ethereum_trades') }}
        UNION ALL
        SELECT DISTINCT
            tx_hash
            , block_number
        FROM
            {{ ref('oneinch_v3_ethereum_trades') }}
        UNION ALL
        SELECT DISTINCT
            tx_hash
            , block_number
        FROM
            {{ ref('oneinch_v4_ethereum_trades') }}
        UNION ALL
        SELECT DISTINCT
            tx_hash
            , block_number
        FROM
            {{ ref('oneinch_v5_ethereum_trades') }}
    ) oneinch
    ON src.tx_hash = oneinch.tx_hash
    AND src.block_number = oneinch.block_number
INNER JOIN {{ source('ethereum', 'transactions') }} AS `tx`
    ON src.tx_hash = tx.hash
    AND src.block_number = tx.block_number
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    AND tx.block_time >= '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} AS `token_bought`
    ON token_bought.contract_address = src.token_bought_address
    AND token_bought.blockchain = '{{blockchain}}'
LEFT JOIN {{ ref('tokens_erc20') }} AS `token_sold`
    ON token_sold.contract_address = src.token_sold_address
    AND token_sold.blockchain = '{{blockchain}}'
LEFT JOIN {{ source('prices', 'usd') }} AS `prices_bought`
    ON prices_bought.minute = date_trunc('minute', src.block_time)
    AND prices_bought.contract_address = src.token_bought_address
    AND prices_bought.blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    AND prices_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% else %}
    AND prices_bought.minute >= '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} AS `prices_sold`
    ON prices_sold.minute = date_trunc('minute', src.block_time)
    AND prices_sold.contract_address = src.token_sold_address
    AND prices_sold.blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    AND prices_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% else %}
    AND prices_sold.minute >= '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} AS `prices_eth`
    ON prices_eth.minute = date_trunc('minute', src.block_time)
    AND prices_eth.blockchain is null
    AND prices_eth.symbol = '{{blockchain_symbol}}'
    {% if is_incremental() %}
    AND prices_eth.minute >= date_trunc("day", now() - interval '1 week')
{% else %}
    AND prices_eth.minute >= '{{ project_start_date }}'
    {% endif %}
