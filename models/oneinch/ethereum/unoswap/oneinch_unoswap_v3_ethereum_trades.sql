{{ config(
        schema='oneinch_unoswap_v3_ethereum',
        alias='trades',
        partition_by = ['block_date'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        materialized='incremental',
        incremental_strategy='merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    )
}}

{% set project_start_date = '2021-03-15' %} --for testing, use small subset of data
{% set generic_null_address = '0x0000000000000000000000000000000000000000' %} --according to etherscan label
{% set burn_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %} --according to etherscan label
{% set blockchain = 'ethereum' %}
{% set blockchain_symbol = 'ETH' %}

WITH unoswap AS (
    SELECT
        call_block_number,
        output_returnamount,
        amount,
        srctoken,
        _0 AS pools,
        call_tx_hash,
        call_trace_address,
        call_block_time,
        contract_address
    FROM
        {{ source('oneinch_v3_ethereum', 'AggregationRouterV3_call_unoswap') }}
    WHERE
        call_success
        {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval "1 week")
        {% else %}
        AND call_block_time >= '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
        call_block_number,
        output_returnamount,
        amount,
        srctoken,
        pools,
        call_tx_hash,
        call_trace_address,
        call_block_time,
        contract_address
    FROM
        {{ source('oneinch_v3_ethereum', 'AggregationRouterV3_call_unoswapWithPermit') }}
    WHERE
        call_success
        {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval "1 week")
        {% else %}
        AND call_block_time >= '{{ project_start_date }}'
        {% endif %}
),

oneinch AS (
    SELECT
        src.call_block_number AS block_number,
        src.call_block_time AS block_time,
        "1inch" AS project,
        "UNI v2" AS version,
        tx.from AS taker,
        CAST(NULL AS string) AS maker,
        src.output_returnamount AS token_bought_amount_raw,
        src.amount AS token_sold_amount_raw,
        CAST(NULL AS double) AS amount_usd,
        CASE
            WHEN
                ll.to = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
                AND SUBSTRING(src.pools[ARRAY_SIZE(src.pools) - 1], 1, 4) IN ("0xc0", "0x40") --spark uses 0-based array index, subtract 1 from size output
                THEN '{{ burn_address }}'
            ELSE ll.to
        END AS token_bought_address,
        CASE
            WHEN src.srctoken = '{{ generic_null_address }}'
                THEN '{{ burn_address }}'
            ELSE src.srctoken
        END AS token_sold_address,
        src.contract_address AS project_contract_address,
        src.call_tx_hash AS tx_hash,
        src.call_trace_address AS trace_address,
        CAST(-1 AS integer) AS evt_index,
        tx.from AS tx_from,
        tx.to AS `tx_to`
    FROM
        unoswap
    INNER JOIN {{ source('ethereum', 'transactions') }}
        ON
            src.call_tx_hash = tx.hash
            AND src.call_block_number = tx.block_number
            {% if is_incremental() %}
                AND tx.block_time >= date_trunc("day", now() - interval "1 week")
            {% else %}
        AND tx.block_time >= '{{ project_start_date }}'
        {% endif %}
    LEFT JOIN {{ source('ethereum', 'traces') }}
        ON
            src.call_tx_hash = ll.tx_hash
            AND src.call_block_number = ll.block_number
            AND ll.trace_address = (
                CONCAT(
                    src.call_trace_address,
                ARRAY
                (
                    ARRAY_SIZE(src.pools)
                    * 2 
                    + CASE
                        WHEN src.srcToken = '{{generic_null_address}}'
                        THEN 1
                        ELSE 0
                    END
                ),
                ARRAY(0)
                )
            )
            {% if is_incremental() %}
                AND ll.block_time >= date_trunc("day", now() - interval "1 week")
            {% else %}
        AND ll.block_time >= '{{ project_start_date }}'
        {% endif %}
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
                    WHEN src.token_bought_address = '{{ burn_address }}'
                        THEN 18
                    ELSE prices_bought.decimals
                END
            )
        )
        *
        (
            CASE
                WHEN src.token_bought_address = '{{ burn_address }}'
                    THEN prices_eth.price
                ELSE prices_bought.price
            END
        ),
        (
            src.token_sold_amount_raw / power(
                10,
                CASE
                    WHEN src.token_sold_address = '{{ burn_address }}'
                        THEN 18
                    ELSE prices_sold.decimals
                END
            )
        )
        *
        (
            CASE
                WHEN src.token_sold_address = '{{ burn_address }}'
                    THEN prices_eth.price
                ELSE prices_sold.price
            END
        )
    ) AS amount_usd,
    src.token_bought_address,
    src.token_sold_address,
    src.taker,
    src.maker,
    src.project_contract_address,
    src.tx_hash,
    src.tx_from,
    src.tx_to,
    CAST(src.trace_address AS ARRAY<long>) AS trace_address,
    src.evt_index
FROM
    oneinch
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
        prices_bought.minute = date_trunc("minute", src.block_time)
        AND prices_bought.contract_address = src.token_bought_address
        AND prices_bought.blockchain = '{{ blockchain }}'
        {% if is_incremental() %}
            AND prices_bought.minute >= date_trunc("day", now() - interval "1 week")
        {% else %}
    AND prices_bought.minute >= '{{ project_start_date }}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }}
    ON
        prices_sold.minute = date_trunc("minute", src.block_time)
        AND prices_sold.contract_address = src.token_sold_address
        AND prices_sold.blockchain = '{{ blockchain }}'
        {% if is_incremental() %}
            AND prices_sold.minute >= date_trunc("day", now() - interval "1 week")
        {% else %}
    AND prices_sold.minute >= '{{ project_start_date }}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }}
    ON
        prices_eth.minute = date_trunc("minute", src.block_time)
        AND prices_eth.blockchain IS NULL
        AND prices_eth.symbol = '{{ blockchain_symbol }}'
        {% if is_incremental() %}
            AND prices_eth.minute >= date_trunc("day", now() - interval "1 week")
        {% else %}
    AND prices_eth.minute >= '{{ project_start_date }}'
    {% endif %}
