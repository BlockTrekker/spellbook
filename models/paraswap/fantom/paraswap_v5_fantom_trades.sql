{{ config(
    schema = 'paraswap_v5_fantom',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["fantom"]\',
                                "project",
                                "paraswap_v5",
                                \'["Henrystats"]\') }}'
    )
}}

{% set project_start_date = '2022-06-15' %} -- min(evet_block_time) in bought & swapped events

WITH

{% set trade_event_tables = [
    source('paraswap_fantom', 'AugustusSwapperV5_evt_BoughtV3')
    ,source('paraswap_fantom', 'AugustusSwapperV5_evt_SwappedV3')
] %}

dexs AS (
    {% for trade_tables in trade_event_tables %}
        SELECT
            evt_block_time AS block_time,
            beneficiary AS taker,
            initiator AS maker,
            receivedamount AS token_bought_amount_raw,
            srcamount AS token_sold_amount_raw,
            CAST(NULL AS double) AS amount_usd,
            CASE
                WHEN desttoken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                    THEN '0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83' -- wftm 
                ELSE desttoken
            END AS token_bought_address,
            CASE
                WHEN srctoken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                    THEN '0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83' -- wftm 
                ELSE srctoken
            END AS token_sold_address,
            contract_address AS project_contract_address,
            evt_tx_hash AS tx_hash,
            CAST(ARRAY() as array<bigint>) AS trace_address,
            evt_index
        FROM {{ trade_tables }}
        {% if is_incremental() %}
            WHERE p.evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT
    'fantom' AS blockchain,
    'paraswap' AS project,
    '5' AS version,
    try_cast(date_trunc('DAY', dexs.block_time) AS date) AS block_date,
    dexs.block_time,
    erc20a.symbol AS token_bought_symbol,
    erc20b.symbol AS token_sold_symbol,
    CASE
        WHEN lower(erc20a.symbol) > lower(erc20b.symbol) THEN concat(erc20b.symbol, '-', erc20a.symbol)
        ELSE concat(erc20a.symbol, '-', erc20b.symbol)
    END AS token_pair,
    dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
    dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount,
    CAST(dexs.token_bought_amount_raw AS decimal(38, 0)) AS token_bought_amount_raw,
    CAST(dexs.token_sold_amount_raw AS decimal(38, 0)) AS token_sold_amount_raw,
    coalesce(
        dexs.amount_usd,
        (dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price,
        (dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    coalesce(dexs.taker, tx.from) AS taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx.from AS tx_from,
    tx.to AS tx_to,
    dexs.trace_address,
    dexs.evt_index
FROM dexs
INNER JOIN {{ source('fantom', 'transactions') }}
    ON
        dexs.tx_hash = tx.hash
        {% if not is_incremental() %}
    and tx.block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND tx.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }}
    ON
        erc20a.contract_address = dexs.token_bought_address
        AND erc20a.blockchain = 'fantom'
LEFT JOIN {{ ref('tokens_erc20') }}
    ON
        erc20b.contract_address = dexs.token_sold_address
        AND erc20b.blockchain = 'fantom'
LEFT JOIN {{ source('prices', 'usd') }}
    ON
        p_bought.minute = date_trunc('minute', dexs.block_time)
        AND p_bought.contract_address = dexs.token_bought_address
        AND p_bought.blockchain = 'fantom'
        {% if not is_incremental() %}
    and p_bought.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND p_bought.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN {{ source('prices', 'usd') }}
    ON
        p_sold.minute = date_trunc('minute', dexs.block_time)
        AND p_sold.contract_address = dexs.token_sold_address
        AND p_sold.blockchain = 'fantom'
        {% if not is_incremental() %}
    and p_sold.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND p_sold.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
