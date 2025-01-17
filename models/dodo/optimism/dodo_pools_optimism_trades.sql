{{ config
(
    alias ='pool_trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "dodo",
                                    \'["owen05"]\') }}'
)
}}

{% set project_start_date = '2022-04-18' %}

-- dodo V1 & V2 adapters
{% set dodo_proxies = [
"0xdd0951b69bc0cf9d39111e5037685fb573204c86",
"0x169ae3d5acc90f0895790f6321ee81cb040e8a6b"
] %}

WITH dexs AS (
    -- dodov2 dvm
    SELECT
        evt_block_time AS block_time,
        'DODO' AS project,
        '2_dvm' AS version,
        trader AS taker,
        receiver AS maker,
        fromamount AS token_bought_amount_raw,
        toamount AS token_sold_amount_raw,
        cast(NULL AS double) AS amount_usd,
        fromtoken AS token_bought_address,
        totoken AS token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM
        {{ source('dodo_optimism', 'DVM_evt_DODOSwap') }}
    WHERE
        {% for dodo_proxy in dodo_proxies %}
            trader != '{{ dodo_proxy }}'
            {% if not loop.last %}
                AND
            {% endif %}
        {% endfor %}
        {% if is_incremental() %}
            AND evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}

    UNION ALL

    -- dodov2 dpp
    SELECT
        evt_block_time AS block_time,
        'DODO' AS project,
        '2_dpp' AS version,
        trader AS taker,
        receiver AS maker,
        fromamount AS token_bought_amount_raw,
        toamount AS token_sold_amount_raw,
        cast(NULL AS double) AS amount_usd,
        fromtoken AS token_bought_address,
        totoken AS token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM
        {{ source('dodo_optimism', 'DPP_evt_DODOSwap') }}
    WHERE
        {% for dodo_proxy in dodo_proxies %}
            trader != '{{ dodo_proxy }}'
            {% if not loop.last %}
                AND
            {% endif %}
        {% endfor %}
        {% if is_incremental() %}
            AND evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}

    UNION ALL

    -- dodov2 dsp
    SELECT
        evt_block_time AS block_time,
        'DODO' AS project,
        '2_dsp' AS version,
        trader AS taker,
        receiver AS maker,
        fromamount AS token_bought_amount_raw,
        toamount AS token_sold_amount_raw,
        cast(NULL AS double) AS amount_usd,
        fromtoken AS token_bought_address,
        totoken AS token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM
        {{ source('dodo_optimism', 'DSP_evt_DODOSwap') }}
    WHERE
        {% for dodo_proxy in dodo_proxies %}
            trader != '{{ dodo_proxy }}'
            {% if not loop.last %}
                AND
            {% endif %}
        {% endfor %}
        {% if is_incremental() %}
            AND evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
)

SELECT
    'optimism' AS blockchain,
    project,
    dexs.version AS version,
    TRY_CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date,
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
        (dexs.token_bought_amount_raw / power(10, (CASE dexs.token_bought_address WHEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 18 ELSE p_bought.decimals END))) * (CASE dexs.token_bought_address WHEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN p_eth.price ELSE p_bought.price END),
        (dexs.token_sold_amount_raw / power(10, (CASE dexs.token_sold_address WHEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 18 ELSE p_sold.decimals END))) * (CASE dexs.token_sold_address WHEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN p_eth.price ELSE p_sold.price END)
    ) AS amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    coalesce(dexs.taker, tx.from) AS taker, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx.from AS tx_from,
    tx.to AS tx_to,
    dexs.trace_address,
    dexs.evt_index
FROM dexs
INNER JOIN {{ source('optimism', 'transactions') }} AS tx
    ON
        dexs.tx_hash = tx.hash
        {% if not is_incremental() %}
    AND tx.block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND tx.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} AS erc20a
    ON
        erc20a.contract_address = dexs.token_bought_address
        AND erc20a.blockchain = 'optimism'
LEFT JOIN {{ ref('tokens_erc20') }} AS erc20b
    ON
        erc20b.contract_address = dexs.token_sold_address
        AND erc20b.blockchain = 'optimism'
LEFT JOIN {{ source('prices', 'usd') }} AS p_bought
    ON
        p_bought.minute = date_trunc('minute', dexs.block_time)
        AND p_bought.contract_address = dexs.token_bought_address
        AND p_bought.blockchain = 'optimism'
        {% if not is_incremental() %}
    AND p_bought.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND p_bought.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} AS p_sold
    ON
        p_sold.minute = date_trunc('minute', dexs.block_time)
        AND p_sold.contract_address = dexs.token_sold_address
        AND p_sold.blockchain = 'optimism'
        {% if not is_incremental() %}
    AND p_sold.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND p_sold.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} AS p_eth
    ON
        p_eth.minute = date_trunc('minute', dexs.block_time)
        AND p_eth.blockchain IS NULL
        AND p_eth.symbol = 'ETH'
        {% if not is_incremental() %}
    AND p_eth.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND p_eth.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
WHERE dexs.token_bought_address != dexs.token_sold_address;
