{{ config(
    schema = 'openocean_v2_avalanche_c',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "openocean_v2",
                                \'["Henrystats"]\') }}'
    )
}}

{% set project_start_date = '2021-09-09' %}

WITH

dexs AS (
    SELECT
        evt_block_time AS block_time,
        dstreceiver AS taker,
        '' AS maker,
        returnamount AS token_bought_amount_raw,
        spentamount AS token_sold_amount_raw,
        CAST(NULL AS double) AS amount_usd,
        CASE
            WHEN CAST(dsttoken AS string) IN ('0', 'O', '0x0000000000000000000000000000000000000000')
                THEN '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7' -- wavax 
            ELSE CAST(dsttoken AS string)
        END AS token_bought_address,
        CASE
            WHEN CAST(srctoken AS string) IN ('0', 'O', '0x0000000000000000000000000000000000000000')
                THEN '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7' -- wavax 
            ELSE CAST(srctoken AS string)
        END AS token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        CAST(ARRAY() as array<bigint>) AS trace_address,
        evt_index
    FROM
        {{ source('openocean_v2_avalanche_c', 'OpenOceanExchangeProxy_evt_Swapped') }}
    {% if not is_incremental() %}
    WHERE evt_block_time >= '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
)

SELECT
    'avalanche_c' AS blockchain,
    'openocean' AS project,
    '2' AS version,
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
    COALESCE(
        dexs.amount_usd,
        (dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price,
        (dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    COALESCE(dexs.taker, tx.from) AS taker,  -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx.from AS tx_from,
    tx.to AS tx_to,
    dexs.trace_address,
    dexs.evt_index
FROM dexs
INNER JOIN {{ source('avalanche_c', 'transactions') }}
    ON
        tx.hash = dexs.tx_hash
        {% if not is_incremental() %}
    AND tx.block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND tx.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }}
    ON
        erc20a.contract_address = dexs.token_bought_address
        AND erc20a.blockchain = 'avalanche_c'
LEFT JOIN {{ ref('tokens_erc20') }}
    ON
        erc20b.contract_address = dexs.token_sold_address
        AND erc20b.blockchain = 'avalanche_c'
LEFT JOIN {{ source('prices', 'usd') }}
    ON
        p_bought.minute = date_trunc('minute', dexs.block_time)
        AND p_bought.contract_address = dexs.token_bought_address
        AND p_bought.blockchain = 'avalanche_c'
        {% if not is_incremental() %}
    AND p_bought.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND p_bought.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN {{ source('prices', 'usd') }}
    ON
        p_sold.minute = date_trunc('minute', dexs.block_time)
        AND p_sold.contract_address = dexs.token_sold_address
        AND p_sold.blockchain = 'avalanche_c'
        {% if not is_incremental() %}
    AND p_sold.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND p_sold.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
