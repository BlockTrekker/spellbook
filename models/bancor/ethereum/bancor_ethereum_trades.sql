{{ config(
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "bancor",
                                \'["tian7"]\') }}'
    )
}}

{% set project_start_date = '2020-01-09' %}
{% set weth_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}

WITH conversions AS (
    SELECT
        t.evt_block_time,
        t._trader,
        t._toamount,
        t._fromamount,
        t._totoken,
        t._fromtoken,
        t.contract_address,
        t.evt_tx_hash,
        t.evt_index
    FROM {{ source('bancornetwork_ethereum', 'BancorNetwork_v6_evt_Conversion') }}
    {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval "1 week")
    {% endif %}
    {% if not is_incremental() %}
    WHERE t.evt_block_time >= '{{ project_start_date }}'
    {% endif %}

    UNION ALL

    SELECT
        t.evt_block_time,
        t._trader,
        t._toamount,
        t._fromamount,
        t._totoken,
        t._fromtoken,
        t.contract_address,
        t.evt_tx_hash,
        t.evt_index
    FROM {{ source('bancornetwork_ethereum', 'BancorNetwork_v7_evt_Conversion') }}
    {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval "1 week")
    {% endif %}
    {% if not is_incremental() %}
    WHERE t.evt_block_time >= '{{ project_start_date }}'
    {% endif %}

    UNION ALL

    SELECT
        t.evt_block_time,
        t._trader,
        t._toamount,
        t._fromamount,
        t._totoken,
        t._fromtoken,
        t.contract_address,
        t.evt_tx_hash,
        t.evt_index
    FROM {{ source('bancornetwork_ethereum', 'BancorNetwork_v8_evt_Conversion') }}
    {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval "1 week")
    {% endif %}
    {% if not is_incremental() %}
    WHERE t.evt_block_time >= '{{ project_start_date }}'
    {% endif %}

    UNION ALL

    SELECT
        t.evt_block_time,
        t._trader,
        t._toamount,
        t._fromamount,
        t._totoken,
        t._fromtoken,
        t.contract_address,
        t.evt_tx_hash,
        t.evt_index
    FROM {{ source('bancornetwork_ethereum', 'BancorNetwork_v9_evt_Conversion') }}
    {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval "1 week")
    {% endif %}
    {% if not is_incremental() %}
    WHERE t.evt_block_time >= '{{ project_start_date }}'
    {% endif %}

    UNION ALL

    SELECT
        t.evt_block_time,
        t._trader,
        t._toamount,
        t._fromamount,
        t._totoken,
        t._fromtoken,
        t.contract_address,
        t.evt_tx_hash,
        t.evt_index
    FROM {{ source('bancornetwork_ethereum', 'BancorNetwork_v10_evt_Conversion') }}
    {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval "1 week")
    {% endif %}
    {% if not is_incremental() %}
    WHERE t.evt_block_time >= '{{ project_start_date }}'
    {% endif %}
),

dexs AS (
    SELECT
        "1" AS version,
        t.evt_block_time AS block_time,
        t._trader AS taker,
        "" AS maker,
        t._toamount AS token_bought_amount_raw,
        t._fromamount AS token_sold_amount_raw,
        CAST(NULL AS double) AS amount_usd,
        CASE
            WHEN t._totoken = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" THEN '{{ weth_address }}'
            ELSE t._totoken
        END AS token_bought_address,
        CASE
            WHEN t._fromtoken = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" THEN '{{ weth_address }}'
            ELSE t._fromtoken
        END AS token_sold_address,
        t.contract_address AS project_contract_address,
        t.evt_tx_hash AS tx_hash,
        "" AS trace_address,
        t.evt_index
    FROM
        conversions

    UNION ALL

    SELECT
        "3" AS version,
        t.evt_block_time AS block_time,
        t.trader AS taker,
        "" AS maker,
        t.targetamount AS token_bought_amount_raw,
        t.sourceamount AS token_sold_amount_raw,
        CAST(NULL AS double) AS amount_usd,
        CASE
            WHEN t.targettoken = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" THEN '{{ weth_address }}'
            ELSE t.targettoken
        END AS token_bought_address,
        CASE
            WHEN t.sourcetoken = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" THEN '{{ weth_address }}'
            ELSE t.sourcetoken
        END AS token_sold_address,
        t.contract_address AS project_contract_address,
        t.evt_tx_hash AS tx_hash,
        "" AS trace_address,
        t.evt_index
    FROM {{ source('bancor3_ethereum', 'BancorNetwork_evt_TokensTraded') }}
    {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval "1 week")
    {% endif %}
    {% if not is_incremental() %}
    WHERE t.evt_block_time >= '{{ project_start_date }}'
    {% endif %}
)

SELECT
    "ethereum" AS blockchain,
    "Bancor Network" AS project,
    version,
    TRY_CAST(date_trunc("DAY", dexs.block_time) AS date) AS block_date,
    dexs.block_time,
    erc20a.symbol AS token_bought_symbol,
    erc20b.symbol AS token_sold_symbol,
    CASE
        WHEN lower(erc20a.symbol) > lower(erc20b.symbol) THEN concat(erc20b.symbol, "-", erc20a.symbol)
        ELSE concat(erc20a.symbol, "-", erc20b.symbol)
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
    coalesce(dexs.taker, tx.from) AS taker, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx.from AS tx_from,
    tx.to AS tx_to,
    dexs.trace_address,
    dexs.evt_index
FROM
    dexs
INNER JOIN {{ source('ethereum', 'transactions') }}
    ON
        tx.hash = dexs.tx_hash
        {% if not is_incremental() %}
    AND tx.block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND tx.block_time >= date_trunc("day", now() - interval "1 week")
        {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }}
    ON
        erc20a.contract_address = dexs.token_bought_address
        AND erc20a.blockchain = "ethereum"
LEFT JOIN {{ ref('tokens_erc20') }}
    ON
        erc20b.contract_address = dexs.token_sold_address
        AND erc20b.blockchain = "ethereum"
LEFT JOIN {{ source('prices', 'usd') }}
    ON
        p_bought.minute = date_trunc("minute", dexs.block_time)
        AND p_bought.contract_address = dexs.token_bought_address
        AND p_bought.blockchain = "ethereum"
        {% if not is_incremental() %}
    AND p_bought.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND p_bought.minute >= date_trunc("day", now() - interval "1 week")
        {% endif %}
LEFT JOIN {{ source('prices', 'usd') }}
    ON
        p_sold.minute = date_trunc("minute", dexs.block_time)
        AND p_sold.contract_address = dexs.token_sold_address
        AND p_sold.blockchain = "ethereum"
        {% if not is_incremental() %}
    AND p_sold.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND p_sold.minute >= date_trunc("day", now() - interval "1 week")
        {% endif %}
;
