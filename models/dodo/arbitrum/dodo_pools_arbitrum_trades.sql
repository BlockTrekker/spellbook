{{ config
(
    alias ='pool_trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "project",
                                    "dodo",
                                    \'["owen05"]\') }}'
)
}}

{% set project_start_date = '2021-08-30' %}

{% set dodo_proxies = [
"0xd5a7e197bace1f3b26e2760321d6ce06ad07281a", 
"0x8ab2d334ce64b50be9ab04184f7ccba2a6bb6391"
] %}

WITH dodo_view_markets (market_contract_address, base_token_symbol, quote_token_symbol, base_token_address, quote_token_address) AS (
    VALUES
    (lower('0xFE176A2b1e1F67250d2903B8d25f56C0DaBcd6b2'), 'WETH', 'USDC', lower('0x82aF49447D8a07e3bd95BD0d56f35241523fBab1'), lower('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8')),
    (lower('0xe4B2Dfc82977dd2DCE7E8d37895a6A8F50CbB4fB'), 'USDT', 'USDC', lower('0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9'), lower('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8')),
    (lower('0xb42a054D950daFD872808B3c839Fbb7AFb86E14C'), 'WBTC', 'USDC', lower('0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f'), lower('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8'))
),

dexs AS (
    -- dodo v1 sell
    SELECT
        s.evt_block_time AS block_time,
        'DODO' AS project,
        '1' AS version,
        s.seller AS taker,
        '' AS maker,
        s.paybase AS token_bought_amount_raw,
        s.receivequote AS token_sold_amount_raw,
        cast(NULL AS double) AS amount_usd,
        m.base_token_address AS token_bought_address,
        m.quote_token_address AS token_sold_address,
        s.contract_address AS project_contract_address,
        s.evt_tx_hash AS tx_hash,
        '' AS trace_address,
        s.evt_index
    FROM
        {{ source('dodo_arbitrum', 'DODO_evt_SellBaseToken') }} AS s
    LEFT JOIN dodo_view_markets AS m
        ON s.contract_address = m.market_contract_address
    WHERE
        {% for dodo_proxy in dodo_proxies %}
            s.seller != '{{ dodo_proxy }}'
            {% if not loop.last %}
                AND
            {% endif %}
        {% endfor %}
        {% if is_incremental() %}
            AND s.evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}

    UNION ALL

    -- dodo v1 buy
    SELECT
        b.evt_block_time AS block_time,
        'DODO' AS project,
        '1' AS version,
        b.buyer AS taker,
        '' AS maker,
        b.receivebase AS token_bought_amount_raw,
        b.payquote AS token_sold_amount_raw,
        cast(NULL AS double) AS amount_usd,
        m.base_token_address AS token_bought_address,
        m.quote_token_address AS token_sold_address,
        b.contract_address AS project_contract_address,
        b.evt_tx_hash AS tx_hash,
        '' AS trace_address,
        b.evt_index
    FROM
        {{ source('dodo_arbitrum','DODO_evt_BuyBaseToken') }} AS b
    LEFT JOIN dodo_view_markets AS m
        ON b.contract_address = m.market_contract_address
    WHERE
        {% for dodo_proxy in dodo_proxies %}
            b.buyer != '{{ dodo_proxy }}'
            {% if not loop.last %}
                AND
            {% endif %}
        {% endfor %}
        {% if is_incremental() %}
            AND b.evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}

    UNION ALL

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
        {{ source('dodo_arbitrum', 'dvm_evt_DODOSwap') }}
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

    -- dodov2 dppOracle
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
        {{ source('dodo_arbitrum', 'DPPOracle_evt_DODOSwap') }}
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
        {{ source('dodo_arbitrum', 'dsp_evt_DODOSwap') }}
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
    'arbitrum' AS blockchain,
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
INNER JOIN {{ source('arbitrum', 'transactions') }} AS tx
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
        AND erc20a.blockchain = 'arbitrum'
LEFT JOIN {{ ref('tokens_erc20') }} AS erc20b
    ON
        erc20b.contract_address = dexs.token_sold_address
        AND erc20b.blockchain = 'arbitrum'
LEFT JOIN {{ source('prices', 'usd') }} AS p_bought
    ON
        p_bought.minute = date_trunc('minute', dexs.block_time)
        AND p_bought.contract_address = dexs.token_bought_address
        AND p_bought.blockchain = 'arbitrum'
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
        AND p_sold.blockchain = 'arbitrum'
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
