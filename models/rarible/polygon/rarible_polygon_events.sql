{{ config(
    schema = 'rarible_polygon',
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["polygon"]\',
                              "project",
                              "rarible",
                              \'["springzh"]\') }}'
    )
}}

{% set nft_start_date = "2022-02-23" %}

WITH trades AS (
    SELECT
        'buy' AS trade_category,
        evt_block_time,
        evt_block_number,
        evt_tx_hash,
        contract_address,
        evt_index,
        'Trade' AS evt_type,
        rightmaker AS buyer,
        leftmaker AS seller,
        '0x' || right(substring(leftasset:data, 3, 64), 40) AS nft_contract_address,
        CAST(bytea2numeric_v3(substr(leftasset:data, 3 + 64, 64)) AS string) AS token_id,
        newrightfill AS number_of_items,
        CASE WHEN leftAsset:assetClass = '0x73ad2146' THEN 'erc721' ELSE 'erc1155' END AS token_standard, -- 0x73ad2146: erc721; 0x973bb640: erc1155
        CASE WHEN rightAsset:assetClass = '0xaaaebeba' THEN '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
            ELSE '0x' || right(substring(rightAsset:data, 3, 64), 40)
        END AS currency_contract,
        newLeftFill AS `amount_raw`
    FROM {{ source ('rarible_polygon', 'Exchange_evt_Match') }}
    WHERE rightasset:assetClass in ('0xaaaebeba', '0x8ae85d84') -- 0xaaaebeba: MATIC; 0x8ae85d84: ERC20 TOKEN
        {% if not is_incremental() %}
        AND evt_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT
        'sell' AS trade_category,
        evt_block_time,
        evt_block_number,
        evt_tx_hash,
        contract_address,
        evt_index,
        'Trade' AS evt_type,
        leftmaker AS buyer,
        rightmaker AS seller,
        '0x' || right(substring(rightasset:data, 3, 64), 40) AS nft_contract_address,
        CAST(bytea2numeric_v3(substr(rightasset:data, 3 + 64, 64)) AS string) AS token_id,
        newleftfill AS number_of_items,
        CASE WHEN rightAsset:assetClass = '0x73ad2146' THEN 'erc721' ELSE 'erc1155' END AS token_standard, -- 0x73ad2146: erc721; 0x973bb640: erc1155
        CASE WHEN leftAsset:assetClass = '0xaaaebeba' THEN '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
            ELSE '0x' || right(substring(leftAsset:data, 3, 64), 40)
        END AS currency_contract,
        newRightFill AS `amount_raw`
    FROM {{ source ('rarible_polygon', 'Exchange_evt_Match') }}
    WHERE leftasset:assetClass in ('0xaaaebeba', '0x8ae85d84') -- 0xaaaebeba: MATIC; 0x8ae85d84: ERC20 TOKEN
        {% if not is_incremental() %}
        AND evt_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),

-- note: this logic will probably not hold for multi trade transactions
trade_amount_detail AS (
    SELECT
        e.block_number AS evt_block_number,
        e.tx_hash AS evt_tx_hash,
        cast(e.value AS double) AS amount_raw,
        row_number() OVER (PARTITION BY e.tx_hash ORDER BY e.trace_address) AS `item_index`
    FROM {{ source('polygon', 'traces') }}
    INNER JOIN trades
        ON
            e.block_number = t.evt_block_number
            AND e.tx_hash = t.evt_tx_hash
            {% if not is_incremental() %}
        AND e.block_time >= '{{ nft_start_date }}'
        {% endif %}
            {% if is_incremental() %}
                AND e.block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    WHERE
        t.currency_contract = '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
        AND cast(e.value AS double) > 0
        AND cardinality(trace_address) > 0 -- exclude the main call record

    UNION ALL

    SELECT
        e.evt_block_number,
        e.evt_tx_hash,
        CAST(e.value AS double) AS amount_raw,
        row_number() OVER (PARTITION BY e.evt_tx_hash ORDER BY e.evt_index) AS `item_index`
    FROM {{ source('erc20_polygon', 'evt_transfer') }}
    INNER JOIN trades
        ON
            e.evt_block_number = t.evt_block_number
            AND e.evt_tx_hash = t.evt_tx_hash
            {% if not is_incremental() %}
        AND e.evt_block_time >= '{{ nft_start_date }}'
        {% endif %}
            {% if is_incremental() %}
                AND e.evt_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    WHERE t.currency_contract != '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
),

trade_amount_summary AS (
    SELECT
        evt_block_number,
        evt_tx_hash,
        amount_raw,
        -- When there is royalty fee, it is the first transfer
        (CASE WHEN transfer_count >= 4 THEN amount_raw_2 ELSE amount_raw_1 END) AS platform_fee_amount_raw,
        (CASE WHEN transfer_count >= 4 THEN amount_raw_1 ELSE 0 END) AS `royalty_fee_amount_raw`
    FROM (
        SELECT
            evt_block_number,
            evt_tx_hash,
            sum(amount_raw) AS amount_raw,
            sum(CASE WHEN item_index = 1 THEN amount_raw ELSE 0 END) AS amount_raw_1,
            sum(CASE WHEN item_index = 2 THEN amount_raw ELSE 0 END) AS amount_raw_2,
            sum(CASE WHEN item_index = 3 THEN amount_raw ELSE 0 END) AS amount_raw_3,
            count(*) AS `transfer_count`
        FROM trade_amount_detail
        GROUP BY 1, 2
    )
)

SELECT
    'polygon' AS blockchain,
    'rarible' AS project,
    'v2' AS version,
    a.evt_tx_hash AS tx_hash,
    date_trunc('day', a.evt_block_time) AS block_date,
    a.evt_block_time AS block_time,
    a.evt_block_number AS block_number,
    coalesce(s.amount_raw, 0) / power(10, erc.decimals) * p.price AS amount_usd,
    coalesce(s.amount_raw, 0) / power(10, erc.decimals) AS amount_original,
    coalesce(s.amount_raw, 0) AS amount_raw,
    CASE WHEN erc.symbol = 'WMATIC' THEN 'MATIC' ELSE erc.symbol END AS currency_symbol,
    a.currency_contract,
    token_id,
    token_standard,
    a.contract_address AS project_contract_address,
    evt_type,
    CAST(NULL AS string) AS collection,
    CASE WHEN number_of_items = 1 THEN 'Single Item Trade' ELSE 'Bundle Trade' END AS trade_type,
    CAST(number_of_items AS decimal(38, 0)) AS number_of_items,
    a.trade_category,
    a.buyer,
    a.seller,
    a.nft_contract_address,
    agg.name AS aggregator_name,
    agg.contract_address AS aggregator_address,
    t.`from` AS tx_from,
    t.`to` AS tx_to,
    coalesce(s.platform_fee_amount_raw, 0) AS platform_fee_amount_raw,
    CAST(coalesce(s.platform_fee_amount_raw, 0) / power(10, erc.decimals) AS double) AS platform_fee_amount,
    CAST(coalesce(s.platform_fee_amount_raw, 0) / power(10, erc.decimals) * p.price AS double) AS platform_fee_amount_usd,
    CAST(coalesce(s.platform_fee_amount_raw, 0) / s.amount_raw * 100 AS double) AS platform_fee_percentage,
    CAST(coalesce(s.royalty_fee_amount_raw, 0) AS double) AS royalty_fee_amount_raw,
    CAST(coalesce(s.royalty_fee_amount_raw, 0) / power(10, erc.decimals) AS double) AS royalty_fee_amount,
    CAST(coalesce(s.royalty_fee_amount_raw, 0) / power(10, erc.decimals) * p.price AS double) AS royalty_fee_amount_usd,
    CAST(coalesce(s.royalty_fee_amount_raw, 0) / s.amount_raw * 100 AS double) AS royalty_fee_percentage,
    CAST(NULL AS varchar(5)) AS royalty_fee_receive_address,
    CAST(NULL AS string) AS royalty_fee_currency_symbol,
    a.evt_tx_hash || '-' || a.evt_type || '-' || a.evt_index || '-' || a.token_id || '-' || CAST(a.number_of_items AS string) AS `unique_trade_id`
FROM trades
INNER JOIN {{ source('polygon','transactions') }}
    ON
        a.evt_block_number = t.block_number
        AND a.evt_tx_hash = t.hash
        {% if not is_incremental() %}
    AND t.block_time >= '{{ nft_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND t.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN trade_amount_summary ON a.evt_block_number = s.evt_block_number AND a.evt_tx_hash = s.evt_tx_hash
LEFT JOIN {{ ref('tokens_erc20') }} ON erc.blockchain = 'polygon' AND erc.contract_address = a.currency_contract
LEFT JOIN {{ source('prices', 'usd') }}
    ON
        p.contract_address = a.currency_contract AND p.minute = date_trunc('minute', a.evt_block_time)
        {% if not is_incremental() %}
    AND p.minute >= '{{ nft_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND p.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN {{ ref('nft_aggregators') }} ON agg.blockchain = 'polygon' AND agg.contract_address = t.`to`
