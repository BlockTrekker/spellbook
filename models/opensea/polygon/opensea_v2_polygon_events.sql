{{ config(
    schema = 'opensea_v2_polygon',
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["polygon"]\',
                              "project",
                              "opensea",
                              \'["springzh"]\') }}'
    )
}}

{% set nft_start_date='2021-06-28' %}

WITH trades AS (
    SELECT
        'Buy' AS trade_category,
        call_block_time AS block_time,
        call_block_number AS block_number,
        call_tx_hash AS tx_hash,
        a.contract_address,
        CAST(0 AS integer) AS evt_index,
        'Trade' AS evt_type,
        a.leftorder:makerAddress AS buyer,
      a.rightOrder:makerAddress AS seller,
      '0x' || right(substring(a.rightOrder:makerAssetData, 11, 64), 40) AS nft_contract_address,
      case when length(a.rightOrder:makerAssetData) = 650 then cast(bytea2numeric_v3(substr(a.rightOrder:makerAssetData,332,64)) as string)
            else cast(bytea2numeric_v3(substr(a.rightOrder:makerAssetData,76,64)) as string)
      end AS token_id,
      least(cast(output_matchedFillResults:left:takerFeePaid as numeric), cast(output_matchedFillResults:right:makerFeePaid as numeric)) AS number_of_items,
      case when length(a.rightOrder:makerAssetData) = 650 then 'erc1155'
            else 'erc721' -- 138
       end AS token_standard, 
      paymentTokenAddress AS currency_contract,
      least(cast(output_matchedFillResults:left:makerFeePaid as decimal(38,0)), cast(output_matchedFillResults:right:takerFeePaid as decimal(38,0))) AS amount_raw,
      2.5 AS platform_fee,
      feeData[0]:recipient AS fee_recipient,
      case when length(feeData[1]:recipient) > 0 then 2.5 else 0 end AS `royalty_fee`
    FROM {{ source('opensea_polygon_v2_polygon','ZeroExFeeWrapper_call_matchOrders') }}
    WHERE
        1 = 1
        AND call_success
        {% if not is_incremental() %}
    AND a.call_block_time >= '{{ nft_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND a.call_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
),

trade_amount_detail AS (
    SELECT
        e.evt_block_number,
        e.evt_tx_hash,
        CAST(e.value AS double) AS amount_raw,
        e.`to` AS receive_address,
        row_number() OVER (PARTITION BY e.evt_tx_hash ORDER BY e.evt_index) AS `item_index`
    FROM {{ source('erc20_polygon', 'evt_transfer') }}
    INNER JOIN trades
        ON
            e.evt_block_number = t.block_number
            AND e.evt_tx_hash = t.tx_hash
            {% if not is_incremental() %}
        AND e.evt_block_time >= '{{ nft_start_date }}'
        {% endif %}
            {% if is_incremental() %}
                AND e.evt_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    WHERE e.`from` = '0xf715beb51ec8f63317d66f491e37e7bb048fcc2d' -- All fees are transferred to this address then split to other addresses
),

trade_amount_grouped AS (
    SELECT
        evt_block_number,
        evt_tx_hash,
        sum(CASE WHEN item_index = 1 THEN amount_raw ELSE 0 END) AS fee_amount_raw_1,
        max(CASE WHEN item_index = 1 THEN receive_address END) AS receive_address,
        sum(CASE WHEN item_index = 2 THEN amount_raw ELSE 0 END) AS fee_amount_raw_2,
        count(*) AS `row_count`
    FROM trade_amount_detail
    GROUP BY 1, 2
),

trade_amount_summary AS (
    SELECT
        evt_block_number,
        evt_tx_hash,
        -- Some tx has no royalty fee: https://polygonscan.com/tx/0x7a583aa2ac9aa7b25fdf969ddc7e3a860f4565e4e48e83c2d5d513355dd952a5
        (CASE WHEN row_count = 2 THEN fee_amount_raw_2 ELSE fee_amount_raw_1 END) AS platform_fee_amount_raw,
        (CASE WHEN row_count = 2 THEN receive_address END) AS royalty_fee_receive_address,
        (CASE WHEN row_count = 2 THEN fee_amount_raw_1 END) AS `royalty_fee_amount_raw`
    FROM trade_amount_grouped
)

SELECT
    'polygon' AS blockchain,
    'opensea' AS project,
    'v2' AS version,
    a.tx_hash,
    date_trunc('day', a.block_time) AS block_date,
    a.block_time,
    a.block_number,
    CAST(a.amount_raw / power(10, erc20.decimals) * p.price AS double) AS amount_usd,
    CAST(a.amount_raw / power(10, erc20.decimals) AS double) AS amount_original,
    CAST(a.amount_raw AS decimal(38, 0)) AS amount_raw,
    erc20.symbol AS currency_symbol,
    a.currency_contract,
    a.token_id,
    a.token_standard,
    a.contract_address AS project_contract_address,
    a.evt_type,
    CAST(null AS string) AS collection,
    CASE WHEN a.number_of_items = 1 THEN 'Single Item Trade' ELSE 'Bundle Trade' END AS trade_type,
    CAST(coalesce(a.number_of_items, 1) AS decimal(38, 0)) AS number_of_items,
    a.trade_category,
    a.buyer,
    a.seller,
    a.nft_contract_address,
    agg.name AS aggregator_name,
    agg.contract_address AS aggregator_address,
    t.`from` AS tx_from,
    t.`to` AS tx_to,
    CAST(f.platform_fee_amount_raw AS double) AS platform_fee_amount_raw,
    CAST(f.platform_fee_amount_raw / power(10, erc20.decimals) AS double) AS platform_fee_amount,
    CAST(f.platform_fee_amount_raw / power(10, erc20.decimals) * p.price AS double) AS platform_fee_amount_usd,
    CAST(f.platform_fee_amount_raw / a.amount_raw * 100 AS double) AS platform_fee_percentage,
    CAST(f.royalty_fee_amount_raw AS double) AS royalty_fee_amount_raw,
    CAST(f.royalty_fee_amount_raw / power(10, erc20.decimals) AS double) AS royalty_fee_amount,
    CAST(f.royalty_fee_amount_raw / power(10, erc20.decimals) * p.price AS double) AS royalty_fee_amount_usd,
    CAST(f.royalty_fee_amount_raw / a.amount_raw * 100 AS double) AS royalty_fee_percentage,
    f.royalty_fee_receive_address,
    erc20.symbol AS royalty_fee_currency_symbol,
    a.tx_hash || '-' || a.evt_type || '-' || a.evt_index || '-' || a.token_id || '-' || cast(coalesce(a.number_of_items, 1) AS string) AS `unique_trade_id`
FROM trades
INNER JOIN {{ source('polygon','transactions') }}
    ON
        a.block_number = t.block_number AND a.tx_hash = t.hash
        {% if not is_incremental() %}
    AND t.block_time >= '{{ nft_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND t.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN trade_amount_summary ON a.block_number = f.evt_block_number AND a.tx_hash = f.evt_tx_hash
LEFT JOIN {{ source('prices', 'usd') }}
    ON
        p.minute = date_trunc('minute', a.block_time)
        AND p.contract_address = a.currency_contract
        AND p.blockchain = 'polygon'
        {% if not is_incremental() %}
    AND minute >= '{{ nft_start_date }}' 
    {% endif %}
        {% if is_incremental() %}
            AND minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} ON erc20.contract_address = a.currency_contract AND erc20.blockchain = 'polygon'
LEFT JOIN {{ ref('nft_aggregators') }} ON agg.blockchain = 'polygon' AND agg.contract_address = t.`to`
