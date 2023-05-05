{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'bundle_index' ],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "tofu",
                                \'["Henrystats"]\') }}')
}}

{%- set ARETH_ERC20_ADDRESS = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' %}
{% set project_start_date = '2021-12-09' %}

WITH tff AS (
    SELECT
        call_block_time,
        call_tx_hash,
        fee_rate,
        royalty_rate,
        fee_address,
        royalty_address,
        bundle_size,
        get_json_object(t, '$.token') AS token,
        get_json_object(t, '$.tokenId') AS token_id,
        get_json_object(t, '$.amount') AS amount,
        i AS `bundle_index`
    FROM (
        SELECT
            call_block_time,
            call_tx_hash,
            get_json_object(get_json_object(detail, '$.settlement'), '$.feeRate') / 1000000 AS fee_rate,
            get_json_object(get_json_object(detail, '$.settlement'), '$.royaltyRate') / 1000000 AS royalty_rate,
            get_json_object(get_json_object(detail, '$.settlement'), '$.feeAddress') AS fee_address,
            get_json_object(get_json_object(detail, '$.settlement'), '$.royaltyAddress') AS royalty_address,
            posexplode(from_json(get_json_object(detail, '$.bundle'), 'array<string>')) as (i,t),
                 json_array_length(get_json_object(detail, '$.bundle'))                              AS `bundle_size`
        FROM {{ source('tofunft_arbitrum', 'MarketNG_call_run') }}
        WHERE
            call_success = true
            {% if is_incremental() %}
                AND call_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    ) AS `tmp`
),

tfe AS (
    SELECT
        evt_tx_hash,
        evt_block_time,
        evt_block_number,
        evt_index,
        get_json_object(inventory, '$.seller') AS seller,
        get_json_object(inventory, '$.buyer') AS buyer,
        get_json_object(inventory, '$.kind') AS kind,
        get_json_object(inventory, '$.price') AS price,
        CASE
            WHEN get_json_object(inventory, '$.currency') = '0x0000000000000000000000000000000000000000'
                THEN '{{ ARETH_ERC20_ADDRESS }}'
            ELSE get_json_object(inventory, '$.currency')
        END AS currency,
        (get_json_object(inventory, '$.currency') = '0x0000000000000000000000000000000000000000') AS native_eth,
        contract_address
    FROM {{ source('tofunft_arbitrum', 'MarketNG_evt_EvInventoryUpdate') }}
    WHERE
        get_json_object(inventory, '$.status') = '1'
        {% if is_incremental() %}
            AND evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
)

SELECT
    'arbitrum' AS blockchain,
    'tofu' AS project,
    'v1' AS version,
    date_trunc('day', tfe.evt_block_time) AS block_date,
    tfe.evt_block_time AS block_time,
    tfe.evt_block_number AS block_number,
    tff.token_id AS token_id,
    nft.standard AS token_standard,
    nft.name AS collection,
    CASE
        WHEN tff.bundle_size = 1 THEN 'Single Item Trade'
        ELSE 'Bundle Trade'
    END AS trade_type,
    CAST(tff.amount AS decimal(38, 0)) AS number_of_items,
    'Trade' AS evt_type,
    tfe.seller AS seller,
    tfe.buyer AS buyer,
    CASE
        WHEN tfe.kind = '1' THEN 'Buy'
        WHEN tfe.kind = '2' THEN 'Sell'
        ELSE 'Auction'
    END AS trade_category,
    CAST(tfe.price AS decimal(38, 0)) AS amount_raw,
    tfe.price / power(10, pu.decimals) AS amount_original,
    pu.price * tfe.price / power(10, pu.decimals) AS amount_usd,
    CASE
        WHEN tfe.native_eth THEN 'ARETH'
        ELSE pu.symbol
    END AS currency_symbol,
    tfe.currency AS currency_contract,
    tfe.contract_address AS project_contract_address,
    tff.token AS nft_contract_address,
    agg.name AS aggregator_name,
    agg.contract_address AS aggregator_address,
    tfe.evt_tx_hash AS tx_hash,
    tx.from AS tx_from,
    tx.to AS tx_to,
    CAST(tfe.price * tff.fee_rate AS double) AS platform_fee_amount_raw,
    CAST(tfe.price * tff.fee_rate / power(10, pu.decimals) AS double) AS platform_fee_amount,
    CAST(pu.price * tfe.price * tff.fee_rate / power(10, pu.decimals) AS double) AS platform_fee_amount_usd,
    CAST(100 * tff.fee_rate AS double) AS platform_fee_percentage,
    tfe.price * tff.royalty_rate AS royalty_fee_amount_raw,
    tfe.price * tff.royalty_rate / power(10, pu.decimals) AS royalty_fee_amount,
    pu.price * tfe.price * tff.royalty_rate / power(10, pu.decimals) AS royalty_fee_amount_usd,
    CAST(100 * tff.royalty_rate AS double) AS royalty_fee_percentage,
    tff.royalty_address AS royalty_fee_receive_address,
    CASE
        WHEN tfe.native_eth THEN 'ARETH'
        ELSE pu.symbol
    END AS royalty_fee_currency_symbol,
    tff.bundle_index,
    concat('arbitrum-tofu-v1-', tfe.evt_block_number, tfe.evt_tx_hash, tfe.evt_index, tff.bundle_index) AS unique_trade_id,
    tfe.evt_index
FROM tfe
INNER JOIN tff
    ON
        tfe.evt_tx_hash = tff.call_tx_hash
        AND tfe.evt_block_time = tff.call_block_time
LEFT JOIN {{ source('arbitrum', 'transactions') }}
    ON
        tx.block_time = tfe.evt_block_time
        AND tx.hash = tfe.evt_tx_hash
        {% if not is_incremental() %}
                       AND tx.block_time >= '{{ project_start_date }}'
                       {% endif %}
        {% if is_incremental() %}
            AND tx.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN {{ ref('tokens_nft') }}
    ON
        tff.token = nft.contract_address
        AND nft.blockchain = 'arbitrum'
LEFT JOIN {{ source('prices', 'usd') }}
    ON
        pu.blockchain = 'arbitrum'
        AND pu.minute = date_trunc('minute', tfe.evt_block_time)
        AND pu.contract_address = tfe.currency
        {% if not is_incremental() %}
                       AND pu.minute >= '{{ project_start_date }}'
                       {% endif %}
        {% if is_incremental() %}
            AND pu.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN {{ ref('nft_aggregators') }}
    ON
        agg.contract_address = tx.`to`
        AND agg.blockchain = 'arbitrum'
