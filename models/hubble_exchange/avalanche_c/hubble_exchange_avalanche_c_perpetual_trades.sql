{{ config(
    alias = 'perpetual_trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "hubble_exchange",
                                \'["Henrystats"]\') }}'
    )
}}

{% set project_start_date = '2022-08-09' %}

WITH

perp_events AS (
    SELECT
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        CASE WHEN (baseasset * 1) >= 0 THEN 'long' ELSE 'short' END AS trade_type,       -- negative baseAsset is for short and positive is for long
        'AVAX' AS virtual_asset,    -- only AVAX can currently be traded on hubble exchange
        '' AS underlying_asset, -- there's no way to track the underlying asset AS `traders` need to deposit into their margin account before they're able to trade which is tracked in a seperate event not tied to the margin positions opened.
        quoteasset / 1E6 AS volume_usd,
        CAST(NULL AS double) AS fee_usd,          -- no event to track fees
        CAST(NULL AS double) AS margin_usd,       -- no event to track margin
        CAST(quoteasset AS double) AS volume_raw,
        trader,
        contract_address AS market_address,
        evt_index,
        evt_tx_hash AS `tx_hash`
    FROM
        {{ source('hubble_exchange_avalanche_c', 'ClearingHouse_evt_PositionModified') }}
    {% if not is_incremental() %}
    WHERE evt_block_time >= '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

trade_data AS (
    -- close position calls 
    SELECT
        call_block_number AS block_number,
        call_tx_hash AS tx_hash,
        'close' AS `trade_data`
    FROM
        {{ source('hubble_exchange_avalanche_c', 'ClearingHouse_call_closePosition') }}
    WHERE
        call_success = TRUE
        {% if not is_incremental() %}
    AND call_block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND call_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}

    UNION ALL

    -- open position calls 
    SELECT
        call_block_number as block_number,
        call_tx_hash as tx_hash,
        'open' AS `trade_data`
    FROM 
    {{ source('hubble_exchange_avalanche_c', 'ClearingHouse_call_openPosition') }}
    WHERE call_success = true 
    {% if not is_incremental() %}
    AND call_block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION ALL

    -- liquidate position events
    SELECT
        evt_block_number as block_number,
        evt_tx_hash as tx_hash,
        'liquidate' AS `trade_data`
    FROM 
    {{ source('hubble_exchange_avalanche_c', 'ClearingHouse_evt_PositionLiquidated') }}
    WHERE 1 = 1
    {% if not is_incremental() %}
    AND evt_block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)

SELECT
    'avalanche_c' AS blockchain,
    'hubble_exchange' AS project,
    '1' AS version,
    'hubble_exchange' AS frontend,
    date_trunc('day', pe.block_time) AS block_date,
    pe.block_time,
    pe.virtual_asset,
    pe.underlying_asset,
    'AVAX' AS market,
    pe.market_address,
    pe.volume_usd,
    pe.fee_usd,
    pe.margin_usd,
    COALESCE(
        td.trade_data || '-' || pe.trade_type, -- using the call/open functions to classify trades
        'adjust' || '-' || pe.trade_type
    ) AS trade,
    pe.trader,
    pe.volume_raw,
    pe.tx_hash,
    txns.to AS tx_to,
    txns.from AS tx_from,
    pe.evt_index
FROM
    perp_events
INNER JOIN
    {{ source('avalanche_c', 'transactions') }}
    ON
        pe.tx_hash = txns.hash
        AND pe.block_number = txns.block_number
        {% if not is_incremental() %}
    AND txns.block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND txns.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN
    trade_data
    ON
        pe.block_number = td.block_number
        AND pe.tx_hash = td.tx_hash
