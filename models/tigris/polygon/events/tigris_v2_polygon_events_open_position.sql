{{ config(
    schema = 'tigris_v2_polygon',
    alias = 'events_open_position',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id']
    )
}}

WITH

pairs AS (
    SELECT *
    FROM
        {{ ref('tigris_polygon_events_asset_added') }}
),

open_positions_v1 AS (
    SELECT
        date_trunc('day', t.evt_block_time) AS day,
        t.evt_block_time,
        t.evt_index,
        t.evt_tx_hash,
        t.id AS position_id,
        t.price / 1e18 AS price,
        t.tradeinfo:margin/1e18 as margin, 
            t.tradeInfo:leverage/1e18 as leverage,
            t.tradeInfo:margin/1e18 * t.tradeInfo:leverage/1e18 as volume_usd, 
            t.tradeInfo:marginAsset as margin_asset, 
            ta.pair, 
            t.tradeInfo:direction as direction, 
            t.tradeInfo:referral as referral, 
            t.trader AS `trader`
    FROM
        {{ source('tigristrade_v2_polygon', 'Trading_evt_PositionOpened') }}
    INNER JOIN
        pairs
        ON t.tradeinfo:asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),

open_positions_v2 AS (
    SELECT
        date_trunc('day', t.evt_block_time) AS day,
        t.evt_block_time,
        t.evt_index,
        t.evt_tx_hash,
        t.id AS position_id,
        t.price / 1e18 AS price,
        t.tradeinfo:margin/1e18 as margin, 
            t.tradeInfo:leverage/1e18 as leverage,
            t.tradeInfo:margin/1e18 * t.tradeInfo:leverage/1e18 as volume_usd, 
            t.tradeInfo:marginAsset as margin_asset, 
            ta.pair, 
            t.tradeInfo:direction as direction, 
            t.tradeInfo:referral as referral, 
            t.trader AS `trader`
    FROM
        {{ source('tigristrade_v2_polygon', 'TradingV2_evt_PositionOpened') }}
    INNER JOIN
        pairs
        ON t.tradeinfo:asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)

SELECT
    *,
    'v2.1' AS `version`
FROM open_positions_v1

UNION ALL

SELECT
    *,
    'v2.2' AS `version`
FROM open_positions_v2
