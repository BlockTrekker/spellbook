{{ config(
    schema = 'tigris_v1_polygon',
    alias = 'events_open_position',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_index', 'evt_tx_hash', 'position_id']
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
        t._id AS position_id,
        t._price / 1e18 AS price,
        t._tradeinfo:margin/1e18 as margin, 
            t._tradeInfo:leverage/1e18 as leverage,
            t._tradeInfo:margin/1e18 * _tradeInfo:leverage/1e18 as volume_usd, 
            t._tradeInfo:marginAsset as margin_asset, 
            ta.pair, 
            t._tradeInfo:direction as direction, 
            t._tradeInfo:referral as referral, 
            t._trader AS `trader`
    FROM
        {{ source('tigristrade_polygon', 'Tradingv1_evt_PositionOpened') }}
    INNER JOIN
        pairs
        ON t._tradeinfo:asset = ta.asset_id 
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
        t._id AS position_id,
        t._price / 1e18 AS price,
        t._tradeinfo:margin/1e18 as margin, 
            t._tradeInfo:leverage/1e18 as leverage,
            t._tradeInfo:margin/1e18 * _tradeInfo:leverage/1e18 as volume_usd, 
            t._tradeInfo:marginAsset as margin_asset, 
            ta.pair, 
            t._tradeInfo:direction as direction, 
            t._tradeInfo:referral as referral, 
            t._trader AS `trader`
    FROM
        {{ source('tigristrade_polygon', 'TradingV2_evt_PositionOpened') }}
    INNER JOIN
        pairs
        ON t._tradeinfo:asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),

open_positions_v3 AS (
    SELECT
        date_trunc('day', t.evt_block_time) AS day,
        t.evt_block_time,
        t.evt_index,
        t.evt_tx_hash,
        t._id AS position_id,
        t._price / 1e18 AS price,
        t._tradeinfo:margin/1e18 as margin, 
            t._tradeInfo:leverage/1e18 as leverage,
            t._tradeInfo:margin/1e18 * _tradeInfo:leverage/1e18 as volume_usd, 
            t._tradeInfo:marginAsset as margin_asset, 
            ta.pair, 
            t._tradeInfo:direction as direction, 
            t._tradeInfo:referral as referral, 
            t._trader AS `trader`
    FROM
        {{ source('tigristrade_polygon', 'TradingV3_evt_PositionOpened') }}
    INNER JOIN
        pairs
        ON t._tradeinfo:asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),

open_positions_v4 AS (
    SELECT
        date_trunc('day', t.evt_block_time) AS day,
        t.evt_block_time,
        t.evt_index,
        t.evt_tx_hash,
        t._id AS position_id,
        t._price / 1e18 AS price,
        t._tradeinfo:margin/1e18 as margin, 
            t._tradeInfo:leverage/1e18 as leverage,
            t._tradeInfo:margin/1e18 * _tradeInfo:leverage/1e18 as volume_usd, 
            t._tradeInfo:marginAsset as margin_asset, 
            ta.pair, 
            t._tradeInfo:direction as direction, 
            t._tradeInfo:referral as referral, 
            t._trader AS `trader`
    FROM
        {{ source('tigristrade_polygon', 'TradingV4_evt_PositionOpened') }}
    INNER JOIN
        pairs
        ON t._tradeinfo:asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),

open_positions_v5 AS (
    SELECT
        date_trunc('day', t.evt_block_time) AS day,
        t.evt_block_time,
        t.evt_index,
        t.evt_tx_hash,
        t._id AS position_id,
        t._price / 1e18 AS price,
        t._tradeinfo:margin/1e18 as margin, 
            t._tradeInfo:leverage/1e18 as leverage,
            t._tradeInfo:margin/1e18 * _tradeInfo:leverage/1e18 as volume_usd, 
            t._tradeInfo:marginAsset as margin_asset, 
            ta.pair, 
            t._tradeInfo:direction as direction, 
            t._tradeInfo:referral as referral, 
            t._trader AS `trader`
    FROM
        {{ source('tigristrade_polygon', 'TradingV5_evt_PositionOpened') }}
    INNER JOIN
        pairs
        ON t._tradeinfo:asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),

open_positions_v6 AS (
    SELECT
        date_trunc('day', t.evt_block_time) AS day,
        t.evt_block_time,
        t.evt_index,
        t.evt_tx_hash,
        t._id AS position_id,
        t._price / 1e18 AS price,
        t._tradeinfo:margin/1e18 as margin, 
            t._tradeInfo:leverage/1e18 as leverage,
            t._tradeInfo:margin/1e18 * _tradeInfo:leverage/1e18 as volume_usd, 
            t._tradeInfo:marginAsset as margin_asset, 
            ta.pair, 
            t._tradeInfo:direction as direction, 
            t._tradeInfo:referral as referral, 
            t._trader AS `trader`
    FROM
        {{ source('tigristrade_polygon', 'TradingV6_evt_PositionOpened') }}
    INNER JOIN
        pairs
        ON t._tradeinfo:asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),

open_positions_v7 AS (
    SELECT
        date_trunc('day', t.evt_block_time) AS day,
        t.evt_block_time,
        t.evt_index,
        t.evt_tx_hash,
        t._id AS position_id,
        t._price / 1e18 AS price,
        t._tradeinfo:margin/1e18 as margin, 
            t._tradeInfo:leverage/1e18 as leverage,
            t._tradeInfo:margin/1e18 * _tradeInfo:leverage/1e18 as volume_usd, 
            t._tradeInfo:marginAsset as margin_asset, 
            ta.pair, 
            t._tradeInfo:direction as direction, 
            t._tradeInfo:referral as referral, 
            t._trader AS `trader`
    FROM
        {{ source('tigristrade_polygon', 'TradingV7_evt_PositionOpened') }}
    INNER JOIN
        pairs
        ON t._tradeinfo:asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),

open_positions_v8 AS (
    SELECT
        date_trunc('day', t.evt_block_time) AS day,
        t.evt_block_time,
        t.evt_index,
        t.evt_tx_hash,
        t._id AS position_id,
        t._price / 1e18 AS price,
        t._tradeinfo:margin/1e18 as margin, 
            t._tradeInfo:leverage/1e18 as leverage,
            t._tradeInfo:margin/1e18 * _tradeInfo:leverage/1e18 as volume_usd, 
            t._tradeInfo:marginAsset as margin_asset, 
            ta.pair, 
            t._tradeInfo:direction as direction, 
            t._tradeInfo:referral as referral, 
            t._trader AS `trader`
    FROM
        {{ source('tigristrade_polygon', 'TradingV8_evt_PositionOpened') }}
    INNER JOIN
        pairs
        ON t._tradeinfo:asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)

SELECT
    *,
    'v1.1' AS `version`
FROM open_positions_v1

UNION ALL

SELECT
    *,
    'v1.2' AS `version`
FROM open_positions_v2

UNION ALL

SELECT
    *,
    'v1.3' AS `version`
FROM open_positions_v3

UNION ALL

SELECT
    *,
    'v1.4' AS `version`
FROM open_positions_v4

UNION ALL

SELECT
    *,
    'v1.5' AS `version`
FROM open_positions_v5

UNION ALL

SELECT
    *,
    'v1.6' AS `version`
FROM open_positions_v6

UNION ALL

SELECT
    *,
    'v1.7' AS `version`
FROM open_positions_v7

UNION ALL

SELECT
    *,
    'v1.8' AS `version`
FROM open_positions_v8;
