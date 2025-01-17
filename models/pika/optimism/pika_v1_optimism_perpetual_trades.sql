{{ config(
	schema = 'pika_v1_optimism',
	alias ='perpetual_trades',
	partition_by = ['block_date'],
	materialized = 'incremental',
	file_format = 'delta',
	incremental_strategy = 'merge',
	unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "pika",
                                \'["msilb7", "drethereum", "rplust"]\') }}'
	)
}}

{% set project_start_date = '2021-11-22' %}

WITH positions AS (
    SELECT
        positionid,
        user AS user,
        productid,
        CAST(islong AS VARCHAR(5)) AS islong,
        price,
        oracleprice,
        margin,
        leverage,
        0 AS fee,
        contract_address,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number,
        '1' AS version
    FROM {{ source('pika_perp_optimism', 'PikaPerpV2_evt_NewPosition') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= DATE_TRUNC('DAY', NOW() - INTERVAL '1 WEEK')
    {% endif %}

    UNION ALL
    --closing positions
    SELECT
        positionid,
        user,
        productid,
        'close' AS action,
        price,
        entryprice,
        margin,
        leverage,
        0 AS fee,
        contract_address,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number,
        '1' AS version
    FROM {{ source('pika_perp_optimism', 'PikaPerpV2_evt_ClosePosition') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= DATE_TRUNC('DAY', NOW() - INTERVAL '1 WEEK')
    {% endif %}
),

perps AS (
    SELECT
        evt_block_time AS block_time,
        evt_block_number AS block_number,

        CASE
            WHEN productid = 1 OR productid = 16 THEN 'ETH'
            WHEN productid = 2 OR productid = 17 THEN 'BTC'
            WHEN productid = 3 OR productid = 18 THEN 'LINK'
            WHEN productid = 4 OR productid = 19 THEN 'SNX'
            WHEN productid = 5 OR productid = 20 THEN 'SOL'
            WHEN productid = 6 OR productid = 21 THEN 'AVAX'
            WHEN productid = 7 OR productid = 22 THEN 'MATIC'
            WHEN productid = 8 THEN 'LUNA'
            WHEN productid = 9 OR productid = 23 THEN 'AAVE'
            WHEN productid = 10 OR productid = 24 THEN 'APE'
            WHEN productid = 11 OR productid = 25 THEN 'AXS'
            WHEN productid = 12 OR productid = 26 THEN 'UNI'
            ELSE CONCAT('product_id_', productid)
        END AS virtual_asset,

        CASE
            WHEN productid = 1 OR productid = 16 THEN 'ETH'
            WHEN productid = 2 OR productid = 17 THEN 'BTC'
            WHEN productid = 3 OR productid = 18 THEN 'LINK'
            WHEN productid = 4 OR productid = 19 THEN 'SNX'
            WHEN productid = 5 OR productid = 20 THEN 'SOL'
            WHEN productid = 6 OR productid = 21 THEN 'AVAX'
            WHEN productid = 7 OR productid = 22 THEN 'MATIC'
            WHEN productid = 8 THEN 'LUNA'
            WHEN productid = 9 OR productid = 23 THEN 'AAVE'
            WHEN productid = 10 OR productid = 24 THEN 'APE'
            WHEN productid = 11 OR productid = 25 THEN 'AXS'
            WHEN productid = 12 OR productid = 26 THEN 'UNI'
            ELSE CONCAT('product_id_', productid)
        END AS underlying_asset,

        CASE
            WHEN productid = 1 OR productid = 16 THEN 'ETH-USD'
            WHEN productid = 2 OR productid = 17 THEN 'BTC-USD'
            WHEN productid = 3 OR productid = 18 THEN 'LINK-USD'
            WHEN productid = 4 OR productid = 19 THEN 'SNX-USD'
            WHEN productid = 5 OR productid = 20 THEN 'SOL-USD'
            WHEN productid = 6 OR productid = 21 THEN 'AVAX-USD'
            WHEN productid = 7 OR productid = 22 THEN 'MATIC-USD'
            WHEN productid = 8 THEN 'LUNA-USD'
            WHEN productid = 9 OR productid = 23 THEN 'AAVE-USD'
            WHEN productid = 10 OR productid = 24 THEN 'APE-USD'
            WHEN productid = 11 OR productid = 25 THEN 'AXS-USD'
            WHEN productid = 12 OR productid = 26 THEN 'UNI-USD'
            ELSE CONCAT('product_id_', productid)
        END AS market,

        contract_address AS market_address,
        (margin / 1e8) * (leverage / 1e8) AS volume_usd,
        fee / 1e8 AS fee_usd,
        margin / 1e8 AS margin_usd,

        CASE
            WHEN islong = 'true' THEN 'long'
            WHEN islong = 'false' THEN 'short'
            ELSE islong
        END AS trade,

        'Pika' AS project,
        version,
        'Pika' AS frontend,
        user AS trader,
        margin * leverage AS volume_raw,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM positions
)

SELECT
    'optimism' AS blockchain,
    TRY_CAST(date_trunc('DAY', perps.block_time) AS DATE) AS block_date,
    perps.block_time,
    perps.virtual_asset,
    perps.underlying_asset,
    perps.market,
    perps.market_address,
    perps.volume_usd,
    perps.fee_usd,
    perps.margin_usd,
    perps.trade,
    perps.project,
    perps.version,
    perps.frontend,
    perps.trader,
    perps.volume_raw,
    perps.tx_hash,
    tx.from AS tx_from,
    tx.to AS tx_to,
    perps.evt_index
FROM perps
INNER JOIN {{ source('optimism', 'transactions') }} AS tx
    ON
        perps.tx_hash = tx.hash
        AND perps.block_number = tx.block_number
        {% if not is_incremental() %}
	AND tx.block_time >= '{{ project_start_date }}'
	{% endif %}
        {% if is_incremental() %}
            AND tx.block_time >= DATE_TRUNC('DAY', NOW() - INTERVAL '1 WEEK')
        {% endif %}
