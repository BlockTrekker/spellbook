{{ config(
    alias = 'transactions_polygon_eth',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'dao_creator_tool', 'dao', 'dao_wallet_address', 'tx_hash', 'tx_index', 'tx_type', 'trace_address', 'address_interacted_with', 'asset_contract_address', 'value']
    )
}}

{% set transactions_start_date = '2021-09-01' %}

WITH

dao_tmp AS (
    SELECT
        blockchain,
        dao_creator_tool,
        dao,
        dao_wallet_address
    FROM
        {{ ref('dao_addresses_polygon') }}
    WHERE dao_wallet_address IS NOT NULL
),

transactions AS (
    SELECT
        block_time,
        tx_hash,
        LOWER('0x0000000000000000000000000000000000001010') AS token,
        value AS value,
        to as dao_wallet_address, 
            'tx_in' as tx_type, 
            tx_index,
            COALESCE(from, '') as address_interacted_with,
            trace_address
    FROM
        {{ source('polygon', 'traces') }}
    {% if not is_incremental() %}
        WHERE block_time >= '{{ transactions_start_date }}'
        {% endif %}
    {% if is_incremental() %}
        WHERE block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
        AND to IN (SELECT dao_wallet_address FROM dao_tmp)
        AND (LOWER(call_type) NOT IN ('delegatecall', 'callcode', 'staticcall') or call_type IS NULL)
        AND success = true 
        AND CAST(value as decimal(38,0)) != 0

    UNION ALL

    SELECT
        block_time,
        tx_hash,
        LOWER('0x0000000000000000000000000000000000001010') AS token,
        value AS `value` 
            from as dao_wallet_address, 
            'tx_out' as tx_type,
            tx_index,
            COALESCE(to, '') as address_interacted_with,
            trace_address
        FROM 
        {{ source('polygon', 'traces') }}
        {% if not is_incremental() %}
        WHERE block_time >= '{{transactions_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND from IN (SELECT dao_wallet_address FROM dao_tmp)
        AND (LOWER(call_type) NOT IN ('delegatecall', 'callcode', 'staticcall') or call_type IS NULL)
        AND success = true 
        AND CAST(value as decimal(38,0)) != 0
)

SELECT
    dt.blockchain,
    dt.dao_creator_tool,
    dt.dao,
    dt.dao_wallet_address,
    TRY_CAST(date_trunc('day', t.block_time) AS date) AS block_date,
    t.block_time,
    t.tx_type,
    t.token AS asset_contract_address,
    'MATIC' AS asset,
    CAST(t.value AS decimal(38, 0)) AS raw_value,
    t.value / POW(10, 18) AS value,
    t.value / POW(10, 18) * COALESCE(p.price, dp.median_price) AS usd_value,
    t.tx_hash,
    t.tx_index,
    t.address_interacted_with,
    t.trace_address
FROM
    transactions
INNER JOIN
    dao_tmp
    ON t.dao_wallet_address = dt.dao_wallet_address
LEFT JOIN
    {{ source('prices', 'usd') }}
    ON
        p.minute = date_trunc('minute', t.block_time)
        AND p.symbol = 'MATIC'
        AND p.blockchain = 'polygon'
        {% if not is_incremental() %}
    AND p.minute >= '{{ transactions_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND p.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN
    {{ ref('dex_prices') }}
    ON
        dp.hour = date_trunc('hour', t.block_time)
        AND dp.contract_address = LOWER('0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270')
        AND dp.blockchain = 'polygon'
        AND dp.hour >= '{{ transactions_start_date }}'
        {% if is_incremental() %}
            AND dp.hour >= date_trunc('day', now() - interval '1 week')
        {% endif %}
