{{
    config(
        schema='balancer_v2_ethereum',
        alias='transfers_bpt',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'evt_tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["stefenon"]\') }}'
    )
}}

{% set event_signature = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' %}
{% set project_start_date = '2021-04-20' %}

WITH registered_pools AS (
    SELECT DISTINCT pooladdress AS pool_address
    FROM
        {{ source('balancer_v2_ethereum', 'Vault_evt_PoolRegistered') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '1 week')
    {% endif %}
)

SELECT DISTINCT * FROM (
    SELECT
        logs.contract_address,
        logs.tx_hash AS evt_tx_hash,
        logs.index AS evt_index,
        logs.block_time AS evt_block_time,
        logs.block_number AS evt_block_number,
        SAFE_CAST(date_trunc('DAY', logs.block_time) AS DATE) AS block_date,
        TO_HEX(SUBSTRING(logs.topics[2], 27, 40)) AS `from`,
        TO_HEX(SUBSTRING(logs.topics[3], 27, 40)) AS `to`,
        bytea2numeric(SUBSTRING(logs.data, 32, 64)) AS value
    FROM {{ source('ethereum', 'logs') }} AS logs
    INNER JOIN registered_pools AS p ON p.pool_address = logs.contract_address
    WHERE
        logs.topic1 = '{{ event_signature }}'
        {% if not is_incremental() %}
        AND logs.block_time >= '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental() %}
            AND logs.block_time >= DATE_TRUNC('day', NOW() - INTERVAL 1 WEEK)
        {% endif %}
) AS transfers
