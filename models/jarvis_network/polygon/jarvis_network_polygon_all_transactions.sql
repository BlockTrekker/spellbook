{{
  config(
    alias='all_transactions',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'evt_tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["polygon"]\',
                                      "project",
                                      "jarvis_network",
                                      \'["0xroll"]\') }}')
}}

{% set project_start_date = '2021-08-16' %}


SELECT
    'polygon' AS blockchain,
    evt_block_time,
    try_cast(date_trunc('DAY', evt_block_time) AS date) AS block_date,
    action,
    user,
    recipient,
    jfiat_token_symbol,
    jfiat_token_amount,
    collateral_symbol,
    collateral_token_amount,
    net_collateral_amount,
    fee_amount,
    collateral_token_amount_usd
        AS net_collateral_amount_usd,
    fee_amount_usd,
    evt_tx_hash,
    evt_index
FROM
    (
        SELECT
            evt_block_time,
            action,
            user,
            recipient,
            am.jfiat_symbol AS jfiat_token_symbol,
            jfiat_token_amount / POWER(10, am.decimals) AS jfiat_token_amount,
            jfiat_collateral_symbol AS collateral_symbol,
            collateral_token_amount / POWER(10, cm.decimals) AS collateral_token_amount,
            net_collateral_amount / POWER(10, cm.decimals) AS net_collateral_amount,
            fee_amount / POWER(10, cm.decimals) AS fee_amount,
            collateral_token_amount / POWER(10, cm.decimals) * price AS collateral_token_amount_usd,
            net_collateral_amount / POWER(10, cm.decimals) * price AS net_collateral_amount_usd,
            fee_amount / POWER(10, cm.decimals) * price AS fee_amount_usd,
            evt_tx_hash,
            evt_index
        FROM
            (
                SELECT
                    evt_block_time,
                    'Mint' AS action,
                    contract_address,
                    user,
                    recipient,
                    mintvalues:numTokens                                  AS jfiat_token_amount,
            mintvalues:totalCollateral                            AS collateral_token_amount,
            mintvalues:exchangeAmount                             AS net_collateral_amount,
            mintvalues:feeAmount                                  AS fee_amount,
            evt_tx_hash,
            evt_index
                FROM {{ source('jarvis_network_polygon','SynthereumMultiLpLiquidityPool_evt_Minted') }}
                {% if is_incremental() %}
                    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
                {% endif %}
                {% if not is_incremental() %}
    WHERE evt_block_time >= '{{ project_start_date }}'
    {% endif %}

                UNION ALL

                SELECT
                    evt_block_time,
                    'Redeem' AS action,
                    contract_address,
                    user AS sender,
                    recipient,
                    redeemvalues:numTokens                                AS jfiat_token_amount,
            redeemvalues:collateralAmount                         AS collateral_token_amount,
            redeemvalues:exchangeAmount                           AS net_collateral_amount,
            redeemvalues:feeAmount                                AS fee_amount,
            evt_tx_hash,
            evt_index
                FROM {{ source('jarvis_network_polygon','SynthereumMultiLpLiquidityPool_evt_Redeemed') }}
                {% if is_incremental() %}
                    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
                {% endif %}
                {% if not is_incremental() %}
    WHERE evt_block_time >= '{{ project_start_date }}'
    {% endif %}

                UNION ALL

                SELECT
                    evt_block_time,
                    'Mint' AS action,
                    contract_address,
                    account AS user,
                    recipient,
                    numtokensreceived AS jfiat_token_amount,
                    collateralsent AS collateral_token_amount,
                    (collateralsent - feepaid) AS net_collateral_amount,
                    feepaid AS fee_amount,
                    evt_tx_hash,
                    evt_index
                FROM {{ source('jarvis_network_polygon','SynthereumPoolOnChainPriceFeed_evt_Mint') }}
                {% if is_incremental() %}
                    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
                {% endif %}
                {% if not is_incremental() %}
    WHERE evt_block_time >= '{{ project_start_date }}'
    {% endif %}

                UNION ALL

                SELECT
                    evt_block_time,
                    'Redeem' AS action,
                    contract_address,
                    account AS user,
                    recipient,
                    numtokenssent AS jfiat_token_amount,
                    collateralreceived + feepaid AS collateral_token_amount,
                    collateralreceived AS net_collateral_amount,
                    feepaid AS fee_amount,
                    evt_tx_hash,
                    evt_index
                FROM {{ source('jarvis_network_polygon','SynthereumPoolOnChainPriceFeed_evt_Redeem') }}
                {% if is_incremental() %}
                    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
                {% endif %}
                {% if not is_incremental() %}
    WHERE evt_block_time >= '{{ project_start_date }}'
    {% endif %}

                UNION ALL

                SELECT
                    evt_block_time,
                    'Exchange' AS action,
                    contract_address,
                    account AS sender,
                    recipient,
                    numtokenssent AS jfiat_token_amount,
                    (feepaid * 1000) AS collateral_token_amount,
                    ((feepaid * 1000) - feepaid) AS net_collateral_amount,
                    feepaid AS fee_amount,
                    evt_tx_hash,
                    evt_index
                FROM {{ source('jarvis_network_polygon','SynthereumPoolOnChainPriceFeed_evt_Exchange') }}
                {% if is_incremental() %}
                    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
                {% endif %}
                {% if not is_incremental() %}
    WHERE evt_block_time >= '{{ project_start_date }}'
    {% endif %}
            ) AS `x`
        INNER JOIN {{ ref('jarvis_network_polygon_jfiat_address_mapping') }}
            ON (x.contract_address = am.jfiat_collateral_pool_address)
        LEFT JOIN {{ ref('jarvis_network_polygon_jfiat_collateral_mapping') }}
            ON (am.jfiat_collateral_pool_address = cm.jfiat_collateral_pool_address)
        LEFT JOIN {{ source('prices', 'usd') }}
            ON
                am.blockchain = pu.blockchain
                AND cm.jfiat_collateral_symbol = pu.symbol
                AND date_trunc('minute', x.evt_block_time) = pu.minute
                {% if not is_incremental() %}
      AND pu.minute >= '{{ project_start_date }}'
      {% endif %}
                {% if is_incremental() %}
                    AND pu.minute >= date_trunc('day', now() - interval '1 week')
                {% endif %}
    ) AS p;
