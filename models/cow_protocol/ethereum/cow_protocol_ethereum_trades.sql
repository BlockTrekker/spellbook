{{ config(
        alias='trades',
        materialized='incremental',
        partition_by = ['block_date'],
        unique_key = ['tx_hash', 'order_uid', 'evt_index'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith", "gentrexha"]\') }}'
    )
}}

-- Find the PoC Query here: https://dune.com/queries/2360196
WITH
-- First subquery joins buy and sell token prices from prices.usd
-- Also deducts fee from sell amount
trades_with_prices AS (
    SELECT
        try_cast(date_trunc('day', evt_block_time) AS date) AS block_date,
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        evt_tx_hash AS tx_hash,
        evt_index,
        trade.contract_address AS project_contract_address,
        owner AS trader,
        orderuid AS order_uid,
        selltoken AS sell_token,
        buytoken AS buy_token,
        (sellamount - feeamount) AS sell_amount,
        buyamount AS buy_amount,
        feeamount AS fee_amount,
        ps.price AS sell_price,
        pb.price AS `buy_price`
    FROM {{ source('gnosis_protocol_v2_ethereum', 'GPv2Settlement_evt_Trade') }}
    LEFT OUTER JOIN {{ source('prices', 'usd') }}
        ON
            selltoken = ps.contract_address
            AND ps.minute = date_trunc('minute', evt_block_time)
            AND ps.blockchain = 'ethereum'
            {% if is_incremental() %}
                AND ps.minute >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    LEFT OUTER JOIN {{ source('prices', 'usd') }}
        ON pb.contract_address = (
            CASE
                WHEN buytoken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                    THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE buytoken
            END
        )
        AND pb.minute = date_trunc('minute', evt_block_time)
        AND pb.blockchain = 'ethereum'
        {% if is_incremental() %}
            AND pb.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

-- Second subquery gets token symbol and decimals from tokens.erc20 (to display units bought and sold)
trades_with_token_units AS (
    SELECT
        block_date,
        block_number,
        block_time,
        tx_hash,
        evt_index,
        project_contract_address,
        order_uid,
        trader,
        sell_token AS sell_token_address,
        (COALESCE(ts.symbol, sell_token)) AS sell_token,
        buy_token AS buy_token_address,
        (CASE
            WHEN tb.symbol IS NULL THEN buy_token
            WHEN buy_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'ETH'
            ELSE tb.symbol
        END) AS buy_token,
        sell_amount / pow(10, ts.decimals) AS units_sold,
        sell_amount AS atoms_sold,
        buy_amount / pow(10, tb.decimals) AS units_bought,
        buy_amount AS atoms_bought,
        -- We use sell value when possible and buy value when not
        fee_amount / pow(10, ts.decimals) AS fee,
        fee_amount AS fee_atoms,
        sell_price,
        buy_price
    FROM trades_with_prices
    LEFT OUTER JOIN {{ ref('tokens_ethereum_erc20') }}
        ON ts.contract_address = sell_token
    LEFT OUTER JOIN {{ ref('tokens_ethereum_erc20') }}
        ON
            tb.contract_address
            = (CASE
                WHEN buy_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                    THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE buy_token
            END)
),

-- This, independent, aggregation defines a mapping of order_uid and trade
sorted_orders AS (
    SELECT
        evt_tx_hash,
        evt_block_number,
        collect_list(orderuid) AS `order_ids`
    FROM (
        SELECT
            evt_tx_hash,
            evt_block_number,
            orderuid
        FROM gnosis_protocol_v2_ethereum.gpv2settlement_evt_trade
        {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
        distribute by
            evt_tx_hash, evt_block_number
        sort by
            evt_index
    )
    GROUP BY evt_tx_hash, evt_block_number
),

orders_and_trades AS (
    SELECT
        evt_tx_hash,
        trades,
        order_ids
    FROM sorted_orders
    INNER JOIN {{ source('gnosis_protocol_v2_ethereum', 'GPv2Settlement_call_settle') }}
        ON
            evt_block_number = call_block_number
            AND evt_tx_hash = call_tx_hash
-- this is implied by the inner join
--      and call_success = true
),

-- Validate Uid <--> app_data mapping here: https://dune.com/queries/1759039?d=1
uid_to_app_id AS (
    SELECT DISTINCT
        uid,
        get_json_object(trade, '$.appData') AS app_data,
        get_json_object(trade, '$.receiver') AS receiver,
        get_json_object(trade, '$.sellAmount') AS limit_sell_amount,
        get_json_object(trade, '$.buyAmount') AS limit_buy_amount,
        get_json_object(trade, '$.validTo') AS valid_to,
        get_json_object(trade, '$.flags') AS `flags`
    FROM orders_and_trades
    lateral view posexplode(order_ids) o as i, uid
        lateral view posexplode(trades) t as j, trade
    where i = j
),

eth_flow_senders AS (
    SELECT
        sender,
        concat(output_orderhash, substring(event.contract_address, 3, 40), 'ffffffff') AS `order_uid`
    FROM {{ source('cow_protocol_ethereum', 'CoWSwapEthFlow_evt_OrderPlacement') }}
    INNER JOIN {{ source('cow_protocol_ethereum', 'CoWSwapEthFlow_call_createOrder') }}
        ON
            call_block_number = evt_block_number
            AND call_tx_hash = evt_tx_hash
            AND call_success = true
    {% if is_incremental() %}
        WHERE
            evt_block_time >= date_trunc('day', now() - interval '1 week')
            AND call_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),


valued_trades AS (
    SELECT
        block_date,
        block_number,
        block_time,
        tx_hash,
        evt_index,
        CAST(ARRAY() as array<bigint>) AS trace_address,
        project_contract_address,
        trades.order_uid,
        -- ETH Flow orders have trader = sender of orderCreation.
        COALESCE(sender, trader) AS trader,
        sell_token_address,
        CASE WHEN sender IS NOT NULL THEN 'ETH' ELSE sell_token END AS sell_token,
        buy_token_address,
        buy_token,
        CASE
            WHEN lower(buy_token) > lower(sell_token) THEN concat(sell_token, '-', buy_token)
            ELSE concat(buy_token, '-', sell_token)
        END AS token_pair,
        units_sold,
        CAST(atoms_sold AS decimal(38, 0)) AS atoms_sold,
        units_bought,
        CAST(atoms_bought AS decimal(38, 0)) AS atoms_bought,
        (CASE
            WHEN sell_price IS NOT NULL
                THEN
                    -- Choose the larger of two prices when both not null.
                    CASE
                        WHEN buy_price IS NOT NULL AND buy_price * units_bought > sell_price * units_sold
                            THEN buy_price * units_bought
                        ELSE sell_price * units_sold
                    END
            WHEN sell_price IS NULL AND buy_price IS NOT NULL THEN buy_price * units_bought
            ELSE null::numeric
        END) AS usd_value,
        buy_price,
        buy_price * units_bought AS buy_value_usd,
        sell_price,
        sell_price * units_sold AS sell_value_usd,
        fee,
        fee_atoms,
        (CASE
            WHEN sell_price IS NOT NULL THEN sell_price * fee
            WHEN buy_price IS NOT NULL THEN buy_price * units_bought * fee / units_sold
            ELSE null::numeric
        END) AS fee_usd,
        app_data,
        CASE
            WHEN receiver = '0x0000000000000000000000000000000000000000'
                THEN trader
            ELSE receiver
        END AS receiver,
        limit_sell_amount,
        limit_buy_amount,
        valid_to,
        flags,
        CASE WHEN (flags % 2) = 0 THEN 'SELL' ELSE 'BUY' END AS order_type,
        cast(cast(flags AS int) & 2 AS boolean) AS `partial_fill`
    FROM trades_with_token_units
    INNER JOIN uid_to_app_id
        ON uid = order_uid
    LEFT OUTER JOIN eth_flow_senders
        ON trades.order_uid = efs.order_uid
)

SELECT
    *,
    -- Relative surplus (in %) is the difference between limit price and executed price AS `a` ratio of the limit price.
    -- Absolute surplus (in USD) is relative surplus multiplied with the value of the trade
    usd_value * (((limit_sell_amount / limit_buy_amount) - (atoms_sold / atoms_bought)) / (limit_sell_amount / limit_buy_amount)) AS `surplus_usd`
FROM valued_trades
