{{ config(
        alias='trades',
        materialized='incremental',
        partition_by = ['block_date'],
        unique_key = ['tx_hash', 'order_uid', 'evt_index'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith", "gentrexha"]\') }}'
    )
}}

-- Find the PoC Query here: https://dune.com/queries/1719733
WITH
-- First subquery joins buy and sell token prices from prices.usd
-- Also deducts fee from sell amount
trades_with_prices AS (
    SELECT
        try_cast(date_trunc('day', evt_block_time) AS date) AS block_date,
        evt_block_time AS block_time,
        evt_tx_hash AS tx_hash,
        evt_index,
        settlement.contract_address AS project_contract_address,
        owner AS trader,
        orderuid AS order_uid,
        selltoken AS sell_token,
        buytoken AS buy_token,
        (sellamount - feeamount) AS sell_amount,
        buyamount AS buy_amount,
        feeamount AS fee_amount,
        ps.price AS sell_price,
        pb.price AS `buy_price`
    FROM {{ source('gnosis_protocol_v2_gnosis', 'GPv2Settlement_evt_Trade') }}
    LEFT OUTER JOIN {{ source('prices', 'usd') }}
        ON
            selltoken = ps.contract_address
            AND ps.minute = date_trunc('minute', evt_block_time)
            AND ps.blockchain = 'gnosis'
            {% if is_incremental() %}
                AND ps.minute >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    LEFT OUTER JOIN {{ source('prices', 'usd') }}
        ON pb.contract_address = (
            CASE
                WHEN buytoken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                    THEN '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d'
                ELSE buytoken
            END
        )
        AND pb.minute = date_trunc('minute', evt_block_time)
        AND pb.blockchain = 'gnosis'
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
            WHEN buy_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'xDAI'
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
    LEFT OUTER JOIN {{ ref('tokens_gnosis_erc20') }}
        ON ts.contract_address = sell_token
    LEFT OUTER JOIN {{ ref('tokens_gnosis_erc20') }}
        ON
            tb.contract_address
            = (CASE
                WHEN buy_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                    THEN '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d'
                ELSE buy_token
            END)
),

-- This, independent, aggregation defines a mapping of order_uid and trade
-- TODO - create a view for the following block mapping uid to app_data
order_ids AS (
    SELECT
        evt_tx_hash,
        collect_list(orderuid) AS `order_ids`
    FROM (
        SELECT
            orderuid,
            evt_tx_hash,
            evt_index
        FROM {{ source('gnosis_protocol_v2_gnosis', 'GPv2Settlement_evt_Trade') }}
        {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
             {% endif %}
                     sort by evt_index
    ) AS `_`
    GROUP BY evt_tx_hash
),

exploded_order_ids AS (
    SELECT
        evt_tx_hash,
        posexplode(order_ids)
    FROM order_ids
),

reduced_order_ids AS (
    SELECT
        col AS order_id,
        -- This is a dirty hack!
        collect_list(evt_tx_hash)[0] AS evt_tx_hash,
        collect_list(pos)[0] AS `pos`
    FROM exploded_order_ids
    GROUP BY order_id
),

trade_data AS (
    SELECT
        call_tx_hash,
        posexplode(trades)
    FROM {{ source('gnosis_protocol_v2_gnosis', 'GPv2Settlement_call_settle') }}
    WHERE
        call_success = true
        {% if is_incremental() %}
            AND call_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
),

uid_to_app_id AS (
    SELECT
        order_id AS uid,
        get_json_object(trades.col, '$.appData') AS app_data,
        get_json_object(trades.col, '$.receiver') AS receiver,
        get_json_object(trades.col, '$.sellAmount') AS limit_sell_amount,
        get_json_object(trades.col, '$.buyAmount') AS limit_buy_amount,
        get_json_object(trades.col, '$.validTo') AS valid_to,
        get_json_object(trades.col, '$.flags') AS `flags`
    FROM reduced_order_ids
    INNER JOIN trade_data
        ON
            evt_tx_hash = call_tx_hash
            AND order_ids.pos = trades.pos
),

valued_trades AS (
    SELECT
        block_date,
        block_time,
        tx_hash,
        evt_index,
        CAST(ARRAY() as array<bigint>) AS trace_address,
        project_contract_address,
        order_uid,
        trader,
        sell_token_address,
        sell_token,
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
            -- Note that this formulation is subject to some precision error in a few irregular cases:
            -- E.g. In this transaction 0x84d57d1d57e01dd34091c763765ddda6ff713ad67840f39735f0bf0cced11f02
            -- buy_price * units_bought * fee / units_sold
            -- 1.001076 * 0.005 * 0.0010148996324193 / 3e-18 = 1693319440706.3
            -- So, if sell_price had been null here (thankfully it is not), we would have a vastly inaccurate fee valuation
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
        flags
    FROM trades_with_token_units
    INNER JOIN uid_to_app_id
        ON uid = order_uid
)

SELECT
    *,
    -- Relative surplus (in %) is the difference between limit price and executed price AS `a` ratio of the limit price.
    -- Absolute surplus (in USD) is relative surplus multiplied with the value of the trade
    usd_value * (((limit_sell_amount / limit_buy_amount) - (atoms_sold / atoms_bought)) / (limit_sell_amount / limit_buy_amount)) AS `surplus_usd`
FROM valued_trades
