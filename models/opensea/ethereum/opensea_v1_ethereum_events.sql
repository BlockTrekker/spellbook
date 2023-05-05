{{ config(
    schema = 'opensea_v1_ethereum',
    alias = 'events',
    materialized = 'table',
    file_format = 'delta',
    partition_by = ['block_date']
    )
}}

{% set ETH_ERC20='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}
{% set ZERO_ADDR='0x0000000000000000000000000000000000000000' %}
{% set SHARED_STOREFRONT='0x495f947276749ce646f68ac8c248420045cb7b5e' %}
{% set OS_WALLET='0x5b3256965e7c3cf26e11fcaf296dfc8807c01073' %}
{% set START_DATE='2018-07-18' %}
{% set END_DATE='2022-08-02' %}

WITH wyvern_call_data AS (
    SELECT
        call_tx_hash AS tx_hash,
        call_block_time AS block_time,
        call_block_number AS block_number,
        addrs[0] AS project_contract_address,
        addrs[1] AS buyer,                           -- maker of buy order
        addrs[8] AS seller,                       -- maker of sell order
        CASE -- we check which side defines the fee_recipient to determine the category
            WHEN addrs[3] != '{{ ZERO_ADDR }}' THEN 'Sell'
            WHEN addrs[10] != '{{ ZERO_ADDR }}' THEN 'Buy'
        END AS trade_category,
        CASE WHEN feemethodssideskindshowtocalls[2] = 0 THEN 'Fixed price' ELSE 'Auction' END AS sale_type,
        CASE -- buyer payment token
            WHEN addrs[6] = '{{ ZERO_ADDR }}' THEN '{{ ETH_ERC20 }}'
            ELSE addrs[6]
        END AS currency_contract,
        (addrs[6] = '{{ ZERO_ADDR }}') AS native_eth,
        CASE -- fee_recipient
            WHEN addrs[3] != '{{ ZERO_ADDR }}' THEN addrs[3]
            WHEN addrs[10] != '{{ ZERO_ADDR }}' THEN addrs[10]
        END AS fee_recipient,
        CASE
            WHEN addrs[4] = '{{ SHARED_STOREFRONT }}' THEN 'Mint'  -- todo: this needs to be verified if correct
            ELSE 'Trade'
        END AS evt_type,
        CASE
            WHEN addrs[3] != '{{ ZERO_ADDR }}'  -- SELL
                THEN (CASE
                    WHEN (uints[0] + uints[1]) / 1e4 < 0.025 -- we assume no marketplace fees then..
                        THEN 0.0
                    ELSE 0.025
                END)
            WHEN addrs[10] != '{{ ZERO_ADDR }}'  -- BUY
                THEN (CASE WHEN (
                    addrs[9] != '{{ ZERO_ADDR }}' --private listing
                    OR (uints[9] + uints[10]) / 1e4 < 0.025
                ) -- we assume no marketplace fees then..
                    THEN 0.0
                ELSE 0.025 END)
        END AS platform_fee,
        CASE
            WHEN addrs[3] != '{{ ZERO_ADDR }}'  -- SELL
                THEN CASE
                    WHEN (uints[0] + uints[1]) / 1e4 < 0.025 -- we assume no marketplace fees then..
                        THEN (uints[0] + uints[1]) / 1e4
                    ELSE (uints[0] + uints[1] - 250) / 1e4
                END
            WHEN addrs[10] != '{{ ZERO_ADDR }}'  -- BUY
                THEN CASE WHEN (
                    addrs[9] != '{{ ZERO_ADDR }}' --private listing
                    OR (uints[9] + uints[10]) / 1e4 < 0.025
                ) -- we assume no marketplace fees then..
                    THEN (uints[9] + uints[10]) / 1e4
                ELSE (uints[9] + uints[10] - 250) / 1e4 END
        END AS royalty_fee,
        CASE
            WHEN addrs[10] != '{{ ZERO_ADDR }}' AND addrs[6] = '{{ ZERO_ADDR }}'
                THEN 1.0 + uints[10] / 1e4      -- on ERC20 BUY: add sell side taker fee (this is not included in the price from the evt) https://etherscan.io/address/0x7be8076f4ea4a4ad08075c2508e481d6c946d12b#code#L838
            ELSE 1.0
        END AS price_correction,
        call_trace_address,
        row_number() OVER (PARTITION BY call_block_number, call_tx_hash ORDER BY call_trace_address ASC) AS `tx_call_order`
    FROM
        {{ source('opensea_ethereum','wyvernexchange_call_atomicmatch_') }}
    WHERE
        1 = 1
        AND (addrs[3] = '{{ OS_WALLET }}' OR addrs[10] = '{{ OS_WALLET }}') -- limit to OpenSea
        AND call_success = true
        AND call_block_time >= '{{ START_DATE }}' AND call_block_time <= '{{ END_DATE }}'
),


-- needed to pull correct prices
order_prices AS (
    SELECT
        evt_block_number AS block_number,
        evt_tx_hash AS tx_hash,
        evt_index AS order_evt_index,
        price,
        row_number() OVER (PARTITION BY evt_block_number, evt_tx_hash ORDER BY evt_index ASC) AS orders_evt_order,
        lag(evt_index) OVER (PARTITION BY evt_block_number, evt_tx_hash ORDER BY evt_index ASC) AS `prev_order_evt_index`
    FROM {{ source('opensea_ethereum','wyvernexchange_evt_ordersmatched') }}
    WHERE evt_block_time >= '{{ START_DATE }}' AND evt_block_time <= '{{ END_DATE }}'

),

-- needed to pull token_id, token_amounts, token_standard and nft_contract_address
nft_transfers AS (
    SELECT
        block_time,
        block_number
        from,
        to,
        contract_address as nft_contract_address,
        token_standard,
        token_id,
        amount,
        evt_index,
        tx_hash
    from {{ ref('nft_ethereum_transfers') }}
    WHERE block_time >= '{{START_DATE}}' AND block_time <= '{{END_DATE}}'
),

-- join call and order data
enhanced_orders AS (
    SELECT
        c.*,
        o.price * c.price_correction AS total_amount_raw,
        o.order_evt_index,
        o.prev_order_evt_index
    FROM wyvern_call_data
    INNER JOIN order_prices -- there should be a 1-to-1 match here
        ON
            c.block_number = o.block_number
            AND c.tx_hash = o.tx_hash
            AND c.tx_call_order = o.orders_evt_order -- in case of multiple calls in 1 tx_hash
),

-- join nft transfers and split trades, we divide the total amount (and fees) proportionally for bulk trades
enhanced_trades AS (
    SELECT
        o.*,
        nft.nft_contract_address,
        nft.token_standard,
        nft.token_id,
        nft.amount AS number_of_items,
        nft.to AS nft_to,
        nft.from AS nft_from,
        total_amount_raw * amount / (sum(nft.amount) OVER (PARTITION BY o.block_number, o.tx_hash, o.order_evt_index)) AS amount_raw,
        CASE
            WHEN count(nft.evt_index) OVER (PARTITION BY o.block_number, o.tx_hash, o.order_evt_index) > 1
                THEN concat('Bundle trade: ', sale_type)
            ELSE concat('Single Item Trade: ', sale_type)
        END AS trade_type,
        nft.evt_index AS `nft_evt_index`
    FROM enhanced_orders
    INNER JOIN nft_transfers
        ON
            o.block_number = nft.block_number
            AND o.tx_hash = nft.tx_hash
            AND ((trade_category = 'Buy' AND nft.from = o.seller) OR (trade_category = 'Sell' AND nft.to = o.buyer))
            AND nft.evt_index <= o.order_evt_index AND (prev_order_evt_index IS NULL OR nft.evt_index > o.prev_order_evt_index)
)


SELECT
    'ethereum' AS blockchain,
    'opensea' AS project,
    'v1' AS version,
    project_contract_address,
    TRY_CAST(date_trunc('DAY', t.block_time) AS date) AS block_date,
    t.block_time,
    t.block_number,
    t.tx_hash,
    t.nft_contract_address,
    t.token_standard,
    nft.name AS collection,
    t.token_id,
    CAST(t.amount_raw AS decimal(38, 0)) AS amount_raw,
    t.amount_raw / power(10, erc20.decimals) AS amount_original,
    t.amount_raw / power(10, erc20.decimals) * p.price AS amount_usd,
    t.trade_category,
    t.trade_type,
    CAST(t.number_of_items AS decimal(38, 0)) AS number_of_items,
    coalesce(t.nft_from, t.seller) AS seller,
    coalesce(t.nft_to, t.buyer) AS buyer,
    t.evt_type,
    CASE WHEN t.native_eth THEN 'ETH' ELSE erc20.symbol END AS currency_symbol,
    t.currency_contract,
    agg.name AS aggregator_name,
    agg.contract_address AS aggregator_address,
    tx.from AS tx_from,
    tx.to AS tx_to,
    -- some complex price calculations, (t.amount_raw/t.price_correction) is the original base price for fees.
    CAST(round((100 * platform_fee), 4) AS double) AS platform_fee_percentage,
    platform_fee * (t.amount_raw / t.price_correction) AS platform_fee_amount_raw,
    platform_fee * (t.amount_raw / t.price_correction) / power(10, erc20.decimals) AS platform_fee_amount,
    platform_fee * (t.amount_raw / t.price_correction) / power(10, erc20.decimals) * p.price AS platform_fee_amount_usd,
    CAST(round((100 * royalty_fee), 4) AS double) AS royalty_fee_percentage,
    royalty_fee * (t.amount_raw / t.price_correction) AS royalty_fee_amount_raw,
    royalty_fee * (t.amount_raw / t.price_correction) / power(10, erc20.decimals) AS royalty_fee_amount,
    royalty_fee * (t.amount_raw / t.price_correction) / power(10, erc20.decimals) * p.price AS royalty_fee_amount_usd,
    t.fee_recipient AS royalty_fee_receive_address,
    CASE WHEN t.native_eth THEN 'ETH' ELSE erc20.symbol END AS royalty_fee_currency_symbol,
    'wyvern-opensea' || '-' || t.tx_hash || '-' || t.order_evt_index || '-' || t.nft_evt_index || '-' || token_id AS `unique_trade_id`
FROM enhanced_trades
INNER JOIN {{ source('ethereum','transactions') }}
    ON
        t.block_number = tx.block_number AND t.tx_hash = tx.hash
        AND tx.block_time >= '{{ START_DATE }}' AND tx.block_time <= '{{ END_DATE }}'
LEFT JOIN {{ ref('tokens_nft') }} ON nft.contract_address = t.nft_contract_address AND nft.blockchain = 'ethereum'
LEFT JOIN {{ ref('nft_aggregators') }} ON agg.contract_address = tx.to AND agg.blockchain = 'ethereum'
LEFT JOIN {{ source('prices', 'usd') }}
    ON
        p.minute = date_trunc('minute', t.block_time)
        AND p.contract_address = t.currency_contract
        AND p.blockchain = 'ethereum'
        AND minute >= '{{ START_DATE }}' AND minute <= '{{ END_DATE }}'
LEFT JOIN {{ ref('tokens_erc20') }} ON erc20.contract_address = t.currency_contract AND erc20.blockchain = 'ethereum';
