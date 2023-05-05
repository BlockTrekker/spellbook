{{ config(
        alias ='wash_trades',
        partition_by='block_date',
        materialized='incremental',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "nft",
                                    \'["hildobby"]\') }}',
        unique_key = ['unique_trade_id']
)
}}

WITH filter_1 AS (
    SELECT
        unique_trade_id,
        COALESCE(nftt.buyer = nftt.seller, FALSE) AS same_buyer_seller
    FROM {{ ref('nft_trades') }} AS nftt
    WHERE
        nftt.blockchain = 'gnosis'
        AND nftt.unique_trade_id IS NOT NULL
        {% if is_incremental() %}
            AND nftt.block_time >= date_trunc('day', NOW() - interval '1 week')
        {% endif %}
),

filter_2 AS (
    SELECT
        nftt.unique_trade_id,
        COALESCE(COUNT(filter_baf.block_number) > 0, FALSE) AS back_and_forth_trade
    FROM {{ ref('nft_trades') }} AS nftt
    INNER JOIN {{ ref('nft_trades') }} AS filter_baf
        ON
            filter_baf.seller = nftt.buyer
            AND filter_baf.buyer = nftt.seller
            AND filter_baf.nft_contract_address = nftt.nft_contract_address
            AND filter_baf.token_id = nftt.token_id
            {% if is_incremental() %}
                AND filter_baf.block_time >= date_trunc('day', NOW() - interval '1 week')
            {% endif %}
    WHERE
        nftt.blockchain = 'gnosis'
        AND nftt.unique_trade_id IS NOT NULL
        {% if is_incremental() %}
            AND nftt.block_time >= date_trunc('day', NOW() - interval '1 week')
        {% endif %}
    GROUP BY nftt.unique_trade_id
),

filter_3_bought AS (
    SELECT
        nftt.unique_trade_id,
        COALESCE(COUNT(filter_bought_3x.block_number) > 2, FALSE) AS bought_3x
    FROM {{ ref('nft_trades') }} AS nftt
    INNER JOIN {{ ref('nft_trades') }} AS filter_bought_3x
        ON
            filter_bought_3x.nft_contract_address = nftt.nft_contract_address
            AND filter_bought_3x.token_id = nftt.token_id
            AND filter_bought_3x.buyer = nftt.buyer
            AND filter_bought_3x.token_standard IN ('erc721', 'erc20')
            {% if is_incremental() %}
                AND filter_bought_3x.block_time >= date_trunc('day', NOW() - interval '1 week')
            {% endif %}
    WHERE
        nftt.blockchain = 'gnosis'
        AND nftt.unique_trade_id IS NOT NULL
        {% if is_incremental() %}
            AND nftt.block_time >= date_trunc('day', NOW() - interval '1 week')
        {% endif %}
    GROUP BY nftt.unique_trade_id
),

filter_3_sold AS (
    SELECT
        nftt.unique_trade_id,
        COALESCE(COUNT(filter_sold_3x.block_number) > 2, FALSE) AS sold_3x
    FROM {{ ref('nft_trades') }} AS nftt
    INNER JOIN {{ ref('nft_trades') }} AS filter_sold_3x
        ON
            filter_sold_3x.nft_contract_address = nftt.nft_contract_address
            AND filter_sold_3x.token_id = nftt.token_id
            AND filter_sold_3x.seller = nftt.seller
            AND filter_sold_3x.token_standard IN ('erc721', 'erc20')
            {% if is_incremental() %}
                AND filter_sold_3x.block_time >= date_trunc('day', NOW() - interval '1 week')
            {% endif %}
    WHERE
        nftt.blockchain = 'gnosis'
        AND nftt.unique_trade_id IS NOT NULL
        {% if is_incremental() %}
            AND nftt.block_time >= date_trunc('day', NOW() - interval '1 week')
        {% endif %}
    GROUP BY nftt.unique_trade_id
),

filter_4 AS (
    SELECT
        nftt.unique_trade_id,
        COALESCE(
            filter_funding_buyer.first_funded_by = filter_funding_seller.first_funded_by
            OR filter_funding_buyer.first_funded_by = nftt.seller
            OR filter_funding_seller.first_funded_by = nftt.buyer, FALSE
        ) AS first_funded_by_same_wallet,
        filter_funding_buyer.first_funded_by AS buyer_first_funded_by,
        filter_funding_seller.first_funded_by AS seller_first_funded_by
    FROM {{ ref('nft_trades') }} AS nftt
    INNER JOIN {{ ref('addresses_events_gnosis_first_funded_by') }} AS filter_funding_buyer
        ON
            filter_funding_buyer.address = nftt.buyer
            AND filter_funding_buyer.first_funded_by NOT IN (SELECT DISTINCT address FROM {{ ref('labels_bridges') }})
            AND filter_funding_buyer.first_funded_by NOT IN (SELECT DISTINCT address FROM {{ ref('labels_cex') }})
            AND filter_funding_buyer.first_funded_by NOT IN (SELECT DISTINCT contract_address FROM {{ ref('tornado_cash_withdrawals') }})
            {% if is_incremental() %}
                AND filter_funding_buyer.block_time >= date_trunc('day', NOW() - interval '1 week')
            {% endif %}
    INNER JOIN {{ ref('addresses_events_gnosis_first_funded_by') }} AS filter_funding_seller
        ON
            filter_funding_seller.address = nftt.seller
            AND filter_funding_seller.first_funded_by NOT IN (SELECT DISTINCT address FROM {{ ref('labels_bridges') }})
            AND filter_funding_seller.first_funded_by NOT IN (SELECT DISTINCT address FROM {{ ref('labels_cex') }})
            AND filter_funding_seller.first_funded_by NOT IN (SELECT DISTINCT contract_address FROM {{ ref('tornado_cash_withdrawals') }})
            {% if is_incremental() %}
                AND filter_funding_seller.block_time >= date_trunc('day', NOW() - interval '1 week')
            {% endif %}
    WHERE
        nftt.blockchain = 'gnosis'
        AND nftt.unique_trade_id IS NOT NULL
        AND nftt.buyer IS NOT NULL
        AND nftt.seller IS NOT NULL
        {% if is_incremental() %}
            AND nftt.block_time >= date_trunc('day', NOW() - interval '1 week')
        {% endif %}
)

SELECT
    nftt.blockchain,
    nftt.project,
    nftt.version,
    nftt.nft_contract_address,
    nftt.token_id,
    nftt.token_standard,
    nftt.trade_category,
    nftt.buyer,
    nftt.seller,
    nftt.project_contract_address,
    nftt.aggregator_name,
    nftt.aggregator_address,
    nftt.tx_from,
    nftt.tx_to,
    nftt.block_time,
    date_trunc('day', nftt.block_time) AS block_date,
    nftt.block_number,
    nftt.tx_hash,
    nftt.unique_trade_id,
    buyer_first_funded_by,
    seller_first_funded_by,
    COALESCE(filter_1.same_buyer_seller, FALSE) AS filter_1_same_buyer_seller,
    COALESCE(filter_2.back_and_forth_trade, FALSE) AS filter_2_back_and_forth_trade,
    COALESCE(
        filter_3_bought.bought_3x
        OR filter_3_sold.sold_3x, FALSE
    ) AS filter_3_bought_or_sold_3x,
    COALESCE(filter_4.first_funded_by_same_wallet, FALSE) AS filter_4_first_funded_by_same_wallet,
    COALESCE(
        filter_1.same_buyer_seller
        OR filter_2.back_and_forth_trade
        OR filter_3_bought.bought_3x
        OR filter_3_sold.sold_3x
        OR filter_4.first_funded_by_same_wallet, FALSE
    ) AS is_wash_trade
FROM {{ ref('nft_trades') }} AS nftt
LEFT JOIN filter_1 ON nftt.unique_trade_id = filter_1.unique_trade_id
LEFT JOIN filter_2 ON nftt.unique_trade_id = filter_2.unique_trade_id
LEFT JOIN filter_3_bought ON nftt.unique_trade_id = filter_3_bought.unique_trade_id
LEFT JOIN filter_3_sold ON nftt.unique_trade_id = filter_3_sold.unique_trade_id
LEFT JOIN filter_4 ON nftt.unique_trade_id = filter_4.unique_trade_id
WHERE
    nftt.blockchain = 'gnosis'
    AND nftt.unique_trade_id IS NOT NULL
    {% if is_incremental() %}
        AND nftt.block_time >= date_trunc('day', NOW() - interval '1 week')
    {% endif %}
;
