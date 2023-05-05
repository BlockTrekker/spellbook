{{ config(
    alias = 'inorganic_volume_filter_looksrare',
    materialized = 'view'
)
}}

{% set project_start_date = '2022-01-02' %} -- looksrare start date 

WITH

trades AS (
    SELECT t.*
    FROM
        {{ ref('nft_trades') }}
    WHERE
        1 = 1
        AND t.project IN ('opensea', 'looksrare')
        AND t.blockchain = 'ethereum'
),

royal_settings AS (
    SELECT
        collection,
        fee
    FROM
        (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY collection ORDER BY evt_block_time DESC) AS `ordering`
            FROM
                (
                    SELECT *
                    FROM
                        {{ source('looksrare_ethereum', 'RoyaltyFeeRegistry_evt_RoyaltyFeeUpdate') }}
                ) AS `x1`
        ) AS `x2`
    WHERE ordering = 1
),

mt_filter AS (
    SELECT
        *,
        'mt' AS `filter`
    FROM
        (
            SELECT
                date_trunc('day', block_time) AS day,
                nft_contract_address,
                token_id AS nft_token_id,
                COUNT(*) AS `num_sales`
            FROM
                trades
            WHERE
                project = 'looksrare'
                AND token_standard = 'erc721'
            GROUP BY 1, 2, 3
        ) AS `trade_count`
    WHERE
        true
        AND num_sales >= 3
),

sb_filter AS (
    SELECT
        *,
        'sb' AS `filter`
    FROM
        (
            SELECT
                date_trunc('day', t.block_time) AS day,
                CASE WHEN t.seller > t.buyer THEN t.seller ELSE t.buyer END AS address1,
                CASE WHEN t.seller > t.buyer THEN t.buyer ELSE t.seller END AS address2,
                COUNT(DISTINCT tx_hash) AS `num_sales`
            FROM
                trades
            LEFT JOIN
                {{ ref('nft_ethereum_aggregators') }}
                ON agg.contract_address = t.buyer
            WHERE
                t.project = 'looksrare'
                AND agg.contract_address IS NULL
            GROUP BY 1, 2, 3
        ) AS `foo`
),

lv_filter AS (
    SELECT
        *,
        'lv' AS `filter`
    FROM
        (
            SELECT
                day,
                nft_address,
                os_vol,
                SUM(os_vol) OVER (PARTITION BY nft_address ORDER BY day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 30d_vol
            FROM
                (
                    SELECT
                        date_trunc('day', t.block_time) AS day,
                        x2.nft_address,
                        SUM(t.amount_usd) AS `os_vol`
                    FROM
                        (
                            SELECT DISTINCT nft_contract_address AS `nft_address`
                            FROM
                                trades
                            WHERE project = 'looksrare'
                        ) AS `x2`
                    LEFT JOIN trades
                        ON x2.nft_address = t.nft_contract_address
                    WHERE t.project = 'opensea'
                    GROUP BY 1, 2
                ) AS `foo`
        ) AS `foo2`
    WHERE true 
        AND 30d_vol < 100
),

hp_filter AS (
    SELECT
        *,
        'hp' AS `filter`
    FROM
        (
            SELECT
                day,
                nft_address,
                high_price,
                10 * highprice_cutoff AS `highprice_cutoff`
            FROM
                (
                    SELECT
                        day,
                        nft_address,
                        highprice_cutoff AS high_price,
                        MAX(highprice_cutoff) OVER (PARTITION BY nft_address ORDER BY day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS `highprice_cutoff`
                    FROM
                        (
                            SELECT
                                date_trunc('day', t.block_time) AS day,
                                x2.nft_address,
                                MAX(t.amount_usd) AS `highprice_cutoff`
                            FROM
                                (
                                    SELECT DISTINCT t.nft_contract_address AS `nft_address`
                                    FROM
                                        trades
                                    INNER JOIN
                                        royal_settings
                                        ON
                                            t.nft_contract_address = r.collection
                                            AND r.fee = 0
                                    WHERE t.project = 'looksrare'
                                ) AS `x2`
                            LEFT JOIN trades
                                ON x2.nft_address = t.nft_contract_address
                            WHERE t.project = 'opensea'
                            GROUP BY 1, 2
                        ) AS `foo`
                ) AS `foo2`
        ) AS `foo3`
),

wf_filter AS (
    SELECT DISTINCT
        t.buyer,
        t.seller,
        'wf' AS `filter`
    FROM
        trades
    LEFT JOIN
        {{ ref('opensea_inorganic_volume_filter_wallet_funders') }}
        ON f1.wallet = t.buyer
    LEFT JOIN
        {{ ref('opensea_inorganic_volume_filter_wallet_funders') }}
        ON f2.wallet = t.seller
    WHERE
        t.project = 'looksrare'
        AND f1.funder = f2.funder
        OR (f1.funder = t.seller OR f2.funder = t.buyer)
),

circular_buyer AS (
    SELECT
        *,
        'circular_buyer' AS `filter`
    FROM
        (
            SELECT
                COUNT(*) AS cnt,
                token_id AS nft_token_id,
                nft_contract_address,
                buyer
            FROM
                trades
            WHERE
                t.project = 'looksrare'
                AND token_standard = 'erc721'
                AND buyer != LOWER('0x39da41747a83aee658334415666f3ef92dd0d541')
            GROUP BY 2, 3, 4
        ) AS `foo`
    WHERE cnt >= 2
),

circular_seller AS (
    SELECT
        *,
        'circular_seller' AS `filter`
    FROM
        (
            SELECT
                COUNT(*) AS cnt,
                token_id AS nft_token_id,
                nft_contract_address,
                seller
            FROM
                trades
            WHERE
                t.project = 'looksrare'
                AND token_standard = 'erc721'
                AND buyer != LOWER('0x39da41747a83aee658334415666f3ef92dd0d541')
            GROUP BY 2, 3, 4
        ) AS `foo`
    WHERE cnt >= 2
),

trades_enrich AS (
    SELECT
        date_trunc('day', t.block_time) AS day,
        t.block_time,
        t.project,
        t.nft_contract_address,
        t.token_id AS nft_token_id,
        t.tx_hash,
        erc20.symbol AS currency,
        t.amount_raw / POW(10, erc20.decimals) AS amount,
        p.price AS usd_price,
        t.amount_raw / POW(10, erc20.decimals) * p.price AS usd_amount,
        t.buyer,
        t.seller,
        t.unique_trade_id
    FROM
        trades
    LEFT JOIN {{ source('prices', 'usd') }}
        ON
            p.minute = date_trunc('minute', t.block_time)
            AND p.contract_address = t.currency_contract
            AND p.blockchain = 'ethereum'
            AND p.minute >= '{{ project_start_date }}'
    LEFT JOIN
        {{ ref('tokens_erc20') }}
        ON
            t.currency_contract = erc20.contract_address
            AND erc20.blockchain = 'ethereum'
    WHERE t.project = 'looksrare'
),

filtered_trades AS (
    SELECT
        t.*,
        COALESCE(mt.filter IS NOT NULL, false) AS mt_filter,
        COALESCE(sb.filter IS NOT NULL, false) AS sb_filter,
        COALESCE(lv.filter IS NOT NULL, false) AS lv_filter,
        COALESCE(hp.filter IS NOT NULL, false) AS hp_filter,
        COALESCE(wf.filter IS NOT NULL, false) AS wf_filter,
        COALESCE(cb.filter IS NOT NULL, false) AS cb_filter,
        COALESCE(cs.filter IS NOT NULL, false) AS cs_filter,
        FILTER(array(mt.filter, sb.filter, lv.filter, hp.filter, wf.filter, cb.filter, cs.filter), x -> x IS NOT NULL) AS `inorganic_filters`
    FROM
        trades_enrich
    LEFT JOIN
        mt_filter
        ON
            mt.day = t.day
            AND mt.nft_contract_address = t.nft_contract_address
            AND mt.nft_token_id = t.nft_token_id
    LEFT JOIN
        sb_filter
        ON
            sb.day = t.day
            AND (
                (t.buyer = sb.address1 AND t.seller = sb.address2)
                OR (t.seller = sb.address1 AND t.buyer = sb.address2)
            )
    LEFT JOIN
        lv_filter
        ON
            lv.nft_address = t.nft_contract_address
            AND lv.day = t.day
    LEFT JOIN
        hp_filter
        ON
            t.nft_contract_address = hp.nft_address
            AND t.day = hp.day
            AND t.usd_amount > hp.highprice_cutoff
    LEFT JOIN
        wf_filter
        ON
            wf.buyer = t.buyer
            AND wf.seller = t.seller
    LEFT JOIN
        circular_buyer
        ON
            t.nft_contract_address = cb.nft_contract_address
            AND t.buyer = cb.buyer
            AND t.nft_token_id = cb.nft_token_id
    LEFT JOIN
        circular_seller
        ON
            t.nft_contract_address = cs.nft_contract_address
            AND t.seller = cs.seller
            AND t.nft_token_id = cs.nft_token_id
)

SELECT
    *,
    COALESCE(cardinality(inorganic_filters) > 0 AND inorganic_filters IS NOT NULL, false) AS `any_filter`
FROM
    filtered_trades
