{{ config(
    schema = 'aztec_v2_ethereum',
    alias = 'deposit_assets',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "aztec_v2",
                                \'["Henrystats"]\') }}')
}}

WITH

assets_added AS (
    SELECT
        CAST('0' AS VARCHAR(5)) AS asset_id,
        '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' AS asset_address,
        null AS asset_gas_limit,
        null AS date_added

        UNION
        
        SELECT 
            assetId as asset_id,
            assetAddress as asset_address,
            assetGasLimit as asset_gas_limit,
            evt_block_time as date_added
    FROM
        {{ source('aztec_v2_ethereum', 'RollupProcessor_evt_AssetAdded') }}
)

SELECT
    a.*,
    t.symbol,
    t.decimals
FROM
    assets_added AS a
LEFT JOIN
    {{ ref('tokens_erc20') }} AS t
    ON
        a.asset_address = t.contract_address
        AND t.blockchain = 'ethereum';
