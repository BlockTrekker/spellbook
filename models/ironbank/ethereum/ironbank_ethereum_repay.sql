{{ config(
    alias = 'repay',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "ironbank",
                                \'["michael-ironbank"]\') }}'
) }}

SELECT
    r.evt_block_number AS block_number,
    r.evt_block_time AS block_time,
    r.evt_tx_hash AS tx_hash,
    r.evt_index AS `index`,
    CAST(r.contract_address AS VARCHAR(100)) AS contract_address,
    r.borrower,
    i.symbol,
    i.underlying_symbol,
    i.underlying_token_address AS underlying_address,
    CAST(r.repayamount AS DOUBLE) / power(10, i.underlying_decimals) AS repay_amount,
    CAST(r.repayamount AS DOUBLE) / power(10, i.underlying_decimals) * p.price AS repay_usd
FROM {{ source('ironbank_ethereum', 'CErc20Delegator_evt_RepayBorrow') }} AS r
LEFT JOIN {{ ref('ironbank_ethereum_itokens') }} AS i ON i.contract_address = CAST(r.contract_address AS VARCHAR(100))
LEFT JOIN {{ source('prices', 'usd') }} AS p ON p.minute = date_trunc('minute', r.evt_block_time) AND CAST(p.contract_address AS VARCHAR(100)) = i.underlying_token_address AND p.blockchain = 'ethereum'
