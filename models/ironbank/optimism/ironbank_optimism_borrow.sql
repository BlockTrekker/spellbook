{{ config(
    alias = 'borrow',
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "ironbank",
                                \'["michael-ironbank"]\') }}'
) }}

SELECT
    b.evt_block_number AS block_number,
    b.evt_block_time AS block_time,
    b.evt_tx_hash AS tx_hash,
    b.evt_index AS `index`,
    CAST(b.contract_address AS VARCHAR(100)) AS contract_address,
    b.borrower,
    i.symbol,
    i.underlying_symbol,
    i.underlying_token_address AS underlying_address,
    CAST(b.borrowamount AS DOUBLE) / power(10, i.underlying_decimals) AS borrow_amount,
    CAST(b.borrowamount AS DOUBLE) / power(10, i.underlying_decimals) * p.price AS borrow_usd
FROM {{ source('ironbank_optimism', 'CErc20Delegator_evt_Borrow') }} AS b
LEFT JOIN {{ ref('ironbank_optimism_itokens') }} AS i ON CAST(b.contract_address AS VARCHAR(100)) = i.contract_address
LEFT JOIN {{ source('prices', 'usd') }} AS p ON p.minute = date_trunc('minute', b.evt_block_time) AND CAST(p.contract_address AS VARCHAR(100)) = i.underlying_token_address AND p.blockchain = 'optimism'
