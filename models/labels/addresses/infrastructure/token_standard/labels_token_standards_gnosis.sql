{{ config(alias='token_standards_gnosis',
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}') }}

SELECT DISTINCT
    'gnosis' AS blockchain,
    erc20.contract_address AS address,
    'erc20' AS name,
    'infrastructure' AS category,
    'hildobby' AS contributor,
    'query' AS source,
    date('2023-03-02') AS created_at,
    NOW() AS modified_at,
    'token_standard' AS model_name,
    'persona' AS `label_type`
FROM {{ source('erc20_gnosis', 'evt_transfer') }}
{% if is_incremental() %}
LEFT ANTI JOIN this t ON t.address = erc20.contract_address
WHERE erc20.evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}

UNION ALL

SELECT DISTINCT
    'gnosis' AS blockchain,
    nft.contract_address AS address,
    token_standard AS name,
    'infrastructure' AS category,
    'hildobby' AS contributor,
    'query' AS source,
    date('2023-03-02') AS created_at,
    NOW() AS modified_at,
    'token_standard' AS model_name,
    'persona' AS `label_type`
FROM {{ ref('nft_gnosis_transfers') }}
{% if is_incremental() %}
LEFT ANTI JOIN this t ON t.address = nft.contract_address
WHERE nft.block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}
