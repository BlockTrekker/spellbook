{{
    config(
        alias='contract_deployers_gnosis',
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby", "hosuke"]\') }}'
    )
}}

SELECT DISTINCT
    'gnosis' AS blockchain,
    creation.`from` AS address,
    'Contract Deployer' AS name,
    'infrastructure' AS category,
    'hildobby' AS contributor,
    'query' AS source,
    date('2023-03-03') AS created_at,
    NOW() AS updated_at,
    'contract_deployers' AS model_name,
    'persona' AS `label_type`
FROM {{ source('gnosis', 'creation_traces') }}
LEFT ANTI JOIN {{ source('gnosis', 'creation_traces') }} anti_table
ON creation.from = anti_table.address