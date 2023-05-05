{{
    config(
        alias='contract_deployers_avalanche_c',
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby", "hosuke"]\') }}'
    )
}}

SELECT DISTINCT
    'avalanche_c' AS blockchain,
    creation.`from` AS address,
    'Contract Deployer' AS name,
    'infrastructure' AS category,
    'hildobby' AS contributor,
    'query' AS source,
    date('2023-03-03') AS created_at,
    NOW() AS updated_at,
    'contract_deployers' AS model_name,
    'persona' AS `label_type`
FROM {{ source('avalanche_c', 'creation_traces') }}
LEFT ANTI JOIN {{ source('avalanche_c', 'creation_traces') }} anti_table
ON creation.from = anti_table.address