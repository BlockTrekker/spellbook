{{
    config(
        alias='contract_deployers_polygon',
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby", "hosuke"]\') }}'
    )
}}

SELECT DISTINCT
    'polygon' AS blockchain,
    creation.`from` AS address,
    'Contract Deployer' AS name,
    'infrastructure' AS category,
    'hildobby' AS contributor,
    'query' AS source,
    date('2023-03-03') AS created_at,
    NOW() AS updated_at,
    'contract_deployers' AS model_name,
    'persona' AS `label_type`
FROM {{ source('polygon', 'creation_traces') }}
LEFT ANTI JOIN {{ source('polygon', 'creation_traces') }} anti_table
ON creation.from = anti_table.address