{{ config(alias='dao_framework') }}

WITH dao_address_w_name AS (
    SELECT
        blockchain,
        dao AS address,
        CASE
            WHEN dao_creator_tool = 'aragon' THEN 'DAO: Aragon'
            WHEN dao_creator_tool = 'colony' THEN 'DAO: Colony'
            WHEN dao_creator_tool = 'dao-haus' THEN 'DAO: DAO Haus'
            WHEN dao_creator_tool = 'syndicate' THEN 'DAO: Syndicate Investment Club'
        END AS `name`

    FROM {{ ref('dao_addresses') }}
    WHERE dao_creator_tool != 'zodiac' -- excluding zodiac since they're gnosis safes

    UNION  -- using a union because there are daos whose contract address also receives and send funds

    SELECT
    blockchain,
    dao_wallet_address as address,
    CASE
        WHEN dao_creator_tool = 'aragon' THEN 'DAO: Aragon'
        WHEN dao_creator_tool = 'colony' THEN 'DAO: Colony'
        WHEN dao_creator_tool = 'dao-haus' THEN 'DAO: DAO Haus'
    END AS `name`
    FROM {{ ref('dao_addresses') }}
    WHERE dao_creator_tool NOT IN ('zodiac', 'syndicate') -- excluding syndicate since their wallet addresses are controlled by EOAs
-- excluding zodiac since they're gnosis safes
)

SELECT
    blockchain,
    address,
    name,
    'dao' AS category,
    'henrystats' AS contributor,
    'query' AS source,
    timestamp('2022-11-05') AS created_at,
    now() AS updated_at,
    'dao_framework' AS model_name,
    'persona' AS `label_type`
FROM dao_address_w_name
