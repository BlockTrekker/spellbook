{{
    config(
        alias='dex_aggregator_traders',
        post_hook='{{ expose_spells(\'["ethereum", "fantom", "arbitrum", "gnosis", "optimism", "polygon"]\', 
        "sector", 
        "labels", 
        \'["gentrexha", "Henrystats"]\') }}'
    )
}}

with
dex_traders as (
    select distinct
        taker as address,
        blockchain
    from {{ ref('dex_aggregator_trades') }}
)

select
    blockchain,
    address,
    "DEX Aggregator Trader" as name,
    "dex" as category,
    "gentrexha" as contributor,
    "query" as source,
    timestamp("2022-12-14") as created_at,
    now() as updated_at,
    "dex_aggregator_traders" as model_name,
    "persona" as label_type
from
    dex_traders
