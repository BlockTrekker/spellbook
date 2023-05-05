{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "stealcam",
                                \'["hildobby","pandajackson42"]\') }}')
}}


{% set project_start_date = '2023-03-10' %}

with stealcam as (
    select
        *,
        case when value > 0 then (value - (0.001 * pow(10, 18))) / 11.0 + (0.001 * pow(10, 18)) else 0 end as `surplus_value`
    from {{ source('stealcam_arbitrum', 'Stealcam_evt_Stolen') }}
    {% if is_incremental() %}
        where evt_block_time >= date_trunc("day", now() - interval "1 week")
    {% endif %}
    {% if not is_incremental() %}
WHERE evt_block_time >= '{{ project_start_date }}'
{% endif %}

)

select
    "arbitrum" as blockchain,
    "Stealcam" as project,
    "v1" as version,
    sc.evt_block_time as block_time,
    date_trunc("day", sc.evt_block_time) as block_date,
    sc.evt_block_number as block_number,
    "Single Item Trade" as trade_type,
    "Buy" as trade_category,
    case when sc.value = 0 then "Mint" else "Trade" end as evt_type,
    sc.from as seller,
    sc.to as buyer,
    sc.contract_address as nft_contract_address,
    "Stealcam" as collection,
    sc.id as token_id,
    "erc721" as token_standard,
    CAST(1 as decimal(38, 0)) as number_of_items,
    "0x82af49447d8a07e3bd95bd0d56f35241523fbab1" as currency_contract,
    "ETH" as currency_symbol,
    CAST(sc.value as decimal(38, 0)) as amount_raw,
    CAST(sc.value / POWER(10, 18) as double) as amount_original,
    CAST(pu.price * sc.value / POWER(10, 18) as double) as amount_usd,
    sc.contract_address as project_contract_address,
    CAST(NULL as string) as aggregator_name,
    CAST(NULL as string) as aggregator_address,
    sc.evt_tx_hash as tx_hash,
    at.from AS `tx_from`
, at.to AS `tx_to`
, CAST(0.1*surplus_value AS DECIMAL(38,0)) AS `platform_fee_amount_raw`
, CAST(0.1*surplus_value/POWER(10, 18) AS double) AS `platform_fee_amount`
, CAST(pu.price*0.1*surplus_value/POWER(10, 18) AS double) AS `platform_fee_amount_usd`
, CAST(coalesce(100*(0.1*surplus_value/sc.value),0) AS double) AS `platform_fee_percentage`
, 'ETH' AS `royalty_fee_currency_symbol`
, CAST(0.45*surplus_value AS DECIMAL(38,0)) AS `royalty_fee_amount_raw`
, CAST(0.45*surplus_value/POWER(10, 18) AS double) AS `royalty_fee_amount`
, CAST(pu.price*0.45*surplus_value/POWER(10, 18) AS double) AS `royalty_fee_amount_usd`
, CAST(coalesce(100*(0.45*surplus_value/sc.value),0) AS double) AS `royalty_fee_percentage`
, m._creator AS `royalty_fee_receive_address`
, 'arbitrum-stealcam-' || sc.evt_tx_hash || '-' || sc.evt_index AS `unique_trade_id`
from stealcam
inner join {{ source('arbitrum', 'transactions') }} at ON at.block_number=sc.evt_block_number
    AND at.hash=sc.evt_tx_hash
    {% if is_incremental() %}
    AND at.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND at.block_time >= '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    AND pu.minute=date_trunc('minute', sc.evt_block_time)
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND pu.minute >= '{{project_start_date}}'
    {% endif %}
INNER JOIN {{ source('stealcam_arbitrum', 'Stealcam_call_mint') }} m ON m.call_success
    AND m.id=sc.id
    {% if is_incremental() %}
    AND m.call_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
{% if not is_incremental() %}
    AND m.call_block_time >= '{{ project_start_date }}'
    {% endif %}
