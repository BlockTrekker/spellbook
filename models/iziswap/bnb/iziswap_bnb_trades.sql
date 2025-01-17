{{ config(
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "iziswap",
                                \'["umer_h_adil"]\') }}'
    )
}}

{% set project_start_date = '2022-05-10' %}

with iziswap_swaps as (
    select
        contract_address,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        case when sellxearny then tokeny else tokenx end as token_bought_address,
        case when sellxearny then tokenx else tokeny end as token_sold_address,
        case when sellxearny then amounty else amountx end as token_bought_amount_raw,
        case when sellxearny then amountx else amounty end as `token_sold_amount_raw`
    from
        {{ source('izumi_bnb', 'iZiSwapPool_evt_Swap') }}
    {% if is_incremental() %}
        where evt_block_time >= date_trunc("day", now() - interval "1 week")
    {% endif %}
)

select
    "bnb" as blockchain,
    "iziswap" as project,
    "1" as version,
    date_trunc("DAY", s.evt_block_time) as block_date,
    s.evt_block_time as block_time,
    CAST(s.token_bought_amount_raw as decimal(38, 0)) as token_bought_amount_raw,
    CAST(s.token_sold_amount_raw as decimal(38, 0)) as token_sold_amount_raw,
    coalesce(
        (s.token_bought_amount_raw / power(10, prices_b.decimals)) * prices_b.price,
        (s.token_sold_amount_raw / power(10, prices_s.decimals)) * prices_s.price
    ) as amount_usd,
    s.token_bought_address as token_bought_address,
    s.token_sold_address as token_sold_address,
    erc20_b.symbol as token_bought_symbol,
    erc20_s.symbol as token_sold_symbol,
    case
        when lower(erc20_b.symbol) > lower(erc20_s.symbol) then concat(erc20_s.symbol, "-", erc20_b.symbol)
        else concat(erc20_b.symbol, "-", erc20_s.symbol)
    end as token_pair,
    s.token_bought_amount_raw / power(10, erc20_b.decimals) as token_bought_amount,
    s.token_sold_amount_raw / power(10, erc20_s.decimals) as token_sold_amount,
    tx.from as taker,
    "" as maker,
    cast(s.contract_address as string) as project_contract_address,
    s.evt_tx_hash as tx_hash,
    tx.from as tx_from,
    tx.to as tx_to,
    "" as trace_address,
    s.evt_index as `evt_index`
from
    iziswap_swaps
inner join {{ source('bnb', 'transactions') }}
    on
        tx.hash = s.evt_tx_hash
        {% if not is_incremental() %}
    and tx.block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            and tx.block_time >= date_trunc("day", now() - interval "1 week")
        {% endif %}
-- bought tokens
left join {{ ref('tokens_erc20') }}
    on
        erc20_b.contract_address = s.token_bought_address
        and erc20_b.blockchain = "bnb"
-- sold tokens
left join {{ ref('tokens_erc20') }}
    on
        erc20_s.contract_address = s.token_sold_address
        and erc20_s.blockchain = "bnb"
-- price of bought tokens
left join {{ source('prices', 'usd') }}
    on
        prices_b.minute = date_trunc("minute", s.evt_block_time)
        and prices_b.contract_address = s.token_bought_address
        and prices_b.blockchain = "bnb"
        {% if not is_incremental() %}
    and prices_b.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            and prices_b.minute >= date_trunc("day", now() - interval "1 week")
        {% endif %}
-- price of sold tokens
left join {{ source('prices', 'usd') }}
    on
        prices_s.minute = date_trunc("minute", s.evt_block_time)
        and prices_s.contract_address = s.token_sold_address
        and prices_s.blockchain = "bnb"
        {% if not is_incremental() %}
    and prices_s.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            and prices_s.minute >= date_trunc("day", now() - interval "1 week")
        {% endif %}
