{{ config(
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "zigzag",
                                \'["mtitus6"]\') }}'
    )
}}

{% set project_start_date = '2022-09-01' %}

with
dexs as (
    select
        call_block_time as block_time,
        zzmo.makerorder:sellToken as token_sold_address,
      zzmo.makerOrder:buyToken as token_bought_address,
      zzmo.output_matchedFillResults:takerSellFilledAmount as token_bought_amount_raw,
      zzmo.output_matchedFillResults:makerSellFilledAmount as token_sold_amount_raw,
      NULL AS amount_usd,
      zzmo.makerOrder:user as maker,
      zzmo.takerOrder:user as taker,
      call_tx_hash as tx_hash,
      '' AS trace_address,
      row_number() OVER(PARTITION BY call_tx_hash ORDER BY zzmo.makerOrder) AS evt_index, --prevent duplicates
      contract_address AS `project_contract_address`
    from
        {{ source('zigzag_test_v6_arbitrum', 'zigzag_settelment_call_matchOrders') }}
    where
        call_success = true
        {% if is_incremental() %}
            and zzmo.call_block_time >= date_trunc("day", now() - interval "1 week")
        {% endif %}
)

select
    "arbitrum" as blockchain,
    "zigzag" as project,
    "1" as version,
    TRY_CAST(date_trunc("DAY", dexs.block_time) as date) as block_date,
    dexs.block_time,
    erc20a.symbol as token_bought_symbol,
    erc20b.symbol as token_sold_symbol,
    case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, "-", erc20a.symbol)
        else concat(erc20a.symbol, "-", erc20b.symbol)
    end as token_pair,
    dexs.token_bought_amount_raw / power(10, erc20a.decimals) as token_bought_amount,
    dexs.token_sold_amount_raw / power(10, erc20b.decimals) as token_sold_amount,
    CAST(dexs.token_bought_amount_raw as decimal(38, 0)) as token_bought_amount_raw,
    CAST(dexs.token_sold_amount_raw as decimal(38, 0)) as token_sold_amount_raw,
    coalesce(
        dexs.amount_usd,
        (dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price,
        (dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) as amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    coalesce(dexs.taker, tx.from) as taker, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx.from as tx_from,
    tx.to as tx_to,
    dexs.trace_address,
    dexs.evt_index
from dexs
inner join {{ source('arbitrum', 'transactions') }}
    on
        tx.hash = dexs.tx_hash
        {% if not is_incremental() %}
    AND tx.block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            and tx.block_time >= date_trunc("day", now() - interval "1 week")
        {% endif %}
left join {{ ref('tokens_erc20') }}
    on
        erc20a.contract_address = dexs.token_bought_address
        and erc20a.blockchain = "arbitrum"
left join {{ ref('tokens_erc20') }}
    on
        erc20b.contract_address = dexs.token_sold_address
        and erc20b.blockchain = "arbitrum"
left join {{ source('prices', 'usd') }}
    on
        p_bought.minute = date_trunc("minute", dexs.block_time)
        and p_bought.contract_address = dexs.token_bought_address
        and p_bought.blockchain = "arbitrum"
        {% if not is_incremental() %}
    AND p_bought.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            and p_bought.minute >= date_trunc("day", now() - interval "1 week")
        {% endif %}
left join {{ source('prices', 'usd') }}
    on
        p_sold.minute = date_trunc("minute", dexs.block_time)
        and p_sold.contract_address = dexs.token_sold_address
        and p_sold.blockchain = "arbitrum"
        {% if not is_incremental() %}
    AND p_sold.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            and p_sold.minute >= date_trunc("day", now() - interval "1 week")
        {% endif %}
