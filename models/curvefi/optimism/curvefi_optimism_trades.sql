{{ config(
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    )
}}

-- This should depend on 'curvefi_optimism_pools' running first
-- Original Ref - Dune v1 Abstraction: https://github.com/duneanalytics/spellbook/blob/main/deprecated-dune-v1-abstractions/optimism2/dex/insert_curve.sql
-- Start Time
-- SELECT MIN(evt_block_time) FROM curvefi_optimism.StableSwap_evt_TokenExchange
-- UNION ALL
-- SELECT MIN(evt_block_time) FROM curvefi_optimism.MetaPoolSwap_evt_TokenExchange
{% set project_start_date = '2022-01-17' %}

with dexs as (
    select
        pool_type,
        block_time,
        block_number,
        taker,
        maker,
        token_bought_amount_raw,
        token_sold_amount_raw,
        ta.token as token_bought_address,
        tb.token as token_sold_address,
        project_contract_address,
        tx_hash,
        trace_address,
        evt_index,
        bought_id,
        sold_id
    from (
        -- Stableswap
        select
            'stable' as pool_type, -- has implications for decimals for curve
            t.evt_block_time as block_time,
            t.evt_block_number as block_number,
            t.buyer as taker,
            '' as maker,
            -- when amount0 is negative it means taker is buying token0 from the pool
            tokens_bought as token_bought_amount_raw,
            tokens_sold as token_sold_amount_raw,
            t.contract_address as project_contract_address,
            t.evt_tx_hash as tx_hash,
            '' as trace_address,
            t.evt_index,
            bought_id,
            sold_id
        from {{ source('curvefi_optimism', 'StableSwap_evt_TokenExchange') }} as t
        {% if is_incremental() %}
            where t.evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}

        union all

        -- MetaPoolSwap TokenExchangeUnderlying
        select
            'meta' as pool_type, -- has implications for decimals for curve
            t.evt_block_time as block_time,
            t.evt_block_number,
            t.buyer as taker,
            '' as maker,
            -- when amount0 is negative it means taker is buying token0 from the pool
            tokens_bought as token_bought_amount_raw,
            tokens_sold as token_sold_amount_raw,
            t.contract_address as project_contract_address,
            t.evt_tx_hash as tx_hash,
            '' as trace_address,
            t.evt_index,
            bought_id,
            sold_id
        from {{ source('curvefi_optimism', 'MetaPoolSwap_evt_TokenExchangeUnderlying') }} as t
        {% if is_incremental() %}
            where t.evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}

        union all

        -- StableSwap - Mislabeled as MetaPoolSwap TokenExchange
        select
            'stable' as pool_type, -- has implications for decimals for curve
            t.evt_block_time as block_time,
            t.evt_block_number,
            t.buyer as taker,
            '' as maker,
            -- when amount0 is negative it means taker is buying token0 from the pool
            tokens_bought as token_bought_amount_raw,
            tokens_sold as token_sold_amount_raw,
            t.contract_address as project_contract_address,
            t.evt_tx_hash as tx_hash,
            '' as trace_address,
            t.evt_index,
            bought_id,
            sold_id
        from {{ source('curvefi_optimism', 'MetaPoolSwap_evt_TokenExchange') }} as t
        -- handle for dupes due to decoding issues
        where
            not exists (
                select 1 from {{ source('curvefi_optimism', 'MetaPoolSwap_evt_TokenExchangeUnderlying') }} as s
                where
                    t.evt_block_number = s.evt_block_number
                    and t.evt_tx_hash = s.evt_tx_hash
                    and t.evt_index = s.evt_index
                    {% if is_incremental() %}
                        and s.evt_block_time >= date_trunc('day', now() - interval '1 week')
                    {% endif %}
            )
            and not exists (
                select 1 from {{ source('curvefi_optimism', 'StableSwap_evt_TokenExchange') }} as s
                where
                    t.evt_block_number = s.evt_block_number
                    and t.evt_tx_hash = s.evt_tx_hash
                    and t.evt_index = s.evt_index
                    {% if is_incremental() %}
                        and s.evt_block_time >= date_trunc('day', now() - interval '1 week')
                    {% endif %}
            )

            {% if is_incremental() %}
                and t.evt_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    ) as cp
    inner join {{ ref('curvefi_optimism_pools') }} as ta
        on
            cp.project_contract_address = ta.pool
            and cp.bought_id = ta.tokenid
    inner join {{ ref('curvefi_optimism_pools') }} as tb
        on
            cp.project_contract_address = tb.pool
            and cp.sold_id = tb.tokenid
    left join {{ ref('tokens_optimism_erc20') }} as ea
        on ea.contract_address = ta.token
    left join {{ ref('tokens_optimism_erc20') }} as eb
        on eb.contract_address = tb.token
)

select distinct
    'optimism' as blockchain,
    'curve' as project,
    '1' as version,
    TRY_CAST(date_trunc('DAY', dexs.block_time) as date) as block_date,
    dexs.block_time,
    COALESCE(erc20a.symbol, p_bought.symbol) as token_bought_symbol,
    COALESCE(erc20b.symbol, p_sold.symbol) as token_sold_symbol,
    case
        when lower(COALESCE(erc20a.symbol, p_bought.symbol)) > lower(COALESCE(erc20b.symbol, p_sold.symbol)) then concat(COALESCE(erc20b.symbol, p_sold.symbol), '-', COALESCE(erc20a.symbol, p_bought.symbol))
        else concat(COALESCE(erc20a.symbol, p_bought.symbol), '-', COALESCE(erc20b.symbol, p_sold.symbol))
    end as token_pair,
    --On Sell: Metapools seem to always use the added coin's decimals if it's the one that's bought - even if the other token has less decimals (i.e. USDC)
    --On Buy: Metapools seem to always use the curve pool token's decimals (18) if bought_id = 0
    dexs.token_bought_amount_raw / POWER(10, (case when pool_type = 'meta' and bought_id = 0 then 18 else COALESCE(erc20a.decimals, p_bought.decimals) end)) as token_bought_amount,
    dexs.token_sold_amount_raw / POWER(10, (case when pool_type = 'meta' and bought_id = 0 then COALESCE(erc20a.decimals, p_bought.decimals) else COALESCE(erc20b.decimals, p_sold.decimals) end)) as token_sold_amount,
    CAST(dexs.token_bought_amount_raw as decimal(38, 0)) as token_bought_amount_raw,
    CAST(dexs.token_sold_amount_raw as decimal(38, 0)) as token_sold_amount_raw,
    coalesce(
        --On Sell: Metapools seem to always use the added coin's decimals if it's the one that's bought - even if the other token has less decimals (i.e. USDC)
        --On Buy: Metapools seem to always use the curve pool token's decimals (18) if bought_id = 0
        dexs.token_bought_amount_raw / POWER(10, case when pool_type = 'meta' and bought_id = 0 then 18 else COALESCE(erc20a.decimals, p_bought.decimals) end) * p_bought.price,
        dexs.token_sold_amount_raw / POWER(10, case when pool_type = 'meta' and bought_id = 0 then COALESCE(erc20a.decimals, p_bought.decimals) else COALESCE(erc20b.decimals, p_sold.decimals) end) * p_sold.price
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
    dexs.evt_index,
    dexs.pool_type
from dexs
inner join {{ source('optimism', 'transactions') }} as tx
    on
        dexs.tx_hash = tx.hash
        and dexs.block_number = tx.block_number
        {% if not is_incremental() %}
    AND tx.block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            and tx.block_time >= date_trunc('day', now() - interval '1' week)
        {% endif %}
left join {{ ref('tokens_erc20') }} as erc20a
    on
        erc20a.contract_address = dexs.token_bought_address
        and erc20a.blockchain = 'optimism'
left join {{ ref('tokens_erc20') }} as erc20b
    on
        erc20b.contract_address = dexs.token_sold_address
        and erc20b.blockchain = 'optimism'
left join {{ source('prices', 'usd') }} as p_bought
    on
        p_bought.minute = date_trunc('minute', dexs.block_time)
        and p_bought.contract_address = dexs.token_bought_address
        and p_bought.blockchain = 'optimism'
        {% if not is_incremental() %}
    AND p_bought.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            and p_bought.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
left join {{ source('prices', 'usd') }} as p_sold
    on
        p_sold.minute = date_trunc('minute', dexs.block_time)
        and p_sold.contract_address = dexs.token_sold_address
        and p_sold.blockchain = 'optimism'
        {% if not is_incremental() %}
    AND p_sold.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            and p_sold.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
;
