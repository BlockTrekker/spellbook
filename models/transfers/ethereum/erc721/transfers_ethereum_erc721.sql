{{ config(materialized='view', alias='erc721') }}

with
received_transfers as (
    select
        'receive' || '-' || evt_tx_hash || '-' || CAST(evt_index as VARCHAR(100)) || '-' || `to` as unique_tx_id,
        `to` as wallet_address,
        contract_address as token_address,
        evt_block_time,
        tokenid,
        1 as `amount`
    from
        {{ source('erc721_ethereum', 'evt_transfer') }}
)

,
sent_transfers as (
    select
        'send' || '-' || evt_tx_hash || '-' || CAST(evt_index as VARCHAR(100)) || '-' || `from` as unique_tx_id,
        `from` as wallet_address,
        contract_address as token_address,
        evt_block_time,
        tokenid,
        -1 as `amount`
    from
        {{ source('erc721_ethereum', 'evt_transfer') }}
)

select
    'ethereum' as blockchain,
    wallet_address,
    token_address,
    evt_block_time,
    tokenid,
    amount,
    unique_tx_id
from received_transfers
union
select 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, tokenId, amount, unique_tx_id
from sent_transfers