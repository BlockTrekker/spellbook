{{ config(materialized='view', alias='erc20',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke","dot2dotseurat"]\') }}') }}

with
    sent_transfers as (
        select
            CAST('send' as VARCHAR(4)) || CAST('-' as VARCHAR(1)) || CAST(evt_tx_hash as VARCHAR(100)) || CAST('-' as VARCHAR(1)) || CAST(evt_index as VARCHAR(100)) || CAST('-' as VARCHAR(1)) || CAST(`to` as VARCHAR(100)) as unique_transfer_id,
            `to` as wallet_address,
            contract_address as token_address,
            evt_block_time,
            value as amount_raw
        from
            {{ source('erc20_ethereum', 'evt_transfer') }}
    )

    ,
    received_transfers as (
        select
        CAST('receive' as VARCHAR(7)) || CAST('-' as VARCHAR(1)) || CAST(evt_tx_hash as VARCHAR(100)) || CAST('-' as VARCHAR(1)) || CAST(evt_index as VARCHAR(100)) || CAST('-' as VARCHAR(1)) || CAST(`from` as VARCHAR(100)) as unique_transfer_id,
        `from` as wallet_address,
        contract_address as token_address,
        evt_block_time,
        '-' || CAST(value as VARCHAR(100)) as amount_raw
        from
            {{ source('erc20_ethereum', 'evt_transfer') }}
    )

select unique_transfer_id, 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw as VARCHAR(100)) as amount_raw
from sent_transfers
union all
select unique_transfer_id, 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw as VARCHAR(100)) as amount_raw
from received_transfers
