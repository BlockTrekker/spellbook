{{ config(materialized='view', alias='erc1155') }}

with
erc1155_ids_batch as (
    select
        *,
        explode(ids) as explode_id,
        evt_tx_hash || '-' || cast(
            row_number() over (
                partition by
                    evt_tx_hash,
                    ids
                order by
                    ids
            ) as `string`
        ) as `unique_transfer_id`
    from {{ source('erc1155_ethereum', 'evt_transferbatch') }}
),

erc1155_values_batch as (
    select
        *,
        explode(
            values
        ) as explode_value,
        evt_tx_hash || '-' || cast(
            row_number() over (
                partition by
                    evt_tx_hash,
                    ids
                order by
                    ids
            ) as `string`
        ) as `unique_transfer_id`
    from {{ source('erc1155_ethereum', 'evt_transferbatch') }}
),

erc1155_transfers_batch as (
    select distinct
        erc1155_ids_batch.explode_id,
        erc1155_values_batch.explode_value,
        erc1155_ids_batch.evt_tx_hash,
        erc1155_ids_batch.to,
        erc1155_ids_batch.from,
        erc1155_ids_batch.contract_address,
        erc1155_ids_batch.evt_index,
        erc1155_ids_batch.evt_block_time
    from erc1155_ids_batch
    inner join erc1155_values_batch on erc1155_ids_batch.unique_transfer_id = erc1155_values_batch.unique_transfer_id
),

sent_transfers as (
    select
        evt_tx_hash,
        evt_tx_hash || '-' || evt_index || '-' || to as unique_tx_id,
      to as wallet_address,
      contract_address as token_address,
      evt_block_time,
      id as tokenId,
      value AS `amount`
    from {{ source('erc1155_ethereum', 'evt_transfersingle') }}
    union all
    select
        evt_tx_hash,
        evt_tx_hash || '-' || evt_index || '-' || to as unique_tx_id,
      to as wallet_address,
      contract_address as token_address,
      evt_block_time,
      explode_id as tokenId,
      explode_value AS `amount`
    from erc1155_transfers_batch
),

received_transfers as (
    select
        evt_tx_hash,
        evt_tx_hash || '-' || evt_index || '-' || to as unique_tx_id,
      from as wallet_address,
      contract_address as token_address,
      evt_block_time,
      id as tokenId,
      - value AS `amount`
    FROM {{source('erc1155_ethereum', 'evt_transfersingle')}}
    union all
    select
        evt_tx_hash,
        evt_tx_hash || '-' || evt_index || '-' || to as unique_tx_id,
      from as wallet_address,
      contract_address as token_address,
      evt_block_time,
      explode_id as tokenId,
      - explode_value AS `amount`
    FROM erc1155_transfers_batch
)

select
    'ethereum' as blockchain,
    wallet_address,
    token_address,
    evt_block_time,
    tokenid,
    amount,
    evt_tx_hash,
    unique_tx_id
from sent_transfers
union all
select
    'ethereum' as blockchain,
    wallet_address,
    token_address,
    evt_block_time,
    tokenid,
    amount,
    evt_tx_hash,
    unique_tx_id
from received_transfers
