{{ config(
        alias ='erc1155_agg_hour',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id'
        )
}}

select
    'ethereum' as blockchain,
    date_trunc('hour', evt_block_time) as hour,
    wallet_address,
    token_address,
    tokenid,
    sum(amount) as amount,
    unique_tx_id || '-' || wallet_address || '-' || token_address || tokenid || '-' || sum(amount)::string as `unique_transfer_id`
from {{ ref('transfers_ethereum_erc1155') }}
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
    where date_trunc('hour', evt_block_time) > now() - interval 2 days
{% endif %}
group by
    date_trunc('hour', evt_block_time), wallet_address, token_address, tokenId, unique_tx_id;
