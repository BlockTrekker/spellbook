{{ config(
        alias ='test_incremental_table',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id'
        )
}}

select
    'ethereum' as blockchain,
    date_trunc('day', evt_block_time) as day,
    wallet_address,
    token_address,
    tokenid,
    sum(amount) as amount,
    unique_tx_id || '-' || wallet_address || '-' || token_address || tokenid || '-' || sum(amount)::string as unique_transfer_id
from {{ ref('test_view') }}
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
    where date_trunc('day', evt_block_time) > now() - interval 2 days
{% endif %}
group by
    date_trunc('day', evt_block_time), wallet_address, token_address, tokenId, unique_tx_id