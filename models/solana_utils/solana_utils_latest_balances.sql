{{
  config(
        alias='latest_balances',
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy='merge',
        unique_key = ['token_mint_address', 'address'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

WITH
updated_balances AS (
    SELECT
        address,
        day,
        sol_balance,
        token_mint_address,
        token_balance,
        token_balance_owner,
        row_number() OVER (PARTITION BY address ORDER BY day DESC) AS latest_balance
    FROM {{ ref('solana_utils_daily_balances') }}
    {% if is_incremental() %}
        WHERE day >= date_trunc("day", now() - interval "1 day")
    {% endif %}
)

SELECT
    address,
    sol_balance,
    token_balance,
    token_mint_address,
    token_balance_owner,
    now() AS updated_at
FROM updated_balances
WHERE latest_balance = 1
