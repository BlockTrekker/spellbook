{{ config(
        schema = 'op_token_optimism',
        alias='inflation_schedule'
        , unique_key = ['schedule_confirmed_date', 'schedule_start_date']
        , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "op_token",
                                  \'["msilb7"]\') }}'
  )
}}

SELECT
    cast(schedule_confirmed_date AS date) AS schedule_confirmed_date,
    cast(schedule_start_date AS date) AS schedule_start_date,
    cast(inflation_rate AS double) AS inflation_rate,
    inflation_time_period_granularity

FROM (
    VALUES

    ('2022-05-31', '2023-05-31', 0.02, 'year') --2% initial rate -- https://community.optimism.io/docs/governance/allocations/#
-- Add rows here if this is changed by governance

)
