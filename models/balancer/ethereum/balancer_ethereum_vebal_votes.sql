{{
    config(
        alias='vebal_votes',
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "balancer",
                                    \'["markusbkoch", "mendesfabio", "stefenon"]\') }}'
    )
}}

WITH calendar AS (
    SELECT explode(sequence(to_date('2022-04-07'), CURRENT_DATE, INTERVAL 1 WEEK)) AS start_date
),

rounds_info AS (
    SELECT
        start_date,
        unix_timestamp(start_date) AS start_timestamp,
        date_add(start_date, 7) AS end_date,
        unix_timestamp(date_add(start_date, 7)) AS end_timestamp,
        row_number() OVER (ORDER BY start_date) AS round_id
    FROM calendar
),

double_counting AS (
    SELECT
        evt_block_time,
        block_timestamp,
        d.block_number,
        v.user AS provider,
        gauge_addr AS gauge,
        unlocked_at,
        slope,
        bias,
        weight / 1e4 AS weight
    FROM {{ source('balancer_ethereum', 'GaugeController_evt_VoteForGauge') }} AS v
    INNER JOIN {{ ref('balancer_ethereum_vebal_slopes') }} AS d
        ON
            d.wallet_address = v.user
            AND d.block_number <= v.evt_block_number
    ORDER BY v.user, evt_block_time
),

max_block_number AS (
    SELECT
        evt_block_time,
        provider,
        gauge,
        max(block_number) AS block_number
    FROM double_counting
    GROUP BY 1, 2, 3
    ORDER BY 2, 1
),

votes_info AS (
    SELECT
        a.evt_block_time,
        a.block_timestamp,
        a.provider,
        a.block_number,
        a.gauge,
        weight,
        unlocked_at,
        slope,
        bias
    FROM double_counting AS a
    INNER JOIN max_block_number AS b
        ON
            a.provider = b.provider
            AND a.gauge = b.gauge
            AND a.block_number = b.block_number
            AND a.evt_block_time = b.evt_block_time
    ORDER BY 3, 1
),

votes_with_gaps AS (
    SELECT
        *,
        LEAD(CAST(round_id AS INT), 1, 9999) OVER (PARTITION BY provider, gauge ORDER BY round_id) AS next_round
    FROM (
        SELECT
            provider,
            gauge,
            weight,
            block_timestamp,
            unlocked_at,
            slope,
            bias,
            COALESCE(round_id, 1) AS round_id
        FROM votes_info AS v
        LEFT JOIN rounds_info AS r
            ON
                v.evt_block_time >= r.start_date
                AND v.evt_block_time < r.end_date
    ) AS foo
    ORDER BY 2, 3, 1
),

running_votes AS (
    SELECT
        r.round_id,
        r.start_date,
        r.end_date,
        r.end_timestamp,
        provider,
        bias,
        slope,
        block_timestamp,
        gauge,
        weight
    FROM rounds_info AS r
    LEFT JOIN votes_with_gaps AS v
        ON
            v.round_id <= r.round_id
            AND r.round_id < v.next_round
            AND r.end_timestamp <= v.unlocked_at
    ORDER BY provider, gauge, round_id
)

SELECT
    round_id,
    start_date,
    end_date,
    gauge,
    provider,
    ((bias - slope * (end_timestamp - block_timestamp)) * weight) AS vote
FROM running_votes;
