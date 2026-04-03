{{
    config(
        materialized='table',
        description='Daily stock metrics — uses Spark-computed indicators from staging.
                     Adds additional window-based metrics on top of what Spark already computed.',
        unique_key=['trade_date', 'symbol']
    )
}}

with base as (
    select * from {{ ref('stg_historical_stock') }}
),

with_metrics as (
    select
        trade_date,
        symbol,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        batch_date,

        -- ── Already computed by Spark (pass through) ──────────────────
        daily_range,
        daily_return_pct,
        is_positive_day,
        sma_5,
        sma_20,

        -- ── Additional metrics computed by dbt ────────────────────────
        -- SMA 50 (not in Spark output)
        round(avg(close_price) over (
            partition by symbol
            order by trade_date
            rows between 49 preceding and current row
        ), 4)                                                       as sma_50,

        -- 20-day rolling volatility
        round(stddev(close_price) over (
            partition by symbol
            order by trade_date
            rows between 19 preceding and current row
        ), 4)                                                       as volatility_20d,

        -- 10-day average volume
        round(avg(volume) over (
            partition by symbol
            order by trade_date
            rows between 9 preceding and current row
        ), 0)                                                       as avg_volume_10d,

        -- Volume ratio vs 10-day avg
        round(volume / nullif(avg(volume) over (
            partition by symbol
            order by trade_date
            rows between 9 preceding and current row
        ), 0), 2)                                                   as volume_ratio,

        -- Previous close for momentum
        lag(close_price, 1) over (
            partition by symbol order by trade_date
        )                                                           as prev_close,

        -- ── Audit ─────────────────────────────────────────────────────
        current_timestamp()                                         as dbt_updated_at

    from base
)

select * from with_metrics