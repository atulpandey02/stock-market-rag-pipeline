{{
    config(
        materialized='table',
        description='Hourly summary of realtime windowed metrics per symbol.
                     Source is already windowed by Spark — this adds
                     cross-window comparisons and trend signals.',
        unique_key=['trade_date', 'trade_hour', 'symbol']
    )
}}

with base as (
    select * from {{ ref('stg_realtime_stock') }}
),

-- Get latest window per symbol per hour
latest_per_hour as (
    select *
    from base
    qualify row_number() over (
        partition by symbol, trade_date, trade_hour
        order by window_start desc
    ) = 1
),

summary as (
    select
        trade_date,
        trade_hour,
        symbol,

        -- ── Latest window metrics ──────────────────────────────────────
        window_start,
        window_15m_end,
        window_1h_end,

        -- ── Moving averages ───────────────────────────────────────────
        ma_15m,
        ma_1h,

        -- ── Trend signal: 15min vs 1hr MA ─────────────────────────────
        case
            when ma_15m > ma_1h then 'BULLISH'
            when ma_15m < ma_1h then 'BEARISH'
            else 'NEUTRAL'
        end                                         as trend_signal,

        -- ── Volatility ────────────────────────────────────────────────
        volatility_15m,
        volatility_1h,

        -- ── Volume ────────────────────────────────────────────────────
        volume_sum_15m,
        volume_sum_1h,

        -- ── Metadata ──────────────────────────────────────────────────
        batch_id,
        ingestion_time,

        -- ── Audit ─────────────────────────────────────────────────────
        current_timestamp()                         as dbt_updated_at

    from latest_per_hour
)

select * from summary