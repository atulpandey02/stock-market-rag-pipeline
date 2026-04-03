{{
    config(
        materialized='table',
        description='Combined performance view joining historical daily metrics
                     with latest realtime windowed analytics per symbol.
                     Uses most recent available data for each symbol.'
    )
}}

with historical as (
    select * from {{ ref('stock_daily_metrics') }}
),

realtime as (
    select * from {{ ref('stock_realtime_summary') }}
),

-- Get most recent historical record per symbol
latest_historical as (
    select *
    from historical
    qualify row_number() over (
        partition by symbol
        order by trade_date desc
    ) = 1
),

-- Get latest realtime entry per symbol
latest_realtime as (
    select *
    from realtime
    qualify row_number() over (
        partition by symbol
        order by trade_date desc, trade_hour desc, window_start desc
    ) = 1
),

combined as (
    select
        h.trade_date,
        h.symbol,

        -- ── Historical OHLCV ──────────────────────────────────────────
        h.open_price,
        h.high_price,
        h.low_price,
        h.close_price,
        h.volume                                    as daily_volume,

        -- ── Spark computed indicators ─────────────────────────────────
        h.sma_5,
        h.sma_20,
        h.daily_return_pct,
        h.daily_range,
        h.is_positive_day,

        -- ── dbt computed indicators ───────────────────────────────────
        h.sma_50,
        h.volatility_20d,
        h.volume_ratio,
        h.avg_volume_10d,
        h.prev_close,

        -- ── Price signals ─────────────────────────────────────────────
        case
            when h.sma_5 > h.sma_20 then 'BULLISH'
            when h.sma_5 < h.sma_20 then 'BEARISH'
            else 'NEUTRAL'
        end                                         as sma_signal,

        case
            when h.close_price > h.sma_20 * 1.05   then 'OVERBOUGHT'
            when h.close_price < h.sma_20 * 0.95   then 'OVERSOLD'
            else 'NORMAL'
        end                                         as price_signal,

        -- ── Realtime metrics ──────────────────────────────────────────
        r.ma_15m                                    as realtime_ma_15m,
        r.ma_1h                                     as realtime_ma_1h,
        r.volatility_15m                            as realtime_volatility,
        r.volume_sum_1h                             as realtime_volume_1h,
        r.trend_signal                              as realtime_trend,
        r.window_start                              as last_window_at,

        -- ── Overall signal ────────────────────────────────────────────
        case
            when h.sma_5 > h.sma_20
             and r.trend_signal = 'BULLISH'         then 'STRONG BUY'
            when h.sma_5 > h.sma_20
             or  r.trend_signal = 'BULLISH'         then 'BUY'
            when h.sma_5 < h.sma_20
             and r.trend_signal = 'BEARISH'         then 'STRONG SELL'
            when h.sma_5 < h.sma_20
             or  r.trend_signal = 'BEARISH'         then 'SELL'
            -- ✅ If no realtime data → use historical signal only
            when h.sma_5 > h.sma_20                 then 'BUY'
            when h.sma_5 < h.sma_20                 then 'SELL'
            else 'HOLD'
        end                                         as overall_signal,

        -- ── Audit ─────────────────────────────────────────────────────
        current_timestamp()                         as dbt_updated_at

    from latest_historical h
    left join latest_realtime r
        on h.symbol = r.symbol
)

select * from combined
order by symbol