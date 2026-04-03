{{
    config(
        materialized='view',
        description='Cleaned realtime windowed analytics from Snowflake REALTIME_STOCK.
                     Contains Spark-computed window metrics (ma_15m, ma_1h, volatility etc.)
                     Columns are already typed correctly by the Snowflake loader.'
    )
}}

with source as (
    select * from {{ source('stream', 'REALTIME_STOCK') }}
),

cleaned as (
    select
        -- ── Keys ──────────────────────────────────────────────────────
        upper(trim(SYMBOL))                         as symbol,

        -- ✅ WINDOW_START is already TIMESTAMP_NTZ — no casting needed
        WINDOW_START                                as window_start,

        -- ── Window boundaries ─────────────────────────────────────────
        WINDOW_15M_END                              as window_15m_end,
        WINDOW_1H_END                               as window_1h_end,

        -- ── Moving averages ───────────────────────────────────────────
        round(cast(MA_15M as float), 4)             as ma_15m,
        round(cast(MA_1H  as float), 4)             as ma_1h,

        -- ── Volatility ────────────────────────────────────────────────
        round(cast(VOLATILITY_15M as float), 4)     as volatility_15m,
        round(cast(VOLATILITY_1H  as float), 4)     as volatility_1h,

        -- ── Volume ────────────────────────────────────────────────────
        cast(VOLUME_SUM_15M as bigint)              as volume_sum_15m,
        cast(VOLUME_SUM_1H  as bigint)              as volume_sum_1h,

        -- ── Metadata ──────────────────────────────────────────────────
        BATCH_ID                                    as batch_id,
        INGESTION_TIME                              as ingestion_time,

        -- ── Derived ───────────────────────────────────────────────────
        date(WINDOW_START)                          as trade_date,
        hour(WINDOW_START)                          as trade_hour,

        -- ── Audit ─────────────────────────────────────────────────────
        current_timestamp()                         as dbt_updated_at

    from source
    where
        SYMBOL       is not null
        and WINDOW_START is not null
)

select * from cleaned