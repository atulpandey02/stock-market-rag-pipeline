{{
    config(
        materialized='view',
        description='Cleaned historical OHLCV stock data from Snowflake HISTORICAL_STOCK table.
                     Columns already renamed by load_to_snowflake.py (open→open_price etc.)
                     and Spark-computed metrics already present (sma_5, sma_20, daily_range etc.)'
    )
}}

with source as (
    select * from {{ source('batch', 'HISTORICAL_STOCK') }}
),

cleaned as (
    select
        -- ── Keys ──────────────────────────────────────────────────────
        to_date(DATE)                               as trade_date,
        upper(trim(SYMBOL))                         as symbol,

        -- ── Prices (already renamed by loader) ────────────────────────
        round(try_to_double(OPEN_PRICE),  4)        as open_price,
        round(try_to_double(HIGH_PRICE),  4)        as high_price,
        round(try_to_double(LOW_PRICE),   4)        as low_price,
        round(try_to_double(CLOSE_PRICE), 4)        as close_price,

        -- ── Volume ────────────────────────────────────────────────────
        try_to_number(VOLUME)                       as volume,

        -- ── Spark-computed metrics (already in Snowflake) ─────────────
        round(try_to_double(DAILY_RANGE), 4)        as daily_range,
        round(try_to_double(DAILY_RETURN_PCT), 4)   as daily_return_pct,
        IS_POSITIVE_DAY                             as is_positive_day,
        round(try_to_double(SMA_5), 4)              as sma_5,
        round(try_to_double(SMA_20), 4)             as sma_20,

        -- ── Metadata ──────────────────────────────────────────────────
        to_date(BATCH_DATE)                         as batch_date,

        -- ── Audit ─────────────────────────────────────────────────────
        current_timestamp()                         as dbt_updated_at

    from source
    where
        DATE    is not null
        and SYMBOL  is not null
        and try_to_double(CLOSE_PRICE) > 0
)

select * from cleaned