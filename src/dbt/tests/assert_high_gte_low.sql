-- Test: High price must be >= Low price

select
    trade_date,
    symbol,
    high_price,
    low_price
from {{ ref('stg_historical_stock') }}
where high_price < low_price