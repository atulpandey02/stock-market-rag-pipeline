-- Test: No negative prices in historical data
-- Fails if any close_price < 0

select
    trade_date,
    symbol,
    close_price
from {{ ref('stock_daily_metrics') }}
where close_price < 0