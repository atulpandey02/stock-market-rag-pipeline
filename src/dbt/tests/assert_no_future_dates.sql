-- Test: No future trade dates

select
    trade_date,
    symbol
from {{ ref('stock_daily_metrics') }}
where trade_date > current_date()

