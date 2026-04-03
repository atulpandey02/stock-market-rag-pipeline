-- Test: Only expected stock symbols present

select distinct symbol
from {{ ref('stock_daily_metrics') }}
where symbol not in (
    'AAPL', 'MSFT', 'GOOGL', 'AMZN',
    'META', 'TSLA', 'NVDA', 'INTC', 'JPM', 'V'
)