import json
import logging
import os
import time
from datetime import datetime
import pandas as pd
from alpha_vantage.timeseries import TimeSeries
from confluent_kafka import Producer
from dotenv import load_dotenv
from typing import Optional

# Load Env Variable 
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
)

logger = logging.getLogger(__name__)

# Kafka Variables
KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
KAFKA_TOPIC_BATCH = os.getenv('KAFKA_TOPIC_BATCH')

# Alpha Vantage API Key — add ALPHA_VANTAGE_API_KEY to your .env file
# Get a free key at: https://www.alphavantage.co/support/#api-key
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')

# Free tier limits: 25 calls/day, 5 calls/minute
# 13 seconds between calls keeps you safely under the per-minute limit
ALPHA_VANTAGE_DELAY_SECONDS = 5

# Define stocks to collect for historical data
STOCKS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "META",
    "TSLA",
    "NVDA",
    "INTC",
    "JPM",
    "V"
]


class HistoricalDataCollector:
    def __init__(self, bootstrap_server=KAFKA_BOOTSTRAP_SERVER, topic=KAFKA_TOPIC_BATCH):

        self.logger = logger
        self.topic = topic

        # Alpha Vantage client
        if not ALPHA_VANTAGE_API_KEY:
            raise ValueError("ALPHA_VANTAGE_API_KEY is not set in your .env file")

        self.av_client = TimeSeries(key=ALPHA_VANTAGE_API_KEY, output_format='pandas')
        self.logger.info("Alpha Vantage client initialized")

        # Create Kafka Producer instance
        producer_config = {
            "bootstrap.servers": bootstrap_server,
            "client.id": "historical-data-collector",
        }

        try:
            self.producer = Producer(producer_config)
            self.logger.info(f"Producer initialized. Sending to: {bootstrap_server}")
        except Exception as e:
            self.logger.error(f"Failed to create Kafka Producer: {e}")
            raise

    def fetch_historical_data(self, symbol: str, period: str = "6mo") -> Optional[pd.DataFrame]:
        """
        Fetch historical daily data from Alpha Vantage.
        Alpha Vantage returns 'full' (20+ years) or 'compact' (100 days) outputsize.
        We map your 'period' param to the right outputsize.
        """
        try:
            self.logger.info(f"Fetching historical data for {symbol} from Alpha Vantage")

            # Use 'full' for any period longer than 100 days, else 'compact'
            outputsize = 'compact'

            df, meta = self.av_client.get_daily(symbol=symbol, outputsize=outputsize)

            if df is None or df.empty:
                self.logger.warning(f"No data returned for {symbol}")
                return None

            # Alpha Vantage returns columns: '1. open', '2. high', '3. low', '4. close', '5. volume'
            df = df.rename(columns={
                '1. open':   'open',
                '2. high':   'high',
                '3. low':    'low',
                '4. close':  'close',
                '5. volume': 'volume'
            })

            # Index is already a DatetimeIndex — convert to string column
            df.index.name = 'date'
            df = df.reset_index()
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

            # Filter rows to match the requested period
            df = self._filter_by_period(df, period)

            df['symbol'] = symbol
            df = df[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']]

            # Ensure numeric types
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            self.logger.info(f"Successfully fetched {len(df)} days of data for {symbol}")
            return df

        except Exception as e:
            self.logger.error(f"Failed to fetch historical data for {symbol}: {e}")
            return None

    def _filter_by_period(self, df: pd.DataFrame, period: str) -> pd.DataFrame:
        """Filter DataFrame rows to match a period string (e.g. '1y', '6mo')."""
        period_map = {
            '1mo':  30,
            '3mo':  90,
            '6mo':  180,
            '1y':   365,
            '2y':   730,
            '5y':   1825,
        }
        days = period_map.get(period, 365)
        cutoff = (datetime.now() - pd.Timedelta(days=days)).strftime('%Y-%m-%d')
        return df[df['date'] >= cutoff].reset_index(drop=True)

    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Delivery failed for message: {msg.key()}")
        else:
            self.logger.info(f"Message delivered to topic {msg.topic()} [{msg.partition()}]")

    def producer_to_kafka(self, df: pd.DataFrame, symbol: str):

        batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
        df['batch_id'] = batch_id
        df['batch_date'] = datetime.now().strftime("%Y-%m-%d")

        records = df.to_dict(orient="records")

        successful_records = 0
        failed_records = 0

        for record in records:
            try:
                data = json.dumps(record)
                self.producer.produce(
                    topic=self.topic,
                    key=symbol,
                    value=data,
                    callback=self.delivery_report
                )
                self.producer.poll(0)
                successful_records += 1

            except Exception as e:
                self.logger.error(f"Failed to produce message for {symbol}: {e}")
                failed_records += 1

        self.producer.flush()
        self.logger.info(
            f"Produced {successful_records} records for {symbol}, failed: {failed_records}"
        )

    def collect_historical_data(self, period: str = "1y"):
        symbols = STOCKS

        self.logger.info(
            f"Starting historical data collection for {len(symbols)} symbols "
            f"(Alpha Vantage free tier: 25 calls/day, delay={ALPHA_VANTAGE_DELAY_SECONDS}s)"
        )

        successful_symbols = 0
        failed_symbols = 0

        for i, symbol in enumerate(symbols):
            try:
                df = self.fetch_historical_data(symbol, period)

                if df is not None and not df.empty:
                    self.producer_to_kafka(df, symbol)
                    successful_symbols += 1
                else:
                    self.logger.warning(f"No data returned for {symbol}")
                    failed_symbols += 1

            except Exception as e:
                self.logger.error(f"Error processing {symbol}: {e}")
                failed_symbols += 1

            # Respect Alpha Vantage rate limit (5 calls/min on free tier)
            # Skip the delay after the last symbol
            if i < len(symbols) - 1:
                self.logger.debug(
                    f"Waiting {ALPHA_VANTAGE_DELAY_SECONDS}s before next request "
                    f"({i + 1}/{len(symbols)} done)"
                )
                time.sleep(ALPHA_VANTAGE_DELAY_SECONDS)

        self.logger.info(
            f"Historical data collection completed. "
            f"Successful: {successful_symbols}, Failed: {failed_symbols}"
        )


def main():
    try:
        logger.info("Starting Historical Stock Data Collector")

        collector = HistoricalDataCollector(
            bootstrap_server=KAFKA_BOOTSTRAP_SERVER,
            topic=KAFKA_TOPIC_BATCH
        )

        collector.collect_historical_data(period="1y")

    except Exception as e:
        logger.error(f"Fatal error: {e}")


if __name__ == "__main__":
    main()