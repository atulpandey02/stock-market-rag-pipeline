import json
import logging
import os
import sys          # ✅ FIX: was missing
import time
from datetime import datetime
import pandas as pd
from alpha_vantage.timeseries import TimeSeries
from confluent_kafka import Producer
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
KAFKA_TOPIC_BATCH      = os.getenv('KAFKA_TOPIC_BATCH')
ALPHA_VANTAGE_API_KEY  = os.getenv('ALPHA_VANTAGE_API_KEY')
ALPHA_VANTAGE_DELAY_SECONDS = 5

STOCKS = ["AAPL","MSFT","GOOGL","AMZN","META","TSLA","NVDA","INTC","JPM","V"]


class HistoricalDataCollector:
    def __init__(self, bootstrap_server=KAFKA_BOOTSTRAP_SERVER, topic=KAFKA_TOPIC_BATCH):
        self.logger = logger
        self.topic  = topic

        if not ALPHA_VANTAGE_API_KEY:
            raise ValueError("ALPHA_VANTAGE_API_KEY is not set in your .env file")

        self.av_client = TimeSeries(key=ALPHA_VANTAGE_API_KEY, output_format='pandas')
        self.logger.info("Alpha Vantage client initialized")

        try:
            self.producer = Producer({
                "bootstrap.servers": bootstrap_server,
                "client.id": "historical-data-collector",
            })
            self.logger.info(f"Producer initialized → {bootstrap_server}")
        except Exception as e:
            self.logger.error(f"Failed to create Kafka Producer: {e}")
            raise

    def fetch_historical_data(self, symbol: str, period: str = "6mo") -> Optional[pd.DataFrame]:
        try:
            self.logger.info(f"Fetching {symbol} from Alpha Vantage")
            df, meta = self.av_client.get_daily(symbol=symbol, outputsize='compact')

            if df is None or df.empty:
                self.logger.warning(f"No data for {symbol}")
                return None

            df = df.rename(columns={
                '1. open': 'open', '2. high': 'high',
                '3. low':  'low',  '4. close': 'close', '5. volume': 'volume'
            })
            df.index.name = 'date'
            df = df.reset_index()
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            df = self._filter_by_period(df, period)
            df['symbol'] = symbol
            df = df[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            self.logger.info(f"Fetched {len(df)} days for {symbol}")
            return df

        except Exception as e:
            self.logger.error(f"Failed to fetch {symbol}: {e}")
            return None

    def _filter_by_period(self, df: pd.DataFrame, period: str) -> pd.DataFrame:
        period_map = {'1mo':30,'3mo':90,'6mo':180,'1y':365,'2y':730,'5y':1825}
        days   = period_map.get(period, 365)
        cutoff = (datetime.now() - pd.Timedelta(days=days)).strftime('%Y-%m-%d')
        return df[df['date'] >= cutoff].reset_index(drop=True)

    def delivery_report(self, err, msg):
        if err:
            self.logger.error(f"Delivery failed: {msg.key()}")
        else:
            self.logger.info(f"Delivered → {msg.topic()} [{msg.partition()}]")

    def producer_to_kafka(self, df: pd.DataFrame, symbol: str):
        batch_id   = datetime.now().strftime("%Y%m%d%H%M%S")
        df['batch_id']   = batch_id
        df['batch_date'] = datetime.now().strftime("%Y-%m-%d")

        ok = fail = 0
        for record in df.to_dict(orient="records"):
            try:
                self.producer.produce(
                    topic=self.topic, key=symbol,
                    value=json.dumps(record),
                    callback=self.delivery_report
                )
                self.producer.poll(0)
                ok += 1
            except Exception as e:
                self.logger.error(f"Produce failed for {symbol}: {e}")
                fail += 1

        self.producer.flush()
        self.logger.info(f"Produced {ok} records for {symbol}, failed: {fail}")

    def collect_historical_data(self, period: str = "6mo"):
        self.logger.info(f"Starting collection for {len(STOCKS)} symbols")
        ok = fail = 0

        for i, symbol in enumerate(STOCKS):
            try:
                df = self.fetch_historical_data(symbol, period)
                if df is not None and not df.empty:
                    self.producer_to_kafka(df, symbol)
                    ok += 1
                else:
                    self.logger.warning(f"No data for {symbol}")
                    fail += 1
            except Exception as e:
                self.logger.error(f"Error processing {symbol}: {e}")
                fail += 1

            if i < len(STOCKS) - 1:
                time.sleep(ALPHA_VANTAGE_DELAY_SECONDS)

        self.logger.info(f"Completed. Successful: {ok}, Failed: {fail}")


def main():
    try:
        logger.info("Starting Historical Stock Data Collector")
        collector = HistoricalDataCollector(
            bootstrap_server=KAFKA_BOOTSTRAP_SERVER,
            topic=KAFKA_TOPIC_BATCH
        )
        collector.collect_historical_data(period="6mo")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)  # ✅ sys now imported


if __name__ == "__main__":
    main()