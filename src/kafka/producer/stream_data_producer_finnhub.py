import json
import logging
import os
import threading
from datetime import datetime

import websocket
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger(__name__)

# Kafka Variables
KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
KAFKA_TOPIC            = os.getenv('KAFKA_TOPIC_REALTIME')

# Finnhub API Key — get free key at https://finnhub.io/register
# Add FINNHUB_API_KEY to your .env file
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')

STOCKS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN',
    'META', 'TSLA', 'NVDA', 'INTC'
]


class FinnhubStreamProducer:
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVER, topic=KAFKA_TOPIC):

        self.logger = logger
        self.topic  = topic
        self.ws     = None

        # Track latest price per symbol so we can calculate change
        self.last_price = {}

        # Kafka Producer
        try:
            self.producer = Producer({
                'bootstrap.servers': bootstrap_servers,
                'client.id': 'finnhub-stream-producer'
            })
            self.logger.info(f"Kafka producer initialized → {bootstrap_servers}")
        except Exception as e:
            self.logger.error(f"Failed to create Kafka producer: {e}")
            raise

    # ──────────────────────────────────────────────────────────────
    # Kafka helpers
    # ──────────────────────────────────────────────────────────────

    def delivery_report(self, err, msg):
        if err:
            self.logger.error(f"Delivery failed: {err}")
        else:
            self.logger.info(f"Delivered → {msg.topic()} [{msg.partition()}]")

    def send_to_kafka(self, stock_data: dict):
        try:
            self.producer.produce(
                topic=self.topic,
                key=stock_data['symbol'],
                value=json.dumps(stock_data),
                callback=self.delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            self.logger.error(f"Failed to produce message: {e}")

    # ──────────────────────────────────────────────────────────────
    # Message processing
    # ──────────────────────────────────────────────────────────────

    def process_trade(self, trade: dict) -> dict | None:
        """
        Finnhub sends trades in this format:
        {
          "data": [{"p": 178.5, "s": "AAPL", "t": 1234567890, "v": 200}],
          "type": "trade"
        }
        p = price, s = symbol, t = timestamp (ms), v = volume
        """
        symbol = trade.get('s')
        price  = trade.get('p')
        volume = trade.get('v')
        ts_ms  = trade.get('t')

        if not all([symbol, price, volume, ts_ms]):
            return None

        # Calculate change from last known price
        last   = self.last_price.get(symbol, price)
        change = round(price - last, 2)
        pct    = round((change / last) * 100, 2) if last else 0.0

        self.last_price[symbol] = price

        return {
            'symbol':         symbol,
            'price':          price,
            'change':         change,
            'percent_change': pct,
            'volume':         volume,
            # Convert Finnhub ms timestamp to ISO string
            'timestamp':      datetime.fromtimestamp(ts_ms / 1000).isoformat()
        }

    # ──────────────────────────────────────────────────────────────
    # WebSocket callbacks
    # ──────────────────────────────────────────────────────────────

    def on_message(self, ws, message):
        """Called on every message from Finnhub."""
        try:
            data = json.loads(message)

            # Finnhub sends 'trade' events and 'ping' keepalives
            if data.get('type') != 'trade':
                return

            for trade in data.get('data', []):
                stock_data = self.process_trade(trade)
                if stock_data:
                    self.logger.debug(
                        f"Trade: {stock_data['symbol']} "
                        f"@ ${stock_data['price']} "
                        f"({stock_data['percent_change']:+.2f}%)"
                    )
                    self.send_to_kafka(stock_data)

        except Exception as e:
            self.logger.error(f"Error processing message: {e}")

    def on_error(self, ws, error):
        self.logger.error(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        self.logger.info(f"WebSocket closed: {close_status_code} — {close_msg}")

    def on_open(self, ws):
        """Subscribe to all stocks once connection is open."""
        self.logger.info("WebSocket connected to Finnhub — subscribing to symbols...")
        for symbol in STOCKS:
            ws.send(json.dumps({'type': 'subscribe', 'symbol': symbol}))
            self.logger.info(f"Subscribed to {symbol}")

    # ──────────────────────────────────────────────────────────────
    # Entry point
    # ──────────────────────────────────────────────────────────────

    def start(self):
        """Connect to Finnhub WebSocket and start streaming."""
        url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

        self.ws = websocket.WebSocketApp(
            url,
            on_message = self.on_message,
            on_error   = self.on_error,
            on_close   = self.on_close,
            on_open    = self.on_open,
        )

        self.logger.info("Starting Finnhub WebSocket stream...")
        try:
            # run_forever handles reconnection automatically
            self.ws.run_forever(ping_interval=30, ping_timeout=10)
        except KeyboardInterrupt:
            self.logger.info("Producer stopped by user")
        finally:
            self.logger.info("Flushing Kafka producer...")
            self.producer.flush()
            if self.ws:
                self.ws.close()


def main():
    if not FINNHUB_API_KEY:
        raise ValueError("FINNHUB_API_KEY is not set in your .env file")

    producer = FinnhubStreamProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        topic=KAFKA_TOPIC
    )
    producer.start()


if __name__ == "__main__":
    main()