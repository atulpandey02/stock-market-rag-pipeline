import io
import json
import logging
import os
import time
from datetime import datetime
from io import StringIO

import pandas as pd
from confluent_kafka import Consumer
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
KAFKA_TOPIC_REALTIME   = os.getenv('KAFKA_TOPIC_REALTIME')
KAFKA_GROUP_ID         = os.getenv('KAFKA_GROUP_REALTIME_ID')

MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET     = os.getenv('MINIO_BUCKET')
MINIO_ENDPOINT   = "localhost:9000"

DEFAULT_BATCH_SIZE = 100
FLUSH_INTERVAL     = 60  # seconds


def create_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )


def ensure_bucket_exists(minio_client, bucket_name):
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logger.info(f"Created bucket: {bucket_name}")
        else:
            logger.info(f"Bucket already exists: {bucket_name}")
    except S3Error as e:
        logger.error(f"Error ensuring bucket {bucket_name}: {e}")
        raise


def flush_to_minio(minio_client, messages: list) -> bool:
    try:
        df  = pd.DataFrame(messages)
        now = datetime.now()

        # Write to year=/month=/day/ — NO hour= level
        # Spark watches today's exact path — bypasses old conflicting folders
        timestamp   = now.strftime("%H%M%S%f")
        object_name = (
            f"raw/realtime/"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            f"stock_data_{timestamp}.csv"
        )

        csv_buffer  = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes   = csv_buffer.getvalue().encode("utf-8")
        data_stream = io.BytesIO(csv_bytes)

        minio_client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
            data=data_stream,
            length=len(csv_bytes),
            content_type="text/csv",
        )

        logger.info(f"Wrote {len(messages)} records → s3://{MINIO_BUCKET}/{object_name}")
        return True

    except Exception as e:
        logger.error(f"Failed to write to MinIO: {e}")
        return False


def main():
    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client, MINIO_BUCKET)

    conf = {
        'bootstrap.servers':  KAFKA_BOOTSTRAP_SERVER,
        'group.id':           KAFKA_GROUP_ID,
        'auto.offset.reset':  'latest',
        'enable.auto.commit': False,
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC_REALTIME])
    logger.info(f"Consumer started on topic: {KAFKA_TOPIC_REALTIME}")

    messages   = []
    flush_time = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                current_time = time.time()
                if (current_time - flush_time >= FLUSH_INTERVAL and messages):
                    logger.info(f"Time-based flush: {len(messages)} messages")
                    if flush_to_minio(minio_client, messages):
                        messages   = []
                        flush_time = time.time()
                        consumer.commit()
                continue

            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                value = json.loads(msg.value().decode("utf-8"))
                messages.append(value)

                if len(messages) % 10 == 0:
                    logger.info(f"Buffered {len(messages)} messages in current batch")

                current_time = time.time()
                size_trigger = len(messages) >= DEFAULT_BATCH_SIZE
                time_trigger = (current_time - flush_time >= FLUSH_INTERVAL
                                and len(messages) > 0)

                if size_trigger or time_trigger:
                    trigger = "size" if size_trigger else "time"
                    logger.info(f"Flushing {len(messages)} messages ({trigger} trigger)")

                    if flush_to_minio(minio_client, messages):
                        messages   = []
                        flush_time = time.time()
                        consumer.commit()
                    else:
                        logger.warning("Flush failed, will retry on next trigger")
                        flush_time = time.time()

            except Exception as e:
                logger.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logger.info("Stopping consumer — flushing remaining messages…")
        if messages:
            if flush_to_minio(minio_client, messages):
                consumer.commit()
    finally:
        consumer.close()
        logger.info("Consumer closed.")


if __name__ == "__main__":
    main()