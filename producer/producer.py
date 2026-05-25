import json
import os
import time
import pandas as pd
from kafka import KafkaProducer
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CSVToKafkaProducer:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'sales-topic')
        self.csv_directory = os.getenv('CSV_DIRECTORY', '/data')
        self.producer = None

    def connect(self):
        max_retries = 30
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda v: str(v).encode('utf-8') if v else None,
                    acks='all',
                    retries=3,
                    max_block_ms=60000
                )
                logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
                return True
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    logger.error("Failed to connect to Kafka")
                    return False
        return False

    def send_csv_file(self, csv_path, delay_between_rows=0.001):
        if not self.producer:
            logger.error("Producer not connected")
            return False

        try:
            df = pd.read_csv(csv_path)
            logger.info(f"Processing {csv_path.name} with {len(df)} rows")
            sent_count = 0

            for idx, row in df.iterrows():
                message = row.to_dict()
                message = {k: (None if pd.isna(v) else v) for k, v in message.items()}

                future = self.producer.send(
                    self.topic,
                    key=idx,
                    value=message
                )

                if idx % 100 == 0:
                    future.get(timeout=10)
                    logger.info(f"Sent {idx + 1} messages from {csv_path.name}")

                sent_count += 1

                time.sleep(delay_between_rows)

            self.producer.flush()
            logger.info(f"Completed {csv_path.name}: sent {sent_count} messages")
            return sent_count

        except Exception as e:
            logger.error(f"Error processing {csv_path}: {e}")
            return 0

    def send_csv_directory(self):
        csv_files = list(Path(self.csv_directory).glob("MOCK_DATA*.csv"))

        if not csv_files:
            logger.warning(f"No CSV files found in {self.csv_directory}")
            logger.info(f"Looking for files matching pattern: MOCK_DATA*.csv")
            all_files = list(Path(self.csv_directory).glob("*.csv"))
            if all_files:
                logger.info(f"Found other CSV files: {[f.name for f in all_files]}")
                csv_files = all_files
            else:
                return False

        logger.info(f"Found {len(csv_files)} CSV files to process")
        total_messages = 0

        for csv_file in sorted(csv_files):
            logger.info(f"Processing {csv_file.name}...")
            sent = self.send_csv_file(csv_file)
            total_messages += sent
            time.sleep(1)

        logger.info(f"Total messages sent: {total_messages}")
        return True

    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")


def main():
    logger.info("=" * 60)
    logger.info("Kafka Producer Started")
    logger.info(f"Kafka Bootstrap Servers: {os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')}")
    logger.info(f"Kafka Topic: {os.getenv('KAFKA_TOPIC', 'sales-topic')}")
    logger.info(f"CSV Directory: {os.getenv('CSV_DIRECTORY', '/data')}")
    logger.info("=" * 60)

    time.sleep(5)

    producer = CSVToKafkaProducer()

    if producer.connect():
        success = producer.send_csv_directory()
        producer.close()

        if success:
            logger.info("Producer completed successfully")
        else:
            logger.error("Producer failed to send messages")
            exit(1)
    else:
        logger.error("Failed to connect to Kafka")
        exit(1)


if __name__ == "__main__":
    main()
