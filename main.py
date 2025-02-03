import stock_producer
import argparse
from multiprocessing import Process
from typing import List

def start_producer(kafka_bootstrap_servers: str, topic: str, symbols: List[str]):
   producer = stock_producer.StockDataProducer(kafka_bootstrap_servers, topic)
   producer.produce_messages(symbols)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-servers", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="stock_data", help="Kafka topic")
    parser.add_argument("--symbols", required=True, help="Comma-seperated stock symbols")

    args = parser.parse_args()
    symbols = args.symbols.split(",")

    producer_process = Process(
        target=start_producer, 
        args=(args.kafka_servers, args.topic, symbols))

    producer_process.start()

    producer_process.join()