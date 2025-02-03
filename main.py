import stock_producer
import spark_consumer
import timescale_connector
import argparse
from multiprocessing import Process
from typing import List

def start_producer(kafka_bootstrap_servers: str, topic: str, symbols: List[str]):
    producer = stock_producer.StockDataProducer(kafka_bootstrap_servers, topic)
    producer.produce_messages(symbols)

def start_consumer(kafka_bootstrap_servers: str, topic: str):
    spark = spark_consumer.SparkConsumer()
    spark.process_stream(kafka_bootstrap_servers, topic)


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
    
    consumer_process = Process(
        target=start_consumer, 
        args=(args.kafka_servers, args.topic))

    producer_process.start()
    consumer_process.start()

    producer_process.join()
    consumer_process.join()