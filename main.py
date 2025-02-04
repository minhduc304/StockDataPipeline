import logging
from multiprocessing import Process
import sys
import os

# Add the current directory to Python path to import local modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import local modules
from config import (
    KAFKA_BOOTSTRAP_SERVERS, 
    KAFKA_TOPIC, 
    TIMESCALE_HOST, 
    TIMESCALE_DB, 
    TIMESCALE_USER, 
    TIMESCALE_PASSWORD, 
    TIMESCALE_PORT,
    DEFAULT_UPDATE_INTERVAL, 
    LOG_LEVEL
)
from stock_producer import StockDataProducer
from spark_consumer import SparkConsumer
from timescale_connector import TimeScaleDBConnector

def setup_logging():
    """Configure logging for the application"""
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def initialize_timescale_db():
    """Initialize TimescaleDB with the required schema"""
    try:
        db_connector = TimeScaleDBConnector(
            host=TIMESCALE_HOST, 
            database=TIMESCALE_DB, 
            user=TIMESCALE_USER, 
            password=TIMESCALE_PASSWORD, 
            port=TIMESCALE_PORT
        )
        logging.info("TimescaleDB initialized successfully")
        return db_connector
    except Exception as e:
        logging.error(f"Failed to initialize TimescaleDB: {e}")
        raise

def run_producer(symbols):
    """Run Kafka producer process"""
    try:
        producer = StockDataProducer(
            kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, 
            topic=KAFKA_TOPIC
        )
        producer.produce_messages(
            symbols=symbols, 
            interval=DEFAULT_UPDATE_INTERVAL
        )
    except Exception as e:
        logging.error(f"Producer process failed: {e}")
        raise

def run_consumer():
    """Run Spark Streaming consumer process"""
    try:
        consumer = SparkConsumer()
        consumer.process_stream(
            kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, 
            topic=KAFKA_TOPIC
        )
    except Exception as e:
        logging.error(f"Consumer process failed: {e}")
        raise

def main():
    """Main entry point for the stock market data pipeline"""
    # Configure logging
    setup_logging()
    
    # List of stock symbols to track
    stock_symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
    
    # try:
        # Initialize TimescaleDB
    initialize_timescale_db()
    
    # Create multiprocessing processes
    producer_process = Process(target=run_producer, args=(stock_symbols,))
    consumer_process = Process(target=run_consumer)
    
    # Start processes
    producer_process.start()
    consumer_process.start()
    
    # Wait for processes to complete
    producer_process.join()
    consumer_process.join()
    
    # except Exception as e:
    #     logging.error(f"Pipeline initialization failed: {e}")
    #     sys.exit(1)

if __name__ == "__main__":
    main()