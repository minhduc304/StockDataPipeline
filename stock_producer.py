from kafka import KafkaProducer
import yfinance as yf
import json
import time
import logging
from typing import List, Dict

class StockDataProducer:
    """
    A class that fetches real-time stock data and produces messages to a Kafka topic.
    """
    def __init__(self, kafka_bootstrap_servers, topic):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.topic = topic
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def get_stock_data(self, symbol):
        try:
            stock = yf.Ticker(symbol)
            data = stock.info
            return {
                'symbol': symbol,
                'timestamp': time.time(),
                'price': data.get('regularMarketPrice', 0),
                'volume': data.get('regularMarketVolume', 0),
                'high': data.get('dayHigh', 0),
                'low': data.get('dayLow', 0),
                'change_percent': data.get('regularMarketChangePercent', 0)
            }
        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return None

    def produce_messages(self, symbols, interval=60):
        while True:
            for symbol in symbols:
                data = self.get_stock_data(symbol)
                if data:
                    try:
                        self.producer.send(self.topic, value=data)
                        self.logger.info(f"Produced data for {symbol}")
                    except Exception as e:
                        self.logger.error(f"Error producing message: {str(e)}")
            time.sleep(interval)


