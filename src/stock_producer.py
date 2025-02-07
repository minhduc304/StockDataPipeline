from kafka import KafkaProducer
import yfinance as yf
import json
import logging
from typing import List, Dict
import pytz
from datetime import datetime
import time

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

    def get_stock_data(self, symbol: str) -> Dict:
        try:
            stock = yf.Ticker(symbol)

             # Some more comprehensive logging
            self.logger.info(f"Fetching data for {symbol}")
                        
            current_timestamp = datetime.now(pytz.UTC).isoformat()

            return {
                'symbol': symbol,
                'timestamp': current_timestamp,
                'price': stock.fast_info.last_price,
                'volume': stock.fast_info.last_volume,
                'high': stock.info.get('dayHigh'),
                'low': stock.info.get('dayLow'),
                '52week_change': stock.get_info().get('52WeekChange')
            }
        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return None

    def produce_messages(self, symbols: List[str], interval: int = 60):
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


