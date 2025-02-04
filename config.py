import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'stock_data')

# TimescaleDB Configuration
TIMESCALE_HOST = os.getenv('TIMESCALE_HOST', 'localhost')
TIMESCALE_DB = os.getenv('TIMESCALE_DB', 'stockmarket')
TIMESCALE_USER = os.getenv('TIMESCALE_USER', 'postgres')
TIMESCALE_PASSWORD = os.getenv('TIMESCALE_PASSWORD', 'password')
TIMESCALE_PORT = os.getenv('TIMESCALE_PORT', 'port')

# Application Configuration
DEFAULT_UPDATE_INTERVAL = int(os.getenv('UPDATE_INTERVAL', '60'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

