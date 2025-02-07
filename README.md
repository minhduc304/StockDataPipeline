# Real-Time Stock Market Data Streaming Pipeline

## Overview

This project is a robust, end-to-end data streaming solution for real-time stock market data processing. It leverages modern big data technologies to capture, stream, and persist stock market information with low latency and high reliability.

The pipeline consists of three main components:
- **Stock Data Producer**: Fetches real-time stock data using Yahoo Finance
- **Kafka Streaming**: Enables distributed message queuing and streaming
- **Spark Consumer & TimescaleDB**: Processes and stores time-series stock data

## Key Features

- Real-time stock data retrieval
- Distributed streaming with Apache Kafka
- Scalable data processing with Apache Spark
- Time-series data storage with TimescaleDB
- Supports multiple stock symbols
- Configurable polling intervals

## Prerequisites

Before you begin, ensure you have the following installed:

- Python 3.8+
- Apache Kafka
- Apache Spark (3.2.0+)
- TimescaleDB
- pip package manager

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/stock-streaming-pipeline.git
cd stock-streaming-pipeline
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
```

3. Install required dependencies:
```bash
pip install -r requirements.txt
```

### Dependencies

Install the following Python packages:
- `pyspark`
- `kafka-python`
- `yfinance`
- `psycopg2-binary`
- `pytz`

### Kafka Setup

1. Download and extract Apache Kafka:
```bash
wget https://downloads.apache.org/kafka/3.3.1/kafka_2.13-3.3.1.tgz
tar -xzf kafka_2.13-3.3.1.tgz
cd kafka_2.13-3.3.1
```

2. Start Zookeeper:
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

3. In a new terminal, start Kafka server:
```bash
bin/kafka-server-start.sh config/server.properties
```

### TimescaleDB Setup

This project uses TimescaleDB Cloud (no reason in particular, I just tried this for quicker set up):
1. Sign up for a free 30-day trial at [TimescaleDB Cloud](https://www.timescale.com/cloud/)
2. Create a new service
3. Obtain connection details (host, port, username, password). Important to make sure everything is correct, but also to wait a few minutes after resetting your password before trying to start the pipeline again.

## Configuration

This project uses environment variables for configuration, managed through a `config.py` file and `.env` support.

### Environment Variables

Create a `.env` file in the project root with the following variables:

```ini
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=stock_data

# TimescaleDB Configuration
TIMESCALE_HOST=localhost
TIMESCALE_DB=stockmarket
TIMESCALE_USER=user
TIMESCALE_PASSWORD=password
TIMESCALE_PORT=33292

# Spark Configuration
SPARK_HOME=/path/to/spark
SPARK_MASTER=local[*]
SPARK_EXECUTOR_MEMORY=4g
SPARK_DRIVER_MEMORY=4g

# Application Configuration
UPDATE_INTERVAL=60
LOG_LEVEL=INFO
```

### Configuration Details

- **Kafka**: 
  - Default bootstrap servers: `localhost:9092`
  - Default topic: `stock_data`

- **TimescaleDB**:
  - Default host: `localhost`
  - Default database: `stockmarket`
  - Configurable connection parameters

- **Spark**:
  - Configurable Spark home directory
  - Default master: `local[*]` (uses all available cores)
  - Default executor and driver memory: 4GB

- **Application**:
  - Default update interval: 60 seconds
  - Configurable log level

### Using python-dotenv

The project uses `python-dotenv` to load environment variables. Ensure you have it installed:

```bash
pip install python-dotenv
```

### Overriding Defaults

You can override these defaults by:
1. Setting environment variables
2. Modifying the `.env` file
3. Directly editing `config.py`

Precedence is in the order listed above.


## Usage

### Starting the Producer

```bash
python stock_producer.py
```

### Starting the Spark Consumer

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 spark_consumer.py
```

## Environment Variables

Set the following environment variables or use a `.env` file:
- `KAFKA_BOOTSTRAP_SERVERS`
- `TIMESCALE_HOST`
- `TIMESCALE_DB`
- `TIMESCALE_USER`
- `TIMESCALE_PASSWORD`

## Performance Considerations

- Adjust Spark and Kafka configurations based on your data volume
- Use TimescaleDB's compression and retention policies for long-term storage
- Monitor Kafka topic sizes and consumer lag

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## Troubleshooting

- Ensure all services (Kafka, Spark, TimescaleDB) are running
- Check logs for connection and data processing errors
- Verify network connectivity and firewall settings

## License

Distributed under the MIT License. See `LICENSE` for more information.

## Contact

Your Name - your.email@example.com

Project Link: [https://github.com/yourusername/stock-streaming-pipeline](https://github.com/yourusername/stock-streaming-pipeline)
