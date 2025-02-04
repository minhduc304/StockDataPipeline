from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from typing import Optional, Dict, List
from datetime import datetime
from timescale_connector import TimeScaleDBConnector

class SparkConsumer :
    def __init__(self, app_name: str = "StockMarketPipeline"):
        self.app_name = app_name
        self.spark: Optional[SparkSession] = None
        self.schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("timestamp", DoubleType(), True),
            StructField("price", DoubleType(), True),
            StructField("volume", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("change_percent", DoubleType(), True)
        ])

        #TimescaleDB configuration
        self.timescale_config = {
            'host': 'localhost',
            'database': 'stockmarket',
            'user': 'postgres',
            'password': 'password',
            'port': 'port'
        }

    def write_to_timescaledb(self, batch_df, batch_id):
        if batch_df.isEmpty():
            return
        
        try:
            records = batch_df.collect()
            formatted_records = []

            for record in records:
                formatted_records.append({
                    'timestamp': datetime.fromtimestamp(record['timestamp']),
                    'symbol': record['record'],
                    'price': record['price'],
                    'volume': record['volume'],
                    'high': record['high'],
                    'low': record['low'],
                    'change_percent': record['change_percent']
                })

            db = TimeScaleDBConnector(**self.timescaleConfig)
            
            try:
                db.insert_batch(formatted_records)

            finally:
                db.close()
        
        except Exception as e:
            self.logger.error(f"Error writing batch {batch_id} to TimeScaleDB: {e}")



    def create_spark_session():
        """
        Create and configure a Spark session with Kafka integration.
        """
        return SparkSession.builder \
            .appName("StockMarketPipeline") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
            .getOrCreate()

    def process_stream(self, kafka_bootstrap_servers: str, topic: str):
        """
        Process streaming data from Kafka using Spark Structured Streaming.
        
        Args:
            spark: SparkSession instance
            kafka_bootstrap_servers: Kafka broker addresses
            topic: Kafka topic to consume from
        """
        if not self.spark:
            self.create_spark_session()

        # try:
        # Create streaming DataFrame from Kafka source
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", topic) \
            .load()

        # Parse JSON data from Kafka messages
        parsed_df = df.select(
            from_json(col("value").cast("string"), self.schema).alias("data")
        ).select("data.*")

        # Convert timestamp to proper format
        processed_df = parsed_df \
            .withColumn("timestamp", col("timestamp").cast(TimestampType()))

        # Start streaming query to write data to TimescaleDB
        query = processed_df.writeStream \
            .foreachBatch(self.write_to_timescaledb) \
            .outputMode("append") \
            .start()
        
        query.awaitTermination()
        
        # except Exception as e:
        #     self.logger.error(f"Stream processing error:{e}")
        #     raise

    def stop(self):
        """Stop Spark session"""   
        if self.spark:
            self.spark.stop()


