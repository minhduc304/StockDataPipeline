from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from typing import Optional, Dict
from timescale_connector import TimeScaleDBConnector
import logging
from pyspark.sql.functions import to_timestamp


class SparkConsumer :
    def __init__(self, host, db, user, password, port, app_name: str = "StockMarketPipeline"):
        self.logger = logging.getLogger(__name__)
        self.app_name = app_name
        self.spark: Optional[SparkSession] = None
        self.schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("timestamp", TimestampType(), False),
            StructField("price", DoubleType(), True),
            StructField("volume", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("change_percent", DoubleType(), True)
        ])

        #TimescaleDB configuration
        self.timescale_config = {
            'host': host,
            'database': db,
            'user': user,
            'password': password,
            'port': port
        }

    def write_to_timescaledb(self, batch_df, batch_id):
        if batch_df.isEmpty():
            return
        
        try:
            # Add debug logging to see the DataFrame schema and contents
            self.logger.info(f"Batch {batch_id} schema: {batch_df.schema}")
            self.logger.info(f"Sample records: {batch_df.take(1)}")

            records = batch_df.collect()
            formatted_records = []

            for record in records:
                row_dict = record.asDict()

                formatted_records.append({
                    'timestamp': row_dict['timestamp'],
                    'symbol': row_dict['symbol'],
                    'price': row_dict['price'],
                    'volume': row_dict['volume'],
                    'high': row_dict['high'],
                    'low': row_dict['low'],
                    'change_percent': row_dict['change_percent']
                })

            db = TimeScaleDBConnector(**self.timescale_config)
            
            try:
                db.insert_batch(formatted_records)

            finally:
                db.close()
        
        except Exception as e:
            self.logger.error(f"Error writing batch {batch_id} to TimeScaleDB: {str(e)}")
            self.logger.error(f"Record that caused error: {record if 'record' in locals() else 'No record available'}")
            raise


    @classmethod
    def create_spark_session(cls, app_name="StockMarketPipeline"):
        """
        Create and configure a Spark session with Kafka integration.
        """
        
        return SparkSession.builder \
            .appName(app_name) \
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
            self.spark = self.create_spark_session(self.app_name)

        try:
            # Create streaming DataFrame from Kafka source
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                .option("subscribe", topic) \
                .load()

            # Parse JSON data from Kafka messages
            parsed_df = df.select(
                from_json(col("value").cast("string"), self.schema).alias("data")
            ).select("data.*") \
                .withColumn("timestamp", to_timestamp(col("timestamp")))

            # Start streaming query to write data to TimescaleDB
            query = parsed_df.writeStream \
                .foreachBatch(self.write_to_timescaledb) \
                .outputMode("append") \
                .start()
            
            query.awaitTermination()
            
        except Exception as e:
            self.logger.error(f"Stream processing error:{e}")
            raise

    def stop(self):
        """Stop Spark session"""   
        if self.spark:
            self.spark.stop()


