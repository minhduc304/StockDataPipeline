import psycopg2
from psycopg2.extras import execute_batch
import logging
from typing import List, Dict


class TimeScaleDBConnector:
    def __init__(self, host: str, database: str, user: str, password: str, port):
        """Initialize TimescaleDB connection"""
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)  
        self.conn_params = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
            'port': port
        }
        self.conn = None
        self.setup_database()

    def setup_database(self):
        """Create necessary tables and hypertables"""

        try:
            self.conn = psycopg2.connect(**self.conn_params)
            with self.conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS stock_data (
                        time TIMESTAMPTZ NOT NULL,
                        symbol TEXT NOT NULL,
                        price DOUBLE PRECISION,
                        volume DOUBLE PRECISION,
                        high DOUBLE PRECISION,
                        low DOUBLE PRECISION,
                        _52WeekChange DOUBLE PRECISION
                    );
                """)

                self.conn.commit()
            
                cur.execute("""
                    SELECT create_hypertable('stock_data', 'time', 
                            if_not_exists => TRUE);
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_stock_symbol_time 
                    ON stock_data (symbol, time DESC);
                """)

                self.conn.commit()

        except Exception as e:
            self.logger.error(f"Database setup error: {e}")
            raise
    
    def insert_batch(self, records: List[Dict]):
        """"Insert a batch of records into TimeScaleDB"""

        try:
            with self.conn.cursor() as cur:
                execute_batch(cur, """
                    INSERT INTO stock_data (
                        time, symbol, price, volume, high, low, _52WeekChange)
                    VALUES ( 
                        %(timestamp)s, %(symbol)s, %(price)s, %(volume)s, 
                        %(high)s, %(low)s, %(_52WeekChange)s)
                """, records)
                self.conn.commit()
        except Exception as e:
            self.logger.error(f"Batch insert error: {e}")
            self.conn.rollback()
            raise

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()