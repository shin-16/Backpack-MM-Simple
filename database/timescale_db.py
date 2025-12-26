"""
TimescaleDB connection and schema management for depth data storage.
"""
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
from typing import List, Dict, Any, Optional
from datetime import datetime
from logger import setup_logger

logger = setup_logger("timescale_db")

# Default connection URL
DEFAULT_DB_URL = "postgresql://postgres:password@localhost:5432/apex"


class TimescaleDB:
    """TimescaleDB connection manager with connection pooling."""
    
    def __init__(self, db_url: str = DEFAULT_DB_URL, min_conn: int = 1, max_conn: int = 5):
        """
        Initialize TimescaleDB connection pool.
        
        Args:
            db_url: PostgreSQL connection URL
            min_conn: Minimum connections in pool
            max_conn: Maximum connections in pool
        """
        self.db_url = db_url
        self.pool = None
        self._connect(min_conn, max_conn)
    
    def _connect(self, min_conn: int, max_conn: int):
        """Establish connection pool."""
        try:
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                min_conn, max_conn, self.db_url
            )
            logger.info(f"TimescaleDB connection pool created (min={min_conn}, max={max_conn})")
        except Exception as e:
            logger.error(f"Failed to connect to TimescaleDB: {e}")
            raise
    
    def get_conn(self):
        """Get a connection from the pool."""
        return self.pool.getconn()
    
    def put_conn(self, conn):
        """Return a connection to the pool."""
        self.pool.putconn(conn)
    
    def init_schema(self):
        """
        Initialize the database schema for depth data.
        Creates hypertable and compression policy.
        """
        conn = None
        try:
            conn = self.get_conn()
            cursor = conn.cursor()
            
            # Create depth_updates table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS depth_updates (
                    time         TIMESTAMPTZ NOT NULL,
                    symbol       TEXT NOT NULL,
                    side         TEXT NOT NULL,
                    price        NUMERIC NOT NULL,
                    quantity     NUMERIC NOT NULL,
                    update_id    BIGINT,
                    msg_type     TEXT
                );
            """)
            
            # Convert to hypertable (ignore error if already exists)
            try:
                cursor.execute("""
                    SELECT create_hypertable('depth_updates', 'time', if_not_exists => TRUE);
                """)
            except psycopg2.errors.InvalidTableDefinition:
                # Already a hypertable
                pass
            
            # Create indexes for efficient backtesting queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_depth_symbol_time 
                ON depth_updates (symbol, time DESC);
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_depth_symbol_side_time 
                ON depth_updates (symbol, side, time DESC);
            """)
            
            # Enable compression (after 7 days)
            try:
                cursor.execute("""
                    ALTER TABLE depth_updates SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'symbol,side'
                    );
                """)
                cursor.execute("""
                    SELECT add_compression_policy('depth_updates', INTERVAL '7 days', if_not_exists => TRUE);
                """)
            except Exception as e:
                logger.warning(f"Compression policy may already exist: {e}")
            
            conn.commit()
            logger.info("TimescaleDB schema initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize schema: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.put_conn(conn)
    
    def bulk_insert_depth(self, records: List[Dict[str, Any]]) -> int:
        """
        Bulk insert depth records.
        
        Args:
            records: List of depth records with keys:
                     time, symbol, side, price, quantity, update_id, msg_type
        
        Returns:
            Number of records inserted
        """
        if not records:
            return 0
        
        conn = None
        try:
            conn = self.get_conn()
            cursor = conn.cursor()
            
            # Prepare data for execute_values
            values = [
                (
                    r.get('time', datetime.utcnow()),
                    r['symbol'],
                    r['side'],
                    r['price'],
                    r['quantity'],
                    r.get('update_id'),
                    r.get('msg_type', 'delta')
                )
                for r in records
            ]
            
            execute_values(
                cursor,
                """
                INSERT INTO depth_updates (time, symbol, side, price, quantity, update_id, msg_type)
                VALUES %s
                """,
                values
            )
            
            conn.commit()
            return len(records)
            
        except Exception as e:
            logger.error(f"Bulk insert failed: {e}")
            if conn:
                conn.rollback()
            return 0
        finally:
            if conn:
                self.put_conn(conn)
    
    def close(self):
        """Close all connections in the pool."""
        if self.pool:
            self.pool.closeall()
            logger.info("TimescaleDB connection pool closed")
