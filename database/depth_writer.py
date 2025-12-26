"""
Async depth data writer for TimescaleDB.
Uses a queue + worker thread for non-blocking writes.
"""
import queue
import threading
import time
from typing import Dict, Any, Optional
from datetime import datetime
from logger import setup_logger

logger = setup_logger("depth_writer")

# Default settings
DEFAULT_BATCH_SIZE = 100
DEFAULT_FLUSH_INTERVAL = 1.0  # seconds


class DepthWriter:
    """
    Async writer for depth data.
    
    Uses an in-memory queue and a background worker thread
    to batch-insert records into TimescaleDB without blocking
    the WebSocket handler.
    """
    
    def __init__(
        self, 
        db, 
        batch_size: int = DEFAULT_BATCH_SIZE,
        flush_interval: float = DEFAULT_FLUSH_INTERVAL
    ):
        """
        Initialize the depth writer.
        
        Args:
            db: TimescaleDB instance
            batch_size: Number of records to batch before inserting
            flush_interval: Max seconds to wait before flushing partial batch
        """
        self.db = db
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        
        self.queue = queue.Queue()
        self.running = False
        self.worker_thread = None
        
        # Stats
        self.records_written = 0
        self.records_queued = 0
        self.batches_written = 0
        self.errors = 0
    
    def start(self):
        """Start the background worker thread."""
        if self.running:
            return
        
        self.running = True
        self.worker_thread = threading.Thread(target=self._worker, daemon=True)
        self.worker_thread.start()
        logger.info(f"DepthWriter started (batch_size={self.batch_size}, flush_interval={self.flush_interval}s)")
    
    def stop(self, timeout: float = 5.0):
        """
        Stop the worker thread gracefully.
        Flushes remaining records before stopping.
        
        Args:
            timeout: Max seconds to wait for worker to finish
        """
        if not self.running:
            return
        
        self.running = False
        
        # Wait for worker to finish
        if self.worker_thread:
            self.worker_thread.join(timeout=timeout)
        
        # Flush any remaining records
        self._flush_remaining()
        
        logger.info(f"DepthWriter stopped. Stats: {self.get_stats()}")
    
    def write(self, symbol: str, side: str, price: float, quantity: float, 
              update_id: Optional[int] = None, msg_type: str = "delta"):
        """
        Queue a depth record for writing.
        Non-blocking - returns immediately.
        
        Args:
            symbol: Trading pair (e.g., AAVEUSDT)
            side: 'bid' or 'ask'
            price: Price level
            quantity: Size at this level
            update_id: APEX sequence number
            msg_type: 'snapshot' or 'delta'
        """
        record = {
            'time': datetime.utcnow(),
            'symbol': symbol,
            'side': side,
            'price': price,
            'quantity': quantity,
            'update_id': update_id,
            'msg_type': msg_type
        }
        
        self.queue.put(record)
        self.records_queued += 1
    
    def write_batch(self, symbol: str, bids: list, asks: list, 
                    update_id: Optional[int] = None, msg_type: str = "delta"):
        """
        Queue multiple depth levels at once.
        
        Args:
            symbol: Trading pair
            bids: List of [price, quantity] pairs
            asks: List of [price, quantity] pairs
            update_id: APEX sequence number
            msg_type: 'snapshot' or 'delta'
        """
        now = datetime.utcnow()
        
        for price, qty in bids:
            self.queue.put({
                'time': now,
                'symbol': symbol,
                'side': 'bid',
                'price': float(price),
                'quantity': float(qty),
                'update_id': update_id,
                'msg_type': msg_type
            })
            self.records_queued += 1
        
        for price, qty in asks:
            self.queue.put({
                'time': now,
                'symbol': symbol,
                'side': 'ask',
                'price': float(price),
                'quantity': float(qty),
                'update_id': update_id,
                'msg_type': msg_type
            })
            self.records_queued += 1
    
    def _worker(self):
        """Background worker that batches and inserts records."""
        batch = []
        last_flush = time.time()
        
        while self.running or not self.queue.empty():
            try:
                # Get record with timeout
                try:
                    record = self.queue.get(timeout=0.1)
                    batch.append(record)
                except queue.Empty:
                    pass
                
                # Check if we should flush
                now = time.time()
                should_flush = (
                    len(batch) >= self.batch_size or
                    (batch and (now - last_flush) >= self.flush_interval)
                )
                
                if should_flush and batch:
                    self._flush_batch(batch)
                    batch = []
                    last_flush = now
                    
            except Exception as e:
                logger.error(f"Worker error: {e}")
                self.errors += 1
        
        # Flush remaining on exit
        if batch:
            self._flush_batch(batch)
    
    def _flush_batch(self, batch: list):
        """Insert a batch of records."""
        try:
            count = self.db.bulk_insert_depth(batch)
            self.records_written += count
            self.batches_written += 1
            
            if self.batches_written % 10 == 0:
                logger.debug(f"DepthWriter: {self.records_written} records written ({self.batches_written} batches)")
                
        except Exception as e:
            logger.error(f"Batch insert error: {e}")
            self.errors += 1
    
    def _flush_remaining(self):
        """Flush any records left in queue after worker stops."""
        batch = []
        while not self.queue.empty():
            try:
                batch.append(self.queue.get_nowait())
            except queue.Empty:
                break
        
        if batch:
            self._flush_batch(batch)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get writer statistics."""
        return {
            'records_queued': self.records_queued,
            'records_written': self.records_written,
            'batches_written': self.batches_written,
            'queue_size': self.queue.qsize(),
            'errors': self.errors
        }
