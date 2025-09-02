#!/usr/bin/env python3
"""
Iceberg Storage Service
Separate process that handles PyIceberg writes via Redis queue.
This avoids SIGSEGV issues by keeping PyIceberg away from multiprocessing.

Run with: python3 iceberg_storage_service.py
"""

import redis
import json
import base64
import logging
import time
import random
import signal
import sys
from datetime import datetime
from typing import Dict, Any
import pyarrow as pa
import pyarrow.ipc
import io

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - Iceberg Service - %(message)s'
)
logger = logging.getLogger(__name__)

class IcebergStorageService:
    def __init__(self):
        self.running = True
        self.redis_client = None
        self.iceberg_initialized = False
        
        # Force accumulation of exactly 5 batches before writing (no time limit)
        self.batch_size = 10  # Exactly 5 batches before writing
        self.batch_timeout = None  # No timeout - always wait for full batch
        self.max_batch_size = 10  # Fixed batch size
        self.pending_batch = []
        self.last_batch_time = time.time()
        self._session_completed = False  # Track session completion
        self._current_session_batches = {}  # Track batches per session
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
        
    def initialize_redis(self):
        """Initialize Redis connection with optimized settings"""
        try:
            # Connection pool for better performance
            pool = redis.ConnectionPool(
                host='localhost', 
                port=6379, 
                db=0, 
                decode_responses=False,
                max_connections=20,  # Connection pooling
                socket_keepalive=True,  # Keep connections alive
                socket_keepalive_options={},
                retry_on_timeout=True
            )
            self.redis_client = redis.Redis(connection_pool=pool)
            self.redis_client.ping()
            logger.info("✅ Connected to Redis with connection pooling")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            return False
            
    def initialize_iceberg(self):
        """Initialize Iceberg catalog once at startup"""
        try:
            import iceberg_handler as ih
            
            logger.info("🔧 Initializing Iceberg catalog...")
            success = ih.initialize_iceberg_catalog()
            if success and ih.iceberg_catalog:
                self.iceberg_initialized = True
                logger.info("✅ Iceberg catalog initialized successfully")
                return True
            else:
                logger.error("❌ Failed to initialize Iceberg catalog")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error initializing Iceberg: {e}")
            return False
    
    def deserialize_table(self, table_data: str) -> pa.Table:
        """Deserialize PyArrow table from base64 string"""
        try:
            # Decode base64 to bytes
            serialized_bytes = base64.b64decode(table_data.encode('utf-8'))
            
            # Deserialize using Arrow IPC format
            buffer = io.BytesIO(serialized_bytes)
            with pa.ipc.open_stream(buffer) as reader:
                table = reader.read_all()
                
            return table
            
        except Exception as e:
            logger.error(f"❌ Failed to deserialize table: {e}")
            raise
    
    def write_batch_to_iceberg(self, tables: list) -> bool:
        """Write multiple PyArrow tables to Iceberg in one operation (much faster)"""
        import iceberg_handler as ih
        
        if not self.iceberg_initialized or not ih.iceberg_catalog:
            logger.error("❌ Iceberg not initialized")
            return False
            
        if not tables:
            return True
            
        try:
            # Concatenate all tables into one big table for batch write
            combined_table = pa.concat_tables(tables)
            total_rows = len(combined_table)
            
            logger.info(f"📝 Writing batch of {len(tables)} tables ({total_rows:,} rows) to Iceberg...")
            
            retries = 3
            retry_delay = 1
            
            for attempt in range(retries + 1):
                try:
                    # Load the existing Iceberg table
                    iceberg_table = ih.iceberg_catalog.load_table("default.batch_statistics")
                    
                    # Single batch write (much faster than individual writes)
                    iceberg_table.append(combined_table)
                    
                    logger.info(f"✅ Batch write successful: {len(tables)} batches, {total_rows:,} rows (attempt {attempt + 1})")
                    return True
                    
                except Exception as e:
                    error_msg = str(e)
                    
                    # Check for concurrent write conflicts and retry
                    is_concurrent_conflict = (
                        "branch main has changed" in error_msg or
                        "expected id" in error_msg or
                        "Table has been updated by another process" in error_msg
                    )
                    
                    if is_concurrent_conflict and attempt < retries:
                        jitter = random.uniform(0, 0.5)
                        sleep_time = retry_delay * (2 ** attempt) + jitter
                        logger.warning(f"🔄 Batch write conflict (attempt {attempt + 1}/{retries + 1}). Retrying in {sleep_time:.2f}s...")
                        time.sleep(sleep_time)
                        continue
                    else:
                        logger.error(f"❌ Batch write failed: {error_msg}")
                        return False
                        
        except Exception as e:
            logger.error(f"❌ Error preparing batch write: {e}")
            return False
            
        return False
    
    def should_write_batch(self) -> bool:
        """Check if we should write the current batch - FORCE exactly 5 batches, no timeout"""
        if not self.pending_batch:
            return False
            
        batch_count = len(self.pending_batch)
        
        # Check if we should write based on batch count or if all batches for session are received
        if batch_count >= self.batch_size:
            logger.info(f"📦 Force writing batch: {batch_count}/{self.batch_size} batches accumulated")
            return True
        else:
            # Check if this is the final batch write (all batches for session received but < 5)
            if hasattr(self, '_session_completed') and self._session_completed:
                logger.info(f"📦 Writing final batch: {batch_count} batches (session completed)")
                return True
            
            # Log waiting status every 2nd accumulation to show progress
            if batch_count % 2 == 0:
                logger.info(f"⏳ Waiting for full batch: {batch_count}/{self.batch_size} accumulated...")
            return False
    
    def check_session_completion(self) -> bool:
        """Check if current session is completed (all batches processed)"""
        if not self.pending_batch:
            return False
            
        try:
            # Extract session ID from batch IDs in pending batch
            current_session_ids = set()
            for batch_item in self.pending_batch:
                batch_id = batch_item['batch_id']
                # Convert to string if it's not already (defensive programming)
                batch_id_str = str(batch_id) if not isinstance(batch_id, str) else batch_id
                
                # Extract session ID from batch ID (format: session_xxx_batch_yyy)
                if '_batch_' in batch_id_str:
                    session_id = batch_id_str.split('_batch_')[0]
                    current_session_ids.add(session_id)
            
            # Check each session for completion, but prioritize active sessions
            for session_id in current_session_ids:
                try:
                    # Get session metadata from Redis
                    session_meta_key = f"session_meta_{session_id}"
                    session_meta = self.redis_client.hgetall(session_meta_key)
                    
                    if not session_meta or b'total_batches' not in session_meta:
                        logger.warning(f"🔍 Session {session_id} has no metadata - cleaning up old batches")
                        self._clean_old_session_batches(session_id)
                        continue
                        
                    total_batches = int(session_meta[b'total_batches'].decode())
                    
                    # Check if session is already marked as ended
                    has_end_time = b'end_time' in session_meta
                    if has_end_time:
                        logger.info(f"🔍 Session {session_id} already ended - cleaning up remaining batches")
                        self._clean_old_session_batches(session_id)
                        continue
                    
                    # Count how many batches from this session we have in pending
                    session_batches_in_pending = 0
                    for item in self.pending_batch:
                        item_batch_id = str(item['batch_id']) if not isinstance(item['batch_id'], str) else item['batch_id']
                        if item_batch_id.startswith(session_id + '_batch_'):
                            session_batches_in_pending += 1
                    
                    # Check if this session has fewer total batches than our batch size
                    logger.info(f"🔍 Session {session_id}: total_batches={total_batches}, batch_size={self.batch_size}, pending={session_batches_in_pending}")
                    
                    if total_batches < self.batch_size:
                        # Small session: check if all batches for this session are in pending
                        if session_batches_in_pending >= total_batches:
                            logger.info(f"🔍 Session {session_id} completed: {session_batches_in_pending}/{total_batches} batches (< {self.batch_size})")
                            return True
                        else:
                            logger.info(f"🔍 Session {session_id} not complete: {session_batches_in_pending}/{total_batches} batches pending")
                    else:
                        # Large session: check if we have the final incomplete batch
                        # Calculate how many batches should have been written in full batches
                        full_batches_written = (total_batches // self.batch_size) * self.batch_size
                        remaining_batches = total_batches - full_batches_written
                        
                        logger.info(f"🔍 Session {session_id}: full_batches_written={full_batches_written}, remaining_batches={remaining_batches}")
                        
                        # If we have remaining batches that represent the final incomplete batch
                        if remaining_batches > 0 and session_batches_in_pending >= remaining_batches:
                            logger.info(f"🔍 Session {session_id} final batch ready: {session_batches_in_pending}/{remaining_batches} remaining batches")
                            return True
                        else:
                            logger.info(f"🔍 Session {session_id} waiting: {session_batches_in_pending}/{remaining_batches if remaining_batches > 0 else self.batch_size} batches needed")
                            
                except Exception as e:
                    logger.warning(f"Error checking session completion for {session_id}: {e}")
            
        except Exception as e:
            logger.warning(f"Error in check_session_completion: {e}")
            
        return False
    
    def _clean_old_session_batches(self, session_id: str):
        """Remove batches from a completed or invalid session from pending batch"""
        try:
            original_count = len(self.pending_batch)
            self.pending_batch = [
                item for item in self.pending_batch 
                if not str(item['batch_id']).startswith(session_id + '_batch_')
            ]
            cleaned_count = original_count - len(self.pending_batch)
            if cleaned_count > 0:
                logger.info(f"🧹 Cleaned {cleaned_count} old batches from session {session_id}")
        except Exception as e:
            logger.warning(f"Error cleaning old session batches for {session_id}: {e}")
    
    def add_to_batch(self, table: pa.Table, batch_id: str) -> bool:
        """Add table to pending batch"""
        self.pending_batch.append({
            'table': table,
            'batch_id': batch_id,
            'timestamp': time.time()
        })
        
        # Update batch start time if this is the first item
        if len(self.pending_batch) == 1:
            self.last_batch_time = time.time()
        
        # Check if session is completed after adding this batch
        self._session_completed = self.check_session_completion()
            
        return True
    
    def flush_batch(self) -> bool:
        """Write current batch to Iceberg and clear"""
        if not self.pending_batch:
            return True
            
        try:
            # Extract tables and batch IDs
            tables = [item['table'] for item in self.pending_batch]
            batch_ids = [item['batch_id'] for item in self.pending_batch]
            
            # Write batch
            success = self.write_batch_to_iceberg(tables)
            
            if success:
                # Update status for all batches in this write
                for batch_id in batch_ids:
                    status_key = f"batch_status:{batch_id}"
                    self.redis_client.setex(status_key, 86400, "iceberg_success")
                
                # Clear batch and reset session completion flag
                self.pending_batch = []
                self.last_batch_time = time.time()
                self._session_completed = False  # Reset for next session
                
                logger.info(f"✅ Batch flushed successfully: {len(batch_ids)} batches")
                return True
            else:
                logger.error(f"❌ Batch flush failed for {len(batch_ids)} batches")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error flushing batch: {e}")
            return False
    
    def process_message(self, message_data: bytes) -> bool:
        """Process a single message - add to batch for efficient writing"""
        try:
            # Parse message
            message = json.loads(message_data.decode('utf-8'))
            batch_id = message['batch_id']
            table_data = message['table_data']
            num_rows = message['num_rows']
            retry_count = message.get('retry_count', 0)
            
            # Deserialize PyArrow table
            table = self.deserialize_table(table_data)
            
            # Add to batch instead of writing immediately (much faster!)
            self.add_to_batch(table, batch_id)
            logger.info(f"📥 Added batch {batch_id} ({num_rows} rows) to write batch [{len(self.pending_batch)}/{self.batch_size}]")
            
            return True
                
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")
            # Try to handle retry if message is parseable
            try:
                message = json.loads(message_data.decode('utf-8'))
                return self.handle_retry(message, f"Processing error: {str(e)}")
            except:
                logger.error("❌ Could not parse message for retry")
                return False
    
    def handle_retry(self, message: Dict[str, Any], error: str) -> bool:
        """Handle retry logic for failed messages"""
        batch_id = message['batch_id']
        retry_count = message.get('retry_count', 0) + 1
        
        if retry_count <= 3:
            # Add to retry queue
            message['retry_count'] = retry_count
            message['last_error'] = error
            message['retry_timestamp'] = datetime.now().isoformat()
            
            self.redis_client.lpush('iceberg_retry_queue', json.dumps(message))
            logger.warning(f"🔄 Batch {batch_id} queued for retry {retry_count}/3: {error}")
            
            # Update status
            status_key = f"batch_status:{batch_id}"
            self.redis_client.setex(status_key, 86400, f"retrying_{retry_count}")
            return True
        else:
            # Move to dead letter queue
            message['retry_count'] = retry_count
            message['final_error'] = error
            message['failed_timestamp'] = datetime.now().isoformat()
            
            self.redis_client.lpush('iceberg_failed_queue', json.dumps(message))
            logger.error(f"💀 Batch {batch_id} moved to dead letter queue after {retry_count} retries")
            
            # Update status
            status_key = f"batch_status:{batch_id}"
            self.redis_client.setex(status_key, 86400, "failed")
            return False
    
    def run(self):
        """Main service loop"""
        logger.info("🚀 Starting Iceberg Storage Service...")
        
        # Initialize connections
        if not self.initialize_redis():
            logger.error("❌ Failed to initialize Redis, exiting")
            sys.exit(1)
            
        if not self.initialize_iceberg():
            logger.error("❌ Failed to initialize Iceberg, exiting")
            sys.exit(1)
        
        logger.info("✅ Iceberg Storage Service ready - waiting for messages...")
        
        # Main processing loop with batching
        last_session_check = time.time()
        
        while self.running:
            try:
                # Process main queue - use shorter timeout when batch is building up
                timeout = 0.5 if len(self.pending_batch) > 0 else 2.0  # Faster polling when accumulating batch
                result = self.redis_client.brpop(['iceberg_write_queue'], timeout=timeout)
                if result:
                    queue_name, message_data = result
                    success = self.process_message(message_data)
                    # Message automatically removed from queue by brpop
                    
                # Also process retry queue
                retry_result = self.redis_client.brpop(['iceberg_retry_queue'], timeout=0.5)
                if retry_result:
                    queue_name, message_data = retry_result
                    success = self.process_message(message_data)
                    # Message automatically removed from queue by brpop
                
                # Periodically check for session completion if we have pending batches
                current_time = time.time()
                if (self.pending_batch and 
                    current_time - last_session_check > 5.0):  # Check every 5 seconds
                    
                    # Extract session IDs for debugging
                    session_ids = set()
                    for batch_item in self.pending_batch:
                        batch_id = str(batch_item['batch_id'])
                        if '_batch_' in batch_id:
                            session_id = batch_id.split('_batch_')[0]
                            session_ids.add(session_id)
                    
                    logger.info(f"🕒 Periodic check: {len(self.pending_batch)} batches pending for sessions: {list(session_ids)}")
                    self._session_completed = self.check_session_completion()
                    logger.info(f"🔍 Session completion result: {self._session_completed}")
                    last_session_check = current_time
                
                # Check if batch should be written (full or timeout)
                if self.should_write_batch():
                    self.flush_batch()
                    
            except redis.ConnectionError as e:
                logger.error(f"❌ Redis connection error: {e}")
                time.sleep(5)
                # Try to reconnect
                if not self.initialize_redis():
                    logger.error("❌ Failed to reconnect to Redis")
                    break
                    
            except Exception as e:
                logger.error(f"❌ Unexpected error in main loop: {e}")
                time.sleep(1)
        
        # Flush any remaining batch on shutdown
        if self.pending_batch:
            logger.info("📤 Flushing remaining batch on shutdown...")
            self.flush_batch()
                
        logger.info("👋 Iceberg Storage Service stopped")

def main():
    """Main entry point"""
    service = IcebergStorageService()
    service.run()

if __name__ == "__main__":
    main()