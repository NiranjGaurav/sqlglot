"""
Celery Worker for Automated Processing
Following patterns from TestDriven.io FastAPI+Celery guide
Run with: celery -A worker.celery worker --loglevel=info
"""

import os
import sys
from celery import Celery
import pyarrow as pa
import pyarrow.compute as pc
import logging
from datetime import datetime
from typing import Dict, Any
from tqdm import tqdm

print("🚀 Worker: Using prefork pool for maximum CPU performance")
print("📦 Worker: Iceberg storage handled via Redis queue (no SIGSEGV)")

# DON'T import iceberg_handler - causes SIGSEGV with prefork
# Iceberg writes handled by separate service via Redis queue
import redis
import json
import base64

# Initialize Redis client for Iceberg queue
try:
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
    redis_client.ping()
    print("✅ Worker: Connected to Redis for Iceberg queue")
except Exception as e:
    print(f"⚠️  Worker: Failed to connect to Redis: {e}")
    redis_client = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Celery Configuration (embedded for now)
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

# Create Celery app - this will be accessible as worker.celery
celery = Celery(
    'automated_processing',
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND
)

# Configure Celery for optimal autoscaling and spawn multiprocessing
celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600,
    task_soft_time_limit=3300,
    # Use prefork pool for maximum CPU performance (PyIceberg handled separately)
    worker_pool='prefork',
    worker_pool_restarts=True,
    # Multi-worker cooperative settings
    worker_prefetch_multiplier=1,  # Only prefetch 1 task per worker (better for multiple workers)
    task_acks_late=True,          # Acknowledge after task completion (safer)
    worker_max_tasks_per_child=10, # Allow more tasks per child before restart
    worker_disable_rate_limits=True, # Better performance for CPU-bound tasks
    # Fair distribution among workers
    task_reject_on_worker_lost=True,  # Redistribute lost tasks to other workers
    result_expires=86400,
    task_routes={
        'process_batch': {'queue': 'processing_queue'},
        'process_query_batch': {'queue': 'processing_queue'}
    }
)

# Add parent directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)


@celery.task(bind=True, name='process_query_batch', max_retries=3)
def process_query_batch(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Consolidated function - Process PyArrow table of queries using vectorized operations
    Handles new PyArrow job format where each job gets one batch row
    
    Creates complete Iceberg-ready table with all metadata columns included.
    """
    from automated_processing.statistics import analyze_sql_functions

    # Extract batch data and metadata from job config (now JSON serializable)
    batch_id = job_config['batch_id']        # Integer batch ID
    queries_list = job_config['queries_list']  # Python list of queries (JSON serializable)
    metadata = job_config['metadata']        # Dictionary with session info

    # Extract metadata
    session_id = metadata['session_id']
    query_column = metadata.get('query_column', 'query')
    from_dialect = metadata.get('from_dialect', 'snowflake')
    to_dialect = metadata.get('to_dialect', 'e6')
    total_batches = metadata.get('total_batches', 1)
    batch_idx = batch_id  # Use batch_id as batch index
    is_testing = job_config.get('testing', False)

    if hasattr(self, 'request') and self.request and hasattr(self.request, 'hostname') and hasattr(self.request, 'id'):
        if self.request.hostname and self.request.id:
            worker_id = f"{self.request.hostname}:{self.request.id[:8]}"
        else:
            worker_id = "test_worker:test_id"
    else:
        worker_id = "test_worker:test_id"

    try:
        num_queries = len(queries_list)
        logger.info(f"Processing {num_queries:,} queries in batch {batch_idx}/{total_batches-1}")

        # Convert list to PyArrow array for processing
        queries_array = pa.array(queries_list, type=pa.string())

        # Process in smaller chunks to maintain Arrow format
        chunk_size = 1000
        result_chunks = []

        progress_desc = f"Batch {batch_idx}/{total_batches-1}"

        for chunk_start in tqdm(range(0, num_queries, chunk_size), desc=progress_desc, unit="chunks"):
            chunk_end = min(chunk_start + chunk_size, num_queries)
            # Use PyArrow slice
            chunk_queries = queries_array[chunk_start:chunk_end]

            # Convert chunk to Python list only for SQL processing (unavoidable for SQLGlot)
            chunk_queries_list = chunk_queries.to_pylist()

            # Process all queries in chunk
            query_ids = []
            statuses = []
            original_queries = []
            converted_queries = []
            executables = []
            supported_functions_lists = []
            unsupported_functions_lists = []
            unsupported_functions_after_transpilation_lists = []
            joins_lists = []
            udf_lists = []
            tables_lists = []
            processing_times = []
            error_messages = []

            # Process queries (this part still needs Python iteration due to SQL analysis)
            for i, query_text in enumerate(chunk_queries_list):
                query_id = f"batch_{batch_idx}_query_{chunk_start + i + 1}"

                try:
                    analysis = analyze_sql_functions(
                        query=query_text,
                        from_sql=from_dialect,
                        query_id=query_id,
                        to_sql=to_dialect
                    )

                    query_ids.append(query_id)
                    statuses.append('success' if not analysis.get('error') else 'failed')
                    original_queries.append(str(query_text))
                    converted_queries.append(str(analysis.get('converted-query', '')))
                    executables.append(str(analysis.get('executable', 'NO')))

                    # Ensure all list elements are strings and handle None values
                    supported_funcs = analysis.get('supported_functions', []) or []
                    supported_functions_lists.append([str(f) for f in supported_funcs if f is not None])

                    unsupported_funcs = analysis.get('unsupported_functions', []) or []
                    unsupported_functions_lists.append([str(f) for f in set(unsupported_funcs) if f is not None])

                    unsupported_after_funcs = analysis.get('unsupported_functions_after_transpilation', []) or []
                    unsupported_functions_after_transpilation_lists.append([str(f) for f in unsupported_after_funcs if f is not None])

                    joins = analysis.get('joins_list', []) or []
                    joins_lists.append([str(j) for j in joins if j is not None])

                    udfs = analysis.get('udf_list', []) or []
                    udf_lists.append([str(u) for u in udfs if u is not None])

                    tables = analysis.get('tables_list', []) or []
                    tables_lists.append([str(t) for t in tables if t is not None])

                    processing_times.append(100)
                    error_messages.append('')

                except Exception as e:
                    query_ids.append(query_id)
                    statuses.append('failed')
                    original_queries.append(str(query_text))
                    converted_queries.append('')
                    executables.append('NO')
                    supported_functions_lists.append([])
                    unsupported_functions_lists.append([])
                    unsupported_functions_after_transpilation_lists.append([])
                    joins_lists.append([])
                    udf_lists.append([])
                    tables_lists.append([])
                    processing_times.append(0)
                    error_messages.append(str(e))

            # Create PyArrow table from chunk results
            chunk_results = pa.table({
                'query_id': pa.array(query_ids, type=pa.string()),
                'status': pa.array(statuses, type=pa.string()),
                'original_query': pa.array(original_queries, type=pa.string()),
                'converted_query': pa.array(converted_queries, type=pa.string()),
                'executable': pa.array(executables, type=pa.string()),
                'supported_functions': pa.array(supported_functions_lists, type=pa.list_(pa.string())),
                'unsupported_functions': pa.array(unsupported_functions_lists, type=pa.list_(pa.string())),
                'unsupported_functions_after_transpilation': pa.array(unsupported_functions_after_transpilation_lists, type=pa.list_(pa.string())),
                'joins_list': pa.array(joins_lists, type=pa.list_(pa.string())),
                'udf_list': pa.array(udf_lists, type=pa.list_(pa.string())),
                'tables_list': pa.array(tables_lists, type=pa.list_(pa.string())),
                'processing_time_ms': pa.array(processing_times, type=pa.int64()),
                'error_message': pa.array(error_messages, type=pa.string())
            })
            result_chunks.append(chunk_results)

        # Concatenate all chunks using PyArrow (vectorized)
        if result_chunks:
            query_results_table = pa.concat_tables(result_chunks)
        else:
            # Empty table with correct schema
            query_results_table = pa.table({
                'query_id': pa.array([], type=pa.string()),
                'status': pa.array([], type=pa.string()),
                'original_query': pa.array([], type=pa.string()),
                'converted_query': pa.array([], type=pa.string()),
                'executable': pa.array([], type=pa.string()),
                'supported_functions': pa.array([], type=pa.list_(pa.string())),
                'unsupported_functions': pa.array([], type=pa.list_(pa.string())),
                'unsupported_functions_after_transpilation': pa.array([], type=pa.list_(pa.string())),
                'joins_list': pa.array([], type=pa.list_(pa.string())),
                'udf_list': pa.array([], type=pa.list_(pa.string())),
                'tables_list': pa.array([], type=pa.list_(pa.string())),
                'processing_time_ms': pa.array([], type=pa.int64()),
                'error_message': pa.array([], type=pa.string())
            })

        # Calculate success count using PyArrow compute
        success_mask = pc.equal(query_results_table['status'], 'success')
        successful_count = pc.sum(pc.cast(success_mask, pa.int64())).as_py()

        # Build complete Iceberg table with all metadata columns
        final_iceberg_table = build_complete_iceberg_table(query_results_table, metadata, batch_id)

        # Store to Iceberg using the complete table
        try:
            if is_testing:
                # Convert PyArrow table to list of dictionaries (same format as parquet)
                query_results = query_results_table.to_pandas().to_dict('records')

                return {
                    'batch_id': batch_id,
                    'processed_count': num_queries,
                    'successful_count': successful_count,
                    'status': 'completed',
                    'query_results': query_results,  # List of dicts, same as parquet format
                    'worker_id': worker_id,
                    'completed_at': datetime.now().isoformat()
                }

            iceberg_success = queue_results_for_iceberg_storage(final_iceberg_table, batch_id)
        except Exception as iceberg_error:
            logger.error(f"❌ Critical Iceberg error - continuing without storage: {str(iceberg_error)}")
            iceberg_success = False

        return {
            'batch_id': batch_id,
            'processed_count': num_queries,
            'successful_count': successful_count,
            'iceberg_success': iceberg_success,
            'worker_id': worker_id,
            'completed_at': datetime.now().isoformat(),
            'status': 'completed'
        }

    except Exception as exc:
        # Retry with exponential backoff (TestDriven.io pattern)
        if self.request.retries < self.max_retries:
            retry_delay = 60 * (2 ** self.request.retries)
            raise self.retry(exc=exc, countdown=retry_delay)
        else:
            return {
                'batch_id': batch_id,
                'status': 'failed',
                'error': str(exc),
                'worker_id': worker_id
            }


def build_complete_iceberg_table(query_results_table: pa.Table, metadata: Dict[str, Any], batch_id: int) -> pa.Table:
    """
    Build complete Iceberg table with all metadata columns.
    Consolidates all table construction logic in one place.
    """
    if len(query_results_table) == 0:
        logger.info("Empty query results table, returning empty Iceberg table")
        # Return empty table with full Iceberg schema
        return create_empty_iceberg_table()
    
    # Prepare metadata columns
    current_time = datetime.now()
    event_date = current_time.strftime("%Y-%m-%d")
    num_rows = len(query_results_table)

    # Create metadata arrays
    query_id_seq = pa.array(list(range(1, num_rows + 1)), type=pa.int64())
    batch_id_full = f"{metadata['session_id']}_{batch_id}"
    
    batch_ids = pa.array([batch_id_full] * num_rows, type=pa.string())
    company_names = pa.array([metadata.get('company_name', 'unknown')] * num_rows, type=pa.string())
    event_dates = pa.array([event_date] * num_rows, type=pa.string())
    batch_numbers = pa.array([metadata.get('batch_idx', batch_id)] * num_rows, type=pa.int32())
    timestamps = pa.array([current_time] * num_rows, type=pa.timestamp('us'))
    from_dialects = pa.array([metadata.get('from_dialect', '')] * num_rows, type=pa.string())
    to_dialects = pa.array([metadata.get('to_dialect', '')] * num_rows, type=pa.string())

    # Create empty list arrays for missing columns
    empty_string_lists = pa.array([[] for _ in range(num_rows)], type=pa.list_(pa.string()))

    # Build complete Iceberg table with proper column ordering
    iceberg_table = pa.table({
        "query_id": query_id_seq,
        "batch_id": batch_ids,
        "company_name": company_names,
        "event_date": event_dates,
        "batch_number": batch_numbers,
        "timestamp": timestamps,
        "status": query_results_table['status'],
        "executable": query_results_table['executable'],
        "from_dialect": from_dialects,
        "to_dialect": to_dialects,
        "original_query": query_results_table['original_query'],
        "converted_query": query_results_table['converted_query'],
        "supported_functions": query_results_table['supported_functions'],
        "unsupported_functions": query_results_table['unsupported_functions'],
        "udf_list": query_results_table['udf_list'] if 'udf_list' in query_results_table.column_names else empty_string_lists,
        "tables_list": query_results_table['tables_list'] if 'tables_list' in query_results_table.column_names else empty_string_lists,
        "processing_time_ms": query_results_table['processing_time_ms'],
        "error_message": query_results_table['error_message'],
        "unsupported_functions_after_transpilation": query_results_table['unsupported_functions_after_transpilation'] if 'unsupported_functions_after_transpilation' in query_results_table.column_names else empty_string_lists,
        "joins_list": query_results_table['joins_list'] if 'joins_list' in query_results_table.column_names else empty_string_lists
    })
    
    logger.info(f"Built complete Iceberg table with {num_rows} rows and {len(iceberg_table.column_names)} columns")
    return iceberg_table


def create_empty_iceberg_table() -> pa.Table:
    """Create empty table with full Iceberg schema."""
    return pa.table({
        "query_id": pa.array([], type=pa.int64()),
        "batch_id": pa.array([], type=pa.string()),
        "company_name": pa.array([], type=pa.string()),
        "event_date": pa.array([], type=pa.string()),
        "batch_number": pa.array([], type=pa.int32()),
        "timestamp": pa.array([], type=pa.timestamp('us')),
        "status": pa.array([], type=pa.string()),
        "executable": pa.array([], type=pa.string()),
        "from_dialect": pa.array([], type=pa.string()),
        "to_dialect": pa.array([], type=pa.string()),
        "original_query": pa.array([], type=pa.string()),
        "converted_query": pa.array([], type=pa.string()),
        "supported_functions": pa.array([], type=pa.list_(pa.string())),
        "unsupported_functions": pa.array([], type=pa.list_(pa.string())),
        "udf_list": pa.array([], type=pa.list_(pa.string())),
        "tables_list": pa.array([], type=pa.list_(pa.string())),
        "processing_time_ms": pa.array([], type=pa.int64()),
        "error_message": pa.array([], type=pa.string()),
        "unsupported_functions_after_transpilation": pa.array([], type=pa.list_(pa.string())),
        "joins_list": pa.array([], type=pa.list_(pa.string()))
    })


def queue_results_for_iceberg_storage(complete_iceberg_table: pa.Table, batch_id: int) -> bool:
    """
    Queue PyArrow table for Iceberg storage via Redis.
    This avoids SIGSEGV by keeping PyIceberg in a separate process.
    
    Args:
        complete_iceberg_table: Complete PyArrow table with all metadata columns included
        batch_id: Batch identifier
        
    Returns:
        bool: Success status (queuing, not actual Iceberg write)
    """
    if len(complete_iceberg_table) == 0:
        logger.info("Empty table received, skipping storage")
        return True

    if not redis_client:
        logger.warning("Redis not available - skipping Iceberg storage")
        return False

    try:
        # Serialize PyArrow table to bytes
        import pyarrow as pa
        import io
        
        # Use Arrow IPC format for efficient serialization
        buffer = io.BytesIO()
        with pa.ipc.new_stream(buffer, complete_iceberg_table.schema) as writer:
            writer.write_table(complete_iceberg_table)
        
        serialized_table = buffer.getvalue()
        
        # Create message for Iceberg service
        message = {
            'batch_id': batch_id,
            'table_data': base64.b64encode(serialized_table).decode('utf-8'),
            'num_rows': len(complete_iceberg_table),
            'timestamp': datetime.now().isoformat(),
            'retry_count': 0
        }
        
        # Push to Redis queue for Iceberg service
        redis_client.lpush('iceberg_write_queue', json.dumps(message))
        
        num_rows = len(complete_iceberg_table)
        logger.info(f"📤 Queued {num_rows} rows for Iceberg storage (batch {batch_id})")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to queue for Iceberg storage: {str(e)}")
        return False
