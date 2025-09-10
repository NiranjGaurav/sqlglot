"""
Celery Worker for Automated Processing
Following patterns from TestDriven.io FastAPI+Celery guide
Run with: celery -A worker.celery worker --loglevel=info
"""

from celery import Celery
import logging
import sys
import os
from datetime import datetime
from typing import Dict, Any
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Celery Configuration
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

# Create Celery app - this will be accessible as worker.celery
celery = Celery("automated_processing", broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)

# Configure Celery for optimal autoscaling with staging-based architecture
celery.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600,
    task_soft_time_limit=3300,
    # Broker connection retry settings (fix deprecation warning)
    broker_connection_retry_on_startup=True,
    # Autoscaling optimization settings
    worker_prefetch_multiplier=1,  # Only prefetch 1 task per worker (better for autoscaling)
    task_acks_late=True,  # Acknowledge after task completion (safer)
    worker_max_tasks_per_child=10,  # Restart workers after 10 tasks (prevent PyArrow memory issues)
    worker_disable_rate_limits=True,  # Better performance for CPU-bound tasks
    result_expires=86400,
    # Fix PyArrow segfaults in multiprocessing
    worker_hijack_root_logger=False,  # Prevent logging conflicts
    worker_log_color=False,  # Disable color in logs (can cause issues)
    # Updated task routing for staging architecture
    task_routes={
        # Worker tasks (parallel processing)
        "process_batch": {"queue": "processing_queue"},
        "process_query_batch": {"queue": "processing_queue"},
        "process_batch_to_staging": {"queue": "processing_queue"},
        "create_staging_manifest": {"queue": "processing_queue"},
        # Committer tasks (single-threaded)
        "commit_session_to_iceberg": {"queue": "iceberg_commit"},
        "retry_failed_commit": {"queue": "iceberg_commit"},
        "commit_multiple_sessions": {"queue": "iceberg_commit"},
    },
)


# Add parent directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)


@celery.task(bind=True, name="process_query_batch", max_retries=3)
def process_query_batch(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    DEPRECATED: Use process_batch_to_staging_task instead
    This function redirects to the new PyArrow-free implementation to prevent SIGSEGV errors
    """
    logger.warning(
        "⚠️ process_query_batch is DEPRECATED. Use process_batch_to_staging_task for PyArrow-free processing"
    )

    # Extract the needed parameters and redirect to the new function
    batch_id = job_config.get("batch_id", 0)
    queries_list = job_config.get("queries_list", [])
    metadata = job_config.get("metadata", {})
    testing = job_config.get("testing", False)

    session_id = metadata.get("session_id", "legacy_session")

    # Call the new PyArrow-free implementation
    return process_batch_to_staging_impl(
        session_id=session_id,
        batch_id=batch_id,
        queries_list=queries_list,
        metadata=metadata,
        testing=testing,
    )


def store_results_table_to_iceberg(results_table, batch_data: Dict[str, Any]) -> bool:
    """
    Store PyArrow results table directly to Iceberg (OPTIMIZED APPROACH)
    Avoids dict->list->PyArrow conversions by working with Arrow tables throughout
    Includes retry logic for handling concurrent writes
    """
    if len(results_table) == 0:
        return True

    retries = 3
    retry_delay = 1  # seconds

    # CRITICAL: Change to automated_processing directory for Celery worker context
    current_dir = os.path.dirname(os.path.abspath(__file__))
    original_cwd = os.getcwd()

    try:
        # Change to the automated_processing directory
        os.chdir(current_dir)
        logger.info(f"Changed working directory to: {current_dir}")

        # Ensure sys.path includes current directory
        if current_dir not in sys.path:
            sys.path.insert(0, current_dir)

        from automated_processing import iceberg_io

        # Test catalog connection
        try:
            catalog = iceberg_io.get_iceberg_catalog()
            logger.debug("Using Iceberg catalog connection")
        except Exception as catalog_error:
            logger.error(f"Failed to get Iceberg catalog: {catalog_error}")
            return False

    except ImportError as e:
        logger.warning(f"iceberg_io not available: {e} - skipping Iceberg storage")
        return False
    except Exception as e:
        logger.error(f"Failed to setup Iceberg context: {e}")
        return False
    finally:
        # Always restore original working directory
        try:
            os.chdir(original_cwd)
        except:
            pass

    for attempt in range(retries + 1):
        try:
            # Load existing batch_statistics table using iceberg_io
            table = iceberg_io.get_table("default.batch_statistics")

            # Prepare metadata using PyArrow compute functions (vectorized)
            current_time = datetime.now()
            event_date = current_time.strftime("%Y-%m-%d")
            num_rows = len(results_table)

            # Create metadata columns using simple PyArrow arrays (avoid complex compute functions)
            query_id_seq = pa.array(
                list(range(1, num_rows + 1)), type=pa.int64()
            )  # 1-based indexing
            batch_id_full = f"{batch_data['session_id']}_{batch_data['batch_id']}"

            # Create constant arrays using simple array creation (more reliable)
            batch_ids = pa.array([batch_id_full] * num_rows, type=pa.string())
            company_names = pa.array(
                [batch_data.get("company_name", "unknown")] * num_rows, type=pa.string()
            )
            event_dates = pa.array([event_date] * num_rows, type=pa.string())
            batch_numbers = pa.array([batch_data.get("batch_idx", 0)] * num_rows, type=pa.int32())
            timestamps = pa.array([current_time] * num_rows, type=pa.timestamp("us"))
            from_dialects = pa.array(
                [batch_data.get("from_dialect", "")] * num_rows, type=pa.string()
            )
            to_dialects = pa.array([batch_data.get("to_dialect", "")] * num_rows, type=pa.string())
            # Create empty list arrays
            empty_string_lists = pa.array([[] for _ in range(num_rows)], type=pa.list_(pa.string()))

            # Combine results table with metadata (vectorized column operations)
            iceberg_table = pa.table(
                {
                    "query_id": query_id_seq,
                    "batch_id": batch_ids,
                    "company_name": company_names,
                    "event_date": event_dates,
                    "batch_number": batch_numbers,
                    "timestamp": timestamps,
                    "status": results_table["status"],
                    "executable": results_table["executable"],
                    "from_dialect": from_dialects,
                    "to_dialect": to_dialects,
                    "original_query": results_table["original_query"],
                    "converted_query": results_table["converted_query"],
                    "supported_functions": results_table["supported_functions"],
                    "unsupported_functions": results_table["unsupported_functions"],
                    "udf_list": results_table["udf_list"]
                    if "udf_list" in results_table.column_names
                    else empty_string_lists,
                    "tables_list": results_table["tables_list"]
                    if "tables_list" in results_table.column_names
                    else empty_string_lists,
                    "processing_time_ms": results_table["processing_time_ms"],
                    "error_message": results_table["error_message"],
                    "unsupported_functions_after_transpilation": results_table[
                        "unsupported_functions_after_transpilation"
                    ]
                    if "unsupported_functions_after_transpilation" in results_table.column_names
                    else empty_string_lists,
                    "joins_list": results_table["joins_list"]
                    if "joins_list" in results_table.column_names
                    else empty_string_lists,
                }
            )

            # Append to Iceberg table (single operation)
            table.append(iceberg_table)

            logger.info(
                f"✅ Stored {num_rows} results to Iceberg batch_statistics table (attempt {attempt + 1})"
            )
            return True

        except Exception as e:
            error_msg = str(e)

            # Check if it's a concurrent write conflict (multiple error patterns)
            is_concurrent_conflict = (
                "branch main has changed" in error_msg
                or "expected id" in error_msg
                or "Table has been updated by another process" in error_msg
                or "updated by another process" in error_msg
                or "Glue detected concurrent update" in error_msg
                or "concurrent update to table version" in error_msg
            )

            if is_concurrent_conflict:
                if attempt < retries:
                    import time
                    import random

                    # Add jitter to avoid thundering herd
                    jitter = random.uniform(0, 0.5)
                    sleep_time = retry_delay * (2**attempt) + jitter
                    logger.warning(
                        f"🔄 Iceberg concurrency conflict (attempt {attempt + 1}/{retries + 1}). Retrying in {sleep_time:.2f}s..."
                    )
                    time.sleep(sleep_time)
                    continue
                else:
                    logger.error(
                        f"❌ Failed to store to Iceberg after {retries + 1} attempts due to concurrency conflicts: {error_msg}"
                    )
                    return False
            else:
                # Non-retryable error
                logger.error(f"❌ Failed to store to Iceberg (non-retryable): {error_msg}")
                return False

    # Should not reach here
    return False


# STAGING-BASED IMPLEMENTATION
from automated_processing import tasks_committer
from automated_processing.tasks_committer import (
    commit_session_to_iceberg,
    retry_failed_commit,
    commit_multiple_sessions,
    CommitterTask,
)
from automated_processing import staging


class WorkerTask(celery.Task):
    """Base task class with common functionality"""

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Handle task failure"""
        logger.error(f"❌ Worker task {task_id} failed: {str(exc)}")
        super().on_failure(exc, task_id, args, kwargs, einfo)

    def on_success(self, retval, task_id, args, kwargs):
        """Handle task success"""
        if isinstance(retval, dict) and "staged_file" in retval:
            logger.info(
                f"✅ Worker task {task_id} completed: {retval.get('processed_count', 0)} queries staged"
            )
        super().on_success(retval, task_id, args, kwargs)


def process_batch_to_staging_impl(
    session_id: str, batch_id: int, queries_list: list, metadata: dict, testing: bool = False
) -> dict:
    """
    Process a batch of queries and stage results in S3

    This function does NOT touch PyArrow - only uses native Python data structures
    PyArrow is ONLY used for final Parquet writing in staging.py
    """
    from automated_processing.statistics import analyze_sql_functions

    if not queries_list:
        logger.warning(f"Empty queries list for batch {batch_id} in session {session_id}")
        return {
            "session_id": session_id,
            "batch_id": batch_id,
            "processed_count": 0,
            "successful_count": 0,
            "status": "completed",
            "staged_file": None,
            "error": "Empty queries list",
        }

    # Extract metadata
    from_dialect = metadata.get("from_dialect", "snowflake")
    to_dialect = metadata.get("to_dialect", "e6")
    company_name = metadata.get("company_name", "unknown")
    total_batches = metadata.get("total_batches", 1)

    try:
        num_queries = len(queries_list)
        logger.info(
            f"🔄 Processing batch {batch_id} in session {session_id}: {num_queries:,} queries"
        )

        # Process queries using native Python data structures (NO PyArrow in worker)
        # Collect all results in simple Python lists
        all_query_ids = []
        all_statuses = []
        all_original_queries = []
        all_converted_queries = []
        all_executables = []
        all_supported_functions_lists = []
        all_unsupported_functions_lists = []
        all_unsupported_functions_after_transpilation_lists = []
        all_joins_lists = []
        all_udf_lists = []
        all_tables_lists = []
        all_processing_times = []
        all_error_messages = []

        # Process queries in chunks for memory efficiency but don't use PyArrow
        chunk_size = 1000

        for chunk_start in range(0, num_queries, chunk_size):
            chunk_end = min(chunk_start + chunk_size, num_queries)
            chunk_queries = queries_list[chunk_start:chunk_end]

            # Process each query in the chunk
            for i, query_text in enumerate(chunk_queries):
                query_id = f"session_{session_id}_batch_{batch_id}_query_{chunk_start + i + 1}"

                try:
                    # Analyze SQL query
                    analysis = analyze_sql_functions(
                        query=query_text,
                        from_sql=from_dialect,
                        query_id=query_id,
                        to_sql=to_dialect,
                    )

                    # Append results to main lists (NO PyArrow operations)
                    all_query_ids.append(query_id)
                    all_statuses.append("success" if not analysis.get("error") else "failed")
                    all_original_queries.append(str(query_text))
                    all_converted_queries.append(str(analysis.get("converted-query", "")))
                    all_executables.append(str(analysis.get("executable", "NO")))

                    # Handle function lists (ensure all are string arrays)
                    supported_funcs = analysis.get("supported_functions", []) or []
                    all_supported_functions_lists.append(
                        [str(f) for f in supported_funcs if f is not None]
                    )

                    unsupported_funcs = analysis.get("unsupported_functions", []) or []
                    all_unsupported_functions_lists.append(
                        [str(f) for f in set(unsupported_funcs) if f is not None]
                    )

                    unsupported_after_funcs = (
                        analysis.get("unsupported_functions_after_transpilation", []) or []
                    )
                    all_unsupported_functions_after_transpilation_lists.append(
                        [str(f) for f in unsupported_after_funcs if f is not None]
                    )

                    joins = analysis.get("joins_list", []) or []
                    all_joins_lists.append([str(j) for j in joins if j is not None])

                    udfs = analysis.get("udf_list", []) or []
                    all_udf_lists.append([str(u) for u in udfs if u is not None])

                    tables = analysis.get("tables_list", []) or []
                    all_tables_lists.append([str(t) for t in tables if t is not None])

                    all_processing_times.append(100)  # Placeholder processing time
                    all_error_messages.append("")

                except Exception as e:
                    # Handle query processing errors
                    all_query_ids.append(query_id)
                    all_statuses.append("failed")
                    all_original_queries.append(str(query_text))
                    all_converted_queries.append("")
                    all_executables.append("NO")
                    all_supported_functions_lists.append([])
                    all_unsupported_functions_lists.append([])
                    all_unsupported_functions_after_transpilation_lists.append([])
                    all_joins_lists.append([])
                    all_udf_lists.append([])
                    all_tables_lists.append([])
                    all_processing_times.append(0)
                    all_error_messages.append(str(e))

                    logger.warning(f"⚠️ Failed to process query {query_id}: {str(e)}")

            # No PyArrow operations needed - data is collected in main lists above

        # Calculate success metrics using native Python (NO PyArrow compute)
        successful_count = sum(1 for status in all_statuses if status == "success")

        # Prepare data for PyArrow table creation (only when needed for staging)
        current_time = datetime.now()
        event_date = current_time.strftime("%Y-%m-%d")
        num_rows = len(all_query_ids)

        # Handle testing mode first (avoid PyArrow entirely)
        if testing:
            # Create list of dictionaries without PyArrow
            query_results = []
            for i in range(num_rows):
                query_results.append(
                    {
                        "query_id": all_query_ids[i],
                        "status": all_statuses[i],
                        "original_query": all_original_queries[i],
                        "converted_query": all_converted_queries[i],
                        "executable": all_executables[i],
                        "supported_functions": all_supported_functions_lists[i],
                        "unsupported_functions": all_unsupported_functions_lists[i],
                        "unsupported_functions_after_transpilation": all_unsupported_functions_after_transpilation_lists[
                            i
                        ],
                        "joins_list": all_joins_lists[i],
                        "udf_list": all_udf_lists[i],
                        "tables_list": all_tables_lists[i],
                        "processing_time_ms": all_processing_times[i],
                        "error_message": all_error_messages[i],
                    }
                )

            return {
                "session_id": session_id,
                "batch_id": batch_id,
                "processed_count": num_queries,
                "successful_count": successful_count,
                "status": "completed",
                "query_results": query_results,
                "staged_file": None,
            }

        # Pass native Python data to staging (NO PyArrow in worker process)
        if num_rows > 0:
            # Create final data structure with Iceberg-compatible schema using native Python
            batch_id_full = f"{session_id}_{batch_id}"

            # Prepare data as native Python dictionaries (NO PyArrow operations)
            native_data = {
                "query_id": list(range(1, num_rows + 1)),  # Python list of integers
                "batch_id": [batch_id_full] * num_rows,  # Python list of strings
                "company_name": [company_name] * num_rows,
                "event_date": [event_date] * num_rows,
                "batch_number": [batch_id] * num_rows,
                "timestamp": [current_time] * num_rows,  # Python datetime objects
                "status": all_statuses,  # Already Python list
                "executable": all_executables,
                "from_dialect": [from_dialect] * num_rows,
                "to_dialect": [to_dialect] * num_rows,
                "original_query": all_original_queries,
                "converted_query": all_converted_queries,
                "supported_functions": all_supported_functions_lists,  # Lists of lists
                "unsupported_functions": all_unsupported_functions_lists,
                "udf_list": all_udf_lists,
                "tables_list": all_tables_lists,
                "processing_time_ms": all_processing_times,
                "error_message": all_error_messages,
                "unsupported_functions_after_transpilation": all_unsupported_functions_after_transpilation_lists,
                "joins_list": all_joins_lists,
            }

            # Stage the results in S3 (staging.py handles ALL PyArrow operations)
            staged_file_path = staging.write_native_data_to_staging(
                data=native_data,
                session_id=session_id,
                batch_id=batch_id,
                metadata={
                    "company_name": company_name,
                    "from_dialect": from_dialect,
                    "to_dialect": to_dialect,
                    "batch_number": batch_id,
                    "total_batches": total_batches,
                    "processed_count": num_queries,
                    "successful_count": successful_count,
                },
            )
        else:
            staged_file_path = None

        logger.info(
            f"✅ Batch {batch_id} processing complete: {num_queries:,} processed, "
            f"{successful_count:,} successful, staged at {staged_file_path}"
        )

        return {
            "session_id": session_id,
            "batch_id": batch_id,
            "processed_count": num_queries,
            "successful_count": successful_count,
            "status": "completed",
            "staged_file": staged_file_path,
            "staging_complete": True,
            "completed_at": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error(f"❌ Failed to process batch {batch_id} in session {session_id}: {str(e)}")

        return {
            "session_id": session_id,
            "batch_id": batch_id,
            "processed_count": 0,
            "successful_count": 0,
            "status": "failed",
            "staged_file": None,
            "error": str(e),
            "failed_at": datetime.now().isoformat(),
        }


def create_staging_manifest_impl(
    session_id: str, worker_results: list, session_metadata: dict
) -> dict:
    """
    Create staging manifest after all workers complete
    """
    try:
        # Extract staged file paths from worker results
        staged_files = []
        total_processed = 0
        total_successful = 0
        failed_batches = []

        for result in worker_results:
            if isinstance(result, dict):
                if result.get("status") == "completed" and result.get("staged_file"):
                    staged_files.append(result["staged_file"])
                    total_processed += result.get("processed_count", 0)
                    total_successful += result.get("successful_count", 0)
                elif result.get("status") == "failed":
                    failed_batches.append(result.get("batch_id"))

        # Create manifest with staging summary
        manifest_metadata = {
            **session_metadata,
            "total_batches": len(worker_results),
            "successful_batches": len(staged_files),
            "failed_batches_count": len(failed_batches),
            "failed_batch_ids": failed_batches,
            "total_queries_processed": total_processed,
            "total_queries_successful": total_successful,
            "staging_completed_at": datetime.now().isoformat(),
        }

        # Write the manifest
        manifest_path = staging.write_staging_manifest(
            session_id=session_id, staged_files=staged_files, metadata=manifest_metadata
        )

        logger.info(
            f"✅ Created staging manifest for session {session_id}: "
            f"{len(staged_files)} files, {total_processed:,} queries"
        )

        return {
            "session_id": session_id,
            "manifest_path": manifest_path,
            "staged_files_count": len(staged_files),
            "total_queries_processed": total_processed,
            "total_queries_successful": total_successful,
            "failed_batches": failed_batches,
            "status": "manifest_created",
            "ready_for_commit": len(staged_files) > 0,
        }

    except Exception as e:
        logger.error(f"❌ Failed to create staging manifest for session {session_id}: {str(e)}")

        return {
            "session_id": session_id,
            "status": "manifest_failed",
            "error": str(e),
            "ready_for_commit": False,
        }


@celery.task(bind=True, name="process_batch_to_staging", max_retries=3, base=WorkerTask)
def process_batch_to_staging_task(
    self, session_id: str, batch_id: int, queries_list: list, metadata: dict, testing: bool = False
) -> dict:
    """
    Worker task: Process batch and stage results in S3
    Runs on 'processing_queue' with parallel workers
    """
    return process_batch_to_staging_impl(
        session_id=session_id,
        batch_id=batch_id,
        queries_list=queries_list,
        metadata=metadata,
        testing=testing,
    )


@celery.task(bind=True, name="create_staging_manifest", max_retries=2, base=WorkerTask)
def create_staging_manifest_task_wrapper(
    self, worker_results: list, session_id: str, session_metadata: dict
) -> dict:
    """
    Worker task: Create staging manifest after all workers complete
    Runs on 'processing_queue'

    CORRECT: In Celery chord pattern, callback receives: callback(worker_results, *signature_args)
    So if we call callback.s(session_id, session_metadata),
    Celery invokes: callback(worker_results, session_id, session_metadata)
    """
    logger.info(f"🔍 CORRECTED CHORD DEBUG - Manifest task received:")
    logger.info(
        f"    - worker_results: type={type(worker_results)}, value={worker_results[:3] if isinstance(worker_results, list) else worker_results}"
    )
    logger.info(f"    - session_id: type={type(session_id)}, value={session_id}")
    logger.info(
        f"    - session_metadata: type={type(session_metadata)}, value={list(session_metadata.keys()) if isinstance(session_metadata, dict) else session_metadata}"
    )

    # Validate parameters are in correct format
    if not isinstance(worker_results, list):
        logger.error(
            f"❌ worker_results should be list, got {type(worker_results)}: {worker_results}"
        )
        return {
            "session_id": str(session_id) if session_id else "unknown",
            "status": "manifest_failed",
            "error": f"Invalid worker_results type: expected list, got {type(worker_results)}",
            "ready_for_commit": False,
        }

    if not isinstance(session_id, str):
        logger.error(f"❌ session_id should be string, got {type(session_id)}: {session_id}")
        return {
            "session_id": "unknown",
            "status": "manifest_failed",
            "error": f"Invalid session_id type: expected str, got {type(session_id)}",
            "ready_for_commit": False,
        }

    return create_staging_manifest_impl(
        session_id=session_id, worker_results=worker_results, session_metadata=session_metadata
    )


@celery.task(bind=True, name="commit_session_to_iceberg", max_retries=5, base=CommitterTask)
def commit_session_to_iceberg_task(
    self,
    session_id: str,
    session_metadata: dict,
    table_name: str = "default.batch_statistics",
    cleanup_staging: bool = True,
    use_chunked_commit: bool = False,
    chunk_size: int = 50,
) -> dict:
    """
    Committer task: Atomically commit staged files to Iceberg
    Runs on 'iceberg_commit' queue (single-threaded, concurrency=1)
    """
    return commit_session_to_iceberg(
        session_id=session_id,
        session_metadata=session_metadata,
        table_name=table_name,
        cleanup_staging=cleanup_staging,
        use_chunked_commit=use_chunked_commit,
        chunk_size=chunk_size,
    )


@celery.task(bind=True, name="retry_failed_commit", max_retries=1, base=CommitterTask)
def retry_failed_commit_task(
    self,
    session_id: str,
    original_error: str,
    retry_attempt: int = 1,
    max_retries: int = 3,
    table_name: str = "default.batch_statistics",
) -> dict:
    """
    Committer task: Retry failed Iceberg commit
    Runs on 'iceberg_commit' queue (single-threaded)
    """
    return retry_failed_commit(
        session_id=session_id,
        original_error=original_error,
        retry_attempt=retry_attempt,
        max_retries=max_retries,
        table_name=table_name,
    )


@celery.task(bind=True, name="commit_multiple_sessions", max_retries=2, base=CommitterTask)
def commit_multiple_sessions_task(
    self,
    session_ids: list,
    table_name: str = "default.batch_statistics",
    cleanup_staging: bool = True,
) -> dict:
    """
    Committer task: Commit multiple sessions sequentially
    Runs on 'iceberg_commit' queue (single-threaded)
    """
    return commit_multiple_sessions(
        session_ids=session_ids, table_name=table_name, cleanup_staging=cleanup_staging
    )


# Worker initialization hooks to fix PyArrow multiprocessing issues
from celery.signals import worker_process_init, worker_process_shutdown


@worker_process_init.connect
def initialize_worker(**kwargs):
    """Initialize worker process (PyArrow-free)"""
    import os
    import sys

    # Ensure no shared memory issues with PyArrow (removed PyArrow initialization)
    os.environ["ARROW_ENABLE_NULL_CHECK_FOR_GET"] = "0"
    os.environ["ARROW_ENABLE_TIMING_TESTS"] = "0"
    os.environ["OMP_NUM_THREADS"] = "1"

    # Add parent directory to path for imports in each process
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

    logger.info(f"✅ PyArrow-free worker process initialized with PID {os.getpid()}")


@worker_process_shutdown.connect
def cleanup_worker(**kwargs):
    """Clean up resources in each worker process (PyArrow-free)"""
    import gc

    # Force garbage collection only
    gc.collect()

    logger.info("🧹 PyArrow-free worker process cleanup completed")
