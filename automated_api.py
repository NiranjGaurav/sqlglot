"""
Automated Processing API
Handles distributed batch processing endpoints for SQL query transpilation
"""

from fastapi import FastAPI, Form, HTTPException, Response
from typing import Optional, Dict, Any
import uvicorn
import os
import json
import logging
from datetime import datetime
import time

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Automated Processing API", version="1.0.0")

# Import distributed processing modules
import sys
sys.path.append("./automated_processing")
from automated_processing.orchestrator import (
    orchestrate_staging_based_processing,
    monitor_session_progress,
)

# Simple cache for session status to improve performance
_session_status_cache = {}
_cache_ttl = 8  # seconds

def get_cached_session_status(session_id: str):
    """Get cached session status if available and not expired"""
    current_time = time.time()
    if session_id in _session_status_cache:
        cached_result, timestamp = _session_status_cache[session_id]
        if current_time - timestamp < _cache_ttl:
            logger.info(f"📋 Returning cached status for session {session_id}")
            return cached_result
    return None

def cache_session_status(session_id: str, result):
    """Cache session status result"""
    current_time = time.time()
    _session_status_cache[session_id] = (result, current_time)
    
    # Simple cleanup: remove old entries if cache gets too large
    if len(_session_status_cache) > 100:
        cutoff_time = current_time - _cache_ttl
        expired_keys = [k for k, (_, ts) in _session_status_cache.items() if ts < cutoff_time]
        for key in expired_keys:
            del _session_status_cache[key]


@app.post("/process-parquet-directory-automated")
async def process_parquet_directory_automated(
    directory_path: str = Form(
        ...,
        description="Path to directory containing parquet files OR path to a single parquet file",
    ),
    company_name: str = Form(..., description="Company identifier for Iceberg partitioning"),
    from_dialect: str = Form(..., description="Source SQL dialect (e.g., snowflake, bigquery)"),
    to_dialect: str = Form("e6", description="Target SQL dialect"),
    query_column: str = Form(..., description="Column name containing SQL queries"),
    batch_size: int = Form(10000, description="Number of queries per batch"),
    filters: Optional[str] = Form(
        None,
        description='JSON string of column filters e.g. \'{"statement_type": "SELECT", "client_application": "PowerBI"}\'',
    ),
    session_name: Optional[str] = Form(None, description="Custom session name for identification"),
):
    """
    Batch processing endpoint for parquet files containing SQL queries.

    Accepts either:
    - Path to a directory containing parquet files (e.g., "/path/to/parquet_files/")
    - Path to a single parquet file (e.g., "/path/to/file.parquet")
    - S3 directory path (e.g., "s3://bucket/path/to/directory/")
    - S3 single file path (e.g., "s3://bucket/path/to/file.parquet")

    Processes queries through SQLGlot transpilation using Celery distributed workers.
    Results are stored in Iceberg table with partitioning by company_name and event_date.
    """

    logger.info(
        f"🚀 Starting FULLY AUTONOMOUS processing: {directory_path} ({from_dialect} -> {to_dialect})"
    )

    try:
        # Validate inputs first
        if not directory_path or not directory_path.strip():
            raise HTTPException(status_code=400, detail="directory_path is required")

        if not query_column or not query_column.strip():
            raise HTTPException(status_code=400, detail="query_column is required")

        if batch_size <= 0:
            raise HTTPException(status_code=400, detail="batch_size must be positive")

        # Parse filters if provided
        filter_dict = {}
        if filters and filters.strip():
            try:
                filter_dict = json.loads(filters)
                logger.info(f"Parsed filters: {filter_dict}")
            except json.JSONDecodeError:
                raise HTTPException(
                    status_code=400, detail="Invalid JSON format for filters parameter"
                )

        # Use existing file processing with staging-based orchestration
        logger.info("🔧 Starting processing with staging-based Celery orchestrator...")

        # Import the existing file processing functions
        from automated_processing.tasks import (
            discover_parquet_files,
            extract_unique_queries_from_file,
            create_query_batch_configs,
        )

        # Use existing file discovery logic
        file_paths = discover_parquet_files(directory_path.strip(), query_column.strip())

        if not file_paths:
            raise HTTPException(
                status_code=400, detail=f"No valid parquet files found in {directory_path}"
            )

        # Process all files and collect batch configs (same as original orchestrator)
        all_batch_configs = []
        session_id = f"api_session_{int(datetime.now().timestamp())}"

        for file_path in file_paths:
            file_name = os.path.basename(file_path)
            logger.info(f"📖 Reading and processing file: {file_name}")

            # Extract unique queries from this file as PyArrow table
            unique_table = extract_unique_queries_from_file(
                file_path, query_column.strip(), filter_dict
            )

            if len(unique_table) == 0:
                logger.warning(f"No queries found in {file_name}")
                continue

            logger.info(f"✅ Extracted {len(unique_table):,} unique queries from {file_name}")

            # Create batch configurations - get PyArrow table + metadata
            batch_table, metadata = create_query_batch_configs(
                unique_table,
                session_id,
                company_name.strip(),
                from_dialect.lower().strip(),
                to_dialect.lower().strip(),
                query_column.strip(),
                batch_size,
                {"file_path": file_path, "file_name": file_name},
            )

            # Each row is a job - extract queries as Python list for JSON serialization
            for i in range(len(batch_table)):
                batch_id = batch_table["batch_id"][i].as_py()  # Extract batch ID
                queries_array = batch_table["queries_array"][
                    i
                ].as_py()  # Extract queries as Python list

                all_batch_configs.append(
                    {
                        "batch_id": batch_id,
                        "queries_list": queries_array,  # Python list (JSON serializable)
                        "metadata": metadata,
                    }
                )

        if not all_batch_configs:
            raise HTTPException(status_code=400, detail="No valid queries found in any files")

        logger.info(f"📦 Created {len(all_batch_configs)} batch configs from file processing")

        # Convert to staging-based batches_data format
        batches_data = []
        for config in all_batch_configs:
            batches_data.append({"queries_list": config["queries_list"], "testing": False})

        # Create session metadata
        session_metadata = {
            "company_name": company_name.strip(),
            "from_dialect": from_dialect.lower().strip(),
            "to_dialect": to_dialect.lower().strip(),
            "query_column": query_column.strip(),
            "batch_size": batch_size,
            "filters": filter_dict,
            "session_name": session_name.strip() if session_name else None,
            "created_at": datetime.now().isoformat(),
            "directory_path": directory_path.strip(),
        }

        # Use staging-based orchestration with real file data
        result = orchestrate_staging_based_processing(
            session_id=session_id,
            batches_data=batches_data,
            session_metadata=session_metadata,
            use_chord=True,
            cleanup_staging=True,
        )

        # Check if there was an error
        if "error" in result:
            logger.error(f"❌ Orchestration failed: {result['error']}")
            raise HTTPException(status_code=500, detail=result["error"])

        logger.info(f"✅ Processing started with session {result['session_id']}")

        # Fixed Iceberg storage structure
        event_date = datetime.now().strftime("%Y-%m-%d")
        iceberg_structure = f"company_name={company_name}/event_date={event_date}/"

        logger.info(f"📂 Iceberg structure: {iceberg_structure}")

        return {
            "session_id": result["session_id"],
            "total_files": result.get("total_files", 0),
            "total_batches": result.get("total_batches", 0),
            "task_ids": result.get("task_ids", []),
            "status": result.get("status", "processing"),
            "created_at": result.get("created_at"),
            "status_url": f"/processing-status/{result['session_id']}",
            "configuration": {
                "directory_path": directory_path,
                "company_name": company_name,
                "query_column": query_column,
                "batch_size": batch_size,
                "dialect_conversion": f"{from_dialect} -> {to_dialect}",
            },
            "iceberg_storage": {
                "table": "default.batch_statistics",
                "partition_structure": iceberg_structure,
                "storage_pattern": f"{iceberg_structure}session_{result['session_id']}_batch_{{batch_id}}.parquet",
            },
            "message": "Processing initiated! Monitor progress at the status_url.",
        }

    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"❌ Error in autonomous processing: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to start autonomous processing: {str(e)}"
        )


@app.get("/sessions/list")
async def list_active_sessions():
    """Get lightweight list of active sessions with basic info"""
    try:
        from automated_processing.orchestrator import discover_active_sessions_from_redis
        from automated_processing.orchestrator import get_session_metadata_from_redis
        
        # Discover all session IDs from Redis
        discovered_sessions = discover_active_sessions_from_redis()
        
        # Get basic info for each session (without heavy status polling)
        sessions_info = []
        for session_id in discovered_sessions:
            try:
                # Get session metadata (lightweight)
                metadata = get_session_metadata_from_redis(session_id)
                
                # Debug log to see what metadata we're getting
                logger.debug(f"Session {session_id} metadata: {metadata}")
                
                # Provide better defaults and validation
                company_name = metadata.get("company_name")
                if not company_name or company_name == "":
                    company_name = "No Company"
                
                created_at = metadata.get("created_at")
                # Validate the timestamp format
                if created_at:
                    try:
                        from datetime import datetime
                        # Test if it's a valid ISO format
                        datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    except (ValueError, AttributeError):
                        logger.debug(f"Invalid created_at format for session {session_id}: {created_at}")
                        created_at = None
                
                session_name = metadata.get("session_name")
                if not session_name:
                    # Create a readable session name from session_id
                    session_name = f"Session {session_id[:8]}"
                
                # Create basic session info with proper timestamp
                session_info = {
                    "session_id": session_id,
                    "session_name": session_name,
                    "company_name": company_name,
                    "created_at": created_at,
                    "status": "processing",  # Default status, will be updated when selected
                }
                sessions_info.append(session_info)
                
            except Exception as e:
                logger.warning(f"Could not get metadata for session {session_id}: {e}")
                # Add minimal info for sessions without metadata with better defaults
                sessions_info.append({
                    "session_id": session_id,
                    "session_name": f"Session {session_id[:8]}",  # Readable fallback
                    "company_name": "No Company",  # Better than "Unknown"
                    "created_at": None,  # Don't use current time as fallback
                    "status": "processing",
                })
        
        return {
            "sessions": sessions_info,
            "total_sessions": len(sessions_info),
        }
        
    except Exception as e:
        logger.error(f"❌ Error listing active sessions: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to list sessions: {str(e)}")


@app.get("/processing-status/{session_id}")
async def get_processing_session_status(session_id: str):
    """Get status of automated processing session or discover all sessions"""
    try:
        # Special case: discover all active sessions
        if session_id == "discover_all":
            from automated_processing.orchestrator import discover_active_sessions_from_redis

            # Discover all session IDs from Redis session metadata
            discovered_sessions = discover_active_sessions_from_redis()

            return {
                "session_id": "discover_all",
                "discovered_sessions": discovered_sessions,
                "total_discovered": len(discovered_sessions),
                "discovery_method": "redis_based",
            }

        # Check cache first to improve performance
        cached_result = get_cached_session_status(session_id)
        if cached_result is not None:
            return cached_result
        
        # Regular session status check using the staging-based monitoring function
        result = monitor_session_progress(
            session_id=session_id,
            workers_group_id=None,  # Will be auto-discovered from staging files
            committer_task_id=None,  # Will be auto-discovered if needed
        )
        
        # Cache the result for future requests
        cache_session_status(session_id, result)
        return result
    except Exception as e:
        logger.error(f"❌ Error getting session status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get session status: {str(e)}")


@app.get("/task-result/{task_id}")
async def get_individual_task_result(task_id: str):
    """Get result of a specific task"""
    try:
        from celery.result import AsyncResult
        from automated_processing.worker import celery

        result = AsyncResult(task_id, backend=celery.backend)

        response = {"task_id": task_id, "state": result.state, "ready": result.ready()}

        if result.ready():
            if result.successful():
                response["result"] = result.result
                response["status"] = "SUCCESS"
            else:
                response["error"] = str(result.info)
                response["status"] = "FAILURE"
        else:
            response["status"] = "PENDING"

        return response
    except Exception as e:
        logger.error(f"❌ Error getting task result: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get task result: {str(e)}")


@app.post("/validate-s3-bucket")
async def validate_s3_bucket(
    s3_path: str = Form(...),
):
    """Validate S3 bucket access and return available columns"""
    try:
        import pyarrow.fs as fs
        import pyarrow.parquet as pq
        
        if not s3_path.startswith("s3://"):
            return {"authenticated": False, "error": "Invalid S3 path"}

        bucket = s3_path.split("/")[2]
        key_prefix = "/".join(s3_path.split("/")[3:])

        try:
            s3fs = fs.S3FileSystem(
                access_key=os.getenv("AWS_ACCESS_KEY_ID"),
                secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                session_token=os.getenv("AWS_SESSION_TOKEN"),
                region=os.getenv("AWS_REGION", "us-east-1"),
                # Add aggressive timeout and retry settings for PyArrow
                connect_timeout=10,
                request_timeout=30,
            )
        except Exception as e:
            return {"authenticated": False, "error": f"S3 auth failed: {str(e)}"}

        path = f"{bucket}/{key_prefix}" if key_prefix else bucket
        file_info = s3fs.get_file_info(path)

        parquet_files = []
        if file_info.type == fs.FileType.File and path.endswith(".parquet"):
            parquet_files = [path]
        else:
            from pyarrow.fs import FileSelector

            selector = FileSelector(path, recursive=True)
            file_infos = s3fs.get_file_info(selector)
            parquet_files = [
                f.path
                for f in file_infos
                if f.type == fs.FileType.File and f.path.endswith(".parquet")
            ]

        if not parquet_files:
            return {"authenticated": True, "error": "No parquet files found"}

        parquet_file = pq.ParquetFile(parquet_files[0], filesystem=s3fs)
        all_columns = [field.name for field in parquet_file.schema]

        return {"authenticated": True, "columns": all_columns}

    except Exception as e:
        logger.error(f"Failed to validate S3 bucket: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to validate S3 bucket: {str(e)}")


@app.get("/health")
def health_check():
    """Enhanced health check including Iceberg and Redis connectivity"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {}
    }
    
    try:
        # Test Iceberg catalog connectivity
        try:
            from automated_processing.iceberg_io import get_iceberg_catalog
            catalog = get_iceberg_catalog()
            health_status["services"]["iceberg"] = {
                "status": "connected",
                "catalog_type": "AWS Glue"
            }
        except Exception as iceberg_error:
            health_status["services"]["iceberg"] = {
                "status": "error",
                "error": str(iceberg_error),
                "catalog_type": "AWS Glue"
            }
            health_status["status"] = "degraded"
        
        # Test Redis connectivity (if available)
        try:
            import redis
            
            # Parse Redis URL from environment
            broker_url = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
            if broker_url.startswith("redis://"):
                url_parts = broker_url.replace("redis://", "").split("/")[0]
                if ":" in url_parts:
                    host, port = url_parts.split(":")
                    port = int(port)
                else:
                    host = url_parts
                    port = 6379
                
                r = redis.Redis(host=host, port=port, db=0, socket_timeout=5)
                r.ping()
                health_status["services"]["redis"] = {
                    "status": "connected",
                    "host": host,
                    "port": port
                }
            else:
                health_status["services"]["redis"] = {
                    "status": "unknown",
                    "error": "Invalid CELERY_BROKER_URL format"
                }
        except Exception as redis_error:
            health_status["services"]["redis"] = {
                "status": "error", 
                "error": str(redis_error)
            }
            if health_status["status"] == "healthy":
                health_status["status"] = "degraded"
        
        # Return appropriate HTTP status
        if health_status["status"] == "healthy":
            return health_status
        else:
            return Response(
                content=json.dumps(health_status),
                status_code=200,  # Still return 200 for degraded to avoid K8s restart
                media_type="application/json"
            )
            
    except Exception as e:
        return Response(
            content=json.dumps({
                "status": "error",
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }),
            status_code=500,
            media_type="application/json"
        )


if __name__ == "__main__":
    import multiprocessing

    # Calculate optimal workers based on CPU cores
    cpu_cores = multiprocessing.cpu_count()
    # Formula: (2 × CPU_cores) + 1, with min 2 and max 20
    optimal_workers = min(max((2 * cpu_cores) + 1, 2), 20)

    # Allow override via environment variable
    workers = int(os.getenv("UVICORN_WORKERS", optimal_workers))

    logger.info(f"Detected {cpu_cores} CPU cores, using {workers} workers")

    uvicorn.run("automated_api:app", host="0.0.0.0", port=8101, proxy_headers=True, workers=workers)