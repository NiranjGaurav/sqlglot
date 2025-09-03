"""
Iceberg I/O Operations
Safe Iceberg table operations with transaction support
"""
import os
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, IntegerType, TimestampType, ListType, LongType
from pyiceberg.partitioning import PartitionSpec, PartitionField

logger = logging.getLogger(__name__)

# AWS Configuration - using same credentials as staging
AWS_ACCESS_KEY_ID = "ASIAZYHN7XI6SJHG2IYS"
AWS_SECRET_ACCESS_KEY = "J1gUJkFCD56VKhyjkC8Ema+RfuwwAxphvS8GC3Jq"
AWS_SESSION_TOKEN = "FwoGZXIvYXdzEOj//////////wEaDGRdqp1tmWssuWSvziLWAX68UXEWe+GYyRaQpdTvG2CYABGE1z2YuUAham+71MnXE+o/dM/qERvUrbkFRg6lfFOILRytUbr/PwiWCdPYad9s5uK+uTzRucOFxpo8lNbD8LUnwIoLiKkA5DdHxK/qsrLPaQX0de4LUvNhBzW7qarP5rLm0G67CmW4lWmfvhp2xcF0CXZWRgk0UkJ+5DaNdvMnOz6IuQQUaAtQlpOZ9i8KuydmOYlk/5b5ybyvdme1vf0oD7iIMQaDdDlN6vCzc7p7VYQPT1vBQwEkF8BBrQcfUa4grGso2LXfxQYyM0qC+4aDBNUmrXGXr5s8ngKDmYfrENGAQAWd50UU3gvU8et5rkUhtXOjY8Q8JweFHHAzcA=="
AWS_REGION = "us-east-1"
S3_WAREHOUSE_PATH = "s3://batch-transpiler/testing-batch-processing/"
ICEBERG_CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "glue_catalog")

# Global catalog instance (initialized once per process)
_iceberg_catalog = None


def get_iceberg_catalog():
    """Get or initialize the Iceberg catalog (singleton pattern)"""
    global _iceberg_catalog
    
    if _iceberg_catalog is None:
        try:
            logger.info(f"Initializing AWS Glue Iceberg catalog with warehouse: {S3_WAREHOUSE_PATH}")
            
            _iceberg_catalog = load_catalog(
                name=ICEBERG_CATALOG_NAME,
                **{
                    "type": "glue",
                    "s3.access-key-id": AWS_ACCESS_KEY_ID,
                    "s3.secret-access-key": AWS_SECRET_ACCESS_KEY,
                    "s3.session-token": AWS_SESSION_TOKEN,
                    "s3.region": AWS_REGION,
                    "warehouse": S3_WAREHOUSE_PATH,
                    "glue.region": AWS_REGION,
                    "glue.access-key-id": AWS_ACCESS_KEY_ID,
                    "glue.secret-access-key": AWS_SECRET_ACCESS_KEY,
                    "glue.session-token": AWS_SESSION_TOKEN
                }
            )
            
            logger.info("✅ AWS Glue Iceberg catalog initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Iceberg catalog: {str(e)}")
            raise
    
    return _iceberg_catalog


def get_table(table_name: str = "default.batch_statistics") -> Table:
    """Load Iceberg table"""
    catalog = get_iceberg_catalog()
    
    try:
        table = catalog.load_table(table_name)
        logger.debug(f"📋 Loaded Iceberg table: {table_name}")
        return table
    except Exception as e:
        logger.error(f"❌ Failed to load table {table_name}: {str(e)}")
        raise


def commit_staged_files_atomic(
    session_id: str,
    staged_files: List[str],
    metadata: Dict[str, Any],
    table_name: str = "default.batch_statistics"
) -> Dict[str, Any]:
    """
    Atomically commit all staged files to Iceberg table
    
    Args:
        session_id: Session identifier
        staged_files: List of S3 paths to staged Parquet files
        metadata: Session metadata (company_name, batch info, etc.)
        table_name: Iceberg table identifier
    
    Returns:
        Dict with commit results
    """
    if not staged_files:
        logger.warning(f"⚠️ No staged files to commit for session {session_id}")
        return {
            "session_id": session_id,
            "success": False,
            "error": "No staged files provided",
            "committed_files": 0,
            "total_rows": 0
        }
    
    table = get_table(table_name)
    commit_start = datetime.now()
    
    try:
        logger.info(f"🚀 Starting atomic commit for session {session_id}: {len(staged_files)} files")
        
        # Collect all staged files for batch append (PyArrow-free approach)
        logger.info(f"💾 Collecting {len(staged_files)} staged files for Iceberg batch append...")
        
        # Convert all S3 paths to Iceberg-compatible paths
        iceberg_file_paths = []
        total_rows_committed = 0
        
        for i, s3_path in enumerate(staged_files):
            try:
                # Convert S3 path to format expected by Iceberg
                # Remove s3:// prefix for Iceberg data file path
                iceberg_path = s3_path.replace('s3://', '')
                iceberg_file_paths.append(iceberg_path)
                
                logger.debug(f"📝 Prepared file {i+1}/{len(staged_files)}: {iceberg_path}")
                
            except Exception as file_error:
                logger.error(f"❌ Failed to process file path {s3_path}: {file_error}")
                raise
        
        # Perform batch append using PyIceberg's direct file append (avoiding PyArrow in worker)
        try:
            logger.info(f"🚀 Performing batch append of {len(iceberg_file_paths)} files to Iceberg...")
            
            # Use PyIceberg's fast_append method if available, otherwise use regular append
            if hasattr(table, 'fast_append'):
                # Try fast append first
                table.fast_append(iceberg_file_paths)
            else:
                # Fall back to regular append (this may still require PyArrow but in isolated context)
                logger.warning("fast_append not available, using regular append method")
                
                # Try to import and use PyArrow in isolated way
                try:
                    import pyarrow.parquet as pq
                    import s3fs
                    
                    # Create s3fs filesystem once
                    s3_fs = s3fs.S3FileSystem(
                        key=AWS_ACCESS_KEY_ID,
                        secret=AWS_SECRET_ACCESS_KEY,
                        token=AWS_SESSION_TOKEN,
                        client_kwargs={'region_name': AWS_REGION}
                    )
                    
                    # Read and append all files
                    for iceberg_path in iceberg_file_paths:
                        parquet_table = pq.read_table(iceberg_path, filesystem=s3_fs)
                        table.append(parquet_table)
                        total_rows_committed += len(parquet_table)
                        
                except Exception as pyarrow_error:
                    logger.error(f"❌ PyArrow-based append failed: {pyarrow_error}")
                    raise
                    
            logger.info(f"✅ Batch append completed successfully")
            
        except Exception as append_error:
            logger.error(f"❌ Failed to append files to Iceberg table: {str(append_error)}")
            raise
            
        # Transaction commits automatically when exiting context
        commit_duration = (datetime.now() - commit_start).total_seconds()
        
        logger.info(f"✅ Atomic commit completed for session {session_id}: "
                   f"{len(staged_files)} files, {total_rows_committed:,} rows in {commit_duration:.2f}s")
        
        return {
            "session_id": session_id,
            "success": True,
            "committed_files": len(staged_files),
            "total_rows": total_rows_committed,
            "commit_duration_seconds": commit_duration,
            "table_name": table_name,
            "committed_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        commit_duration = (datetime.now() - commit_start).total_seconds()
        error_msg = str(e)
        
        logger.error(f"❌ Atomic commit failed for session {session_id} after {commit_duration:.2f}s: {error_msg}")
        
        return {
            "session_id": session_id,
            "success": False,
            "error": error_msg,
            "committed_files": 0,
            "total_rows": 0,
            "commit_duration_seconds": commit_duration,
            "failed_at": datetime.now().isoformat()
        }


def commit_staged_files_chunked(
    session_id: str,
    staged_files: List[str],
    metadata: Dict[str, Any],
    chunk_size: int = 50,
    table_name: str = "default.batch_statistics"
) -> Dict[str, Any]:
    """
    Commit staged files in chunks (for very large sessions)
    
    Args:
        session_id: Session identifier
        staged_files: List of S3 paths to staged Parquet files
        metadata: Session metadata
        chunk_size: Number of files to commit per chunk
        table_name: Iceberg table identifier
    
    Returns:
        Dict with commit results
    """
    if not staged_files:
        return {
            "session_id": session_id,
            "success": False,
            "error": "No staged files provided"
        }
    
    # Split files into chunks
    file_chunks = [staged_files[i:i + chunk_size] for i in range(0, len(staged_files), chunk_size)]
    
    logger.info(f"🔄 Committing {len(staged_files)} files in {len(file_chunks)} chunks of {chunk_size}")
    
    total_committed = 0
    total_rows = 0
    successful_chunks = 0
    failed_chunks = []
    
    commit_start = datetime.now()
    
    for i, chunk in enumerate(file_chunks):
        try:
            logger.info(f"📝 Committing chunk {i+1}/{len(file_chunks)}: {len(chunk)} files")
            
            chunk_result = commit_staged_files_atomic(
                session_id=f"{session_id}_chunk_{i+1}",
                staged_files=chunk,
                metadata=metadata,
                table_name=table_name
            )
            
            if chunk_result["success"]:
                total_committed += chunk_result["committed_files"]
                total_rows += chunk_result["total_rows"]
                successful_chunks += 1
            else:
                failed_chunks.append({
                    "chunk_index": i,
                    "files": chunk,
                    "error": chunk_result["error"]
                })
                
        except Exception as e:
            failed_chunks.append({
                "chunk_index": i,
                "files": chunk,
                "error": str(e)
            })
            logger.error(f"❌ Failed to commit chunk {i+1}: {str(e)}")
    
    commit_duration = (datetime.now() - commit_start).total_seconds()
    success = len(failed_chunks) == 0
    
    result = {
        "session_id": session_id,
        "success": success,
        "total_files": len(staged_files),
        "committed_files": total_committed,
        "total_rows": total_rows,
        "successful_chunks": successful_chunks,
        "failed_chunks": len(failed_chunks),
        "commit_duration_seconds": commit_duration,
        "table_name": table_name
    }
    
    if failed_chunks:
        result["failed_chunk_details"] = failed_chunks
        logger.error(f"❌ Chunked commit partially failed: {len(failed_chunks)} chunks failed")
    else:
        logger.info(f"✅ Chunked commit completed successfully: "
                   f"{total_committed} files, {total_rows:,} rows in {commit_duration:.2f}s")
    
    result["completed_at"] = datetime.now().isoformat()
    return result


def validate_table_schema(table_name: str = "default.batch_statistics") -> Dict[str, Any]:
    """
    Validate that the Iceberg table exists and has expected schema
    
    Returns:
        Dict with validation results
    """
    try:
        table = get_table(table_name)
        schema = table.schema()
        
        # Expected column names for batch_statistics table
        expected_columns = {
            "query_id", "batch_id", "company_name", "event_date", "batch_number",
            "timestamp", "status", "executable", "from_dialect", "to_dialect",
            "original_query", "converted_query", "supported_functions",
            "unsupported_functions", "udf_list", "tables_list",
            "processing_time_ms", "error_message",
            "unsupported_functions_after_transpilation", "joins_list"
        }
        
        actual_columns = {field.name for field in schema.fields}
        
        missing_columns = expected_columns - actual_columns
        extra_columns = actual_columns - expected_columns
        
        validation_result = {
            "table_name": table_name,
            "table_exists": True,
            "schema_valid": len(missing_columns) == 0,
            "total_columns": len(actual_columns),
            "expected_columns": len(expected_columns),
            "missing_columns": list(missing_columns),
            "extra_columns": list(extra_columns),
            "partition_spec": str(table.spec()) if hasattr(table, 'spec') else None
        }
        
        if validation_result["schema_valid"]:
            logger.info(f"✅ Table {table_name} schema validation passed")
        else:
            logger.warning(f"⚠️ Table {table_name} schema validation failed: missing {missing_columns}")
        
        return validation_result
        
    except Exception as e:
        logger.error(f"❌ Table validation failed for {table_name}: {str(e)}")
        return {
            "table_name": table_name,
            "table_exists": False,
            "schema_valid": False,
            "error": str(e)
        }


def get_table_statistics(table_name: str = "default.batch_statistics") -> Dict[str, Any]:
    """Get basic statistics about the Iceberg table"""
    try:
        table = get_table(table_name)
        
        # Get table metadata
        metadata = table.metadata
        current_snapshot = metadata.current_snapshot()
        
        stats = {
            "table_name": table_name,
            "table_uuid": str(metadata.table_uuid),
            "format_version": metadata.format_version,
            "location": metadata.location,
            "current_snapshot_id": current_snapshot.snapshot_id if current_snapshot else None,
            "total_snapshots": len(metadata.snapshots),
            "partition_spec": str(table.spec()) if hasattr(table, 'spec') else None,
            "schema_columns": len(table.schema().fields)
        }
        
        if current_snapshot:
            stats.update({
                "snapshot_timestamp": current_snapshot.timestamp_ms,
                "total_data_files": len(current_snapshot.manifest_list) if hasattr(current_snapshot, 'manifest_list') else None,
                "operation": current_snapshot.summary.get('operation') if hasattr(current_snapshot, 'summary') else None
            })
        
        logger.debug(f"📊 Retrieved statistics for table {table_name}")
        return stats
        
    except Exception as e:
        logger.error(f"❌ Failed to get table statistics for {table_name}: {str(e)}")
        return {
            "table_name": table_name,
            "error": str(e)
        }