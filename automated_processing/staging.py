"""
S3 Staging Helper Functions
Manages Parquet file staging in S3 before Iceberg commits
"""
import os
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import s3fs

logger = logging.getLogger(__name__)

# S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "batch-transpiler")
S3_STAGING_PREFIX = os.getenv("S3_STAGING_PREFIX", "staging")
AWS_ACCESS_KEY_ID = "ASIAZYHN7XI6SJHG2IYS"
AWS_SECRET_ACCESS_KEY = "J1gUJkFCD56VKhyjkC8Ema+RfuwwAxphvS8GC3Jq"
AWS_SESSION_TOKEN = "FwoGZXIvYXdzEOj//////////wEaDGRdqp1tmWssuWSvziLWAX68UXEWe+GYyRaQpdTvG2CYABGE1z2YuUAham+71MnXE+o/dM/qERvUrbkFRg6lfFOILRytUbr/PwiWCdPYad9s5uK+uTzRucOFxpo8lNbD8LUnwIoLiKkA5DdHxK/qsrLPaQX0de4LUvNhBzW7qarP5rLm0G67CmW4lWmfvhp2xcF0CXZWRgk0UkJ+5DaNdvMnOz6IuQQUaAtQlpOZ9i8KuydmOYlk/5b5ybyvdme1vf0oD7iIMQaDdDlN6vCzc7p7VYQPT1vBQwEkF8BBrQcfUa4grGso2LXfxQYyM0qC+4aDBNUmrXGXr5s8ngKDmYfrENGAQAWd50UU3gvU8et5rkUhtXOjY8Q8JweFHHAzcA=="
AWS_REGION = "us-east-1"


def get_s3_filesystem() -> s3fs.S3FileSystem:
    """Get configured S3 filesystem using s3fs (safer for multiprocessing)"""
    return s3fs.S3FileSystem(
        key=AWS_ACCESS_KEY_ID,
        secret=AWS_SECRET_ACCESS_KEY,
        token=AWS_SESSION_TOKEN,
        client_kwargs={'region_name': AWS_REGION},
        config_kwargs={'connect_timeout': 30, 'read_timeout': 60}
    )


def generate_staging_path(session_id: str, batch_id: int) -> str:
    """Generate unique S3 staging path for a batch"""
    return f"s3://{S3_BUCKET}/{S3_STAGING_PREFIX}/{session_id}/batch_{batch_id:04d}.parquet"


def generate_manifest_path(session_id: str) -> str:
    """Generate S3 path for the staging manifest"""
    return f"s3://{S3_BUCKET}/{S3_STAGING_PREFIX}/{session_id}/manifest.json"


def write_native_data_to_staging(
    data: Dict[str, list],
    session_id: str, 
    batch_id: int,
    metadata: Optional[Dict[str, Any]] = None
) -> str:
    """
    Write native Python data to S3 staging area as Parquet
    
    Args:
        data: Dictionary of column_name -> list of values (native Python types)
        session_id: Session identifier
        batch_id: Batch identifier
        metadata: Optional metadata
    
    Returns:
        str: S3 path where file was written
    """
    # Import PyArrow inside function to prevent segfaults in multiprocessing
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    s3_path = generate_staging_path(session_id, batch_id)
    s3fs = get_s3_filesystem()
    
    # Parse S3 path
    bucket_and_key = s3_path.replace('s3://', '')
    
    try:
        # Convert native Python data to PyArrow table (ONLY in staging.py)
        pa_data = {}
        for col_name, values in data.items():
            if col_name == "query_id":
                pa_data[col_name] = pa.array(values, type=pa.int64())
            elif col_name == "batch_number":
                pa_data[col_name] = pa.array(values, type=pa.int32())
            elif col_name == "processing_time_ms":
                pa_data[col_name] = pa.array(values, type=pa.int64())
            elif col_name == "timestamp":
                pa_data[col_name] = pa.array(values, type=pa.timestamp('us'))
            elif col_name in ["supported_functions", "unsupported_functions", "udf_list", "tables_list", "unsupported_functions_after_transpilation", "joins_list"]:
                pa_data[col_name] = pa.array(values, type=pa.list_(pa.string()))
            else:
                # Default to string type for most columns
                pa_data[col_name] = pa.array(values, type=pa.string())
        
        # Create PyArrow table from native data
        table = pa.table(pa_data)
        
        # Write parquet file to S3 using s3fs
        with s3fs.open(bucket_and_key, 'wb') as f:
            # Add metadata if provided (attach to table schema, not write_table function)
            if metadata:
                # Store metadata as custom key-value pairs in table schema
                schema_metadata = {f"staging_{k}": str(v) for k, v in metadata.items()}
                # Replace table schema with metadata
                new_schema = table.schema.with_metadata(schema_metadata)
                table = table.cast(new_schema)
            
            pq.write_table(
                table, 
                f, 
                compression='snappy',
                write_statistics=True
            )
        
        logger.info(f"✅ Staged {len(table):,} rows to {s3_path}")
        return s3_path
        
    except Exception as e:
        logger.error(f"❌ Failed to stage batch {batch_id} for session {session_id}: {str(e)}")
        raise


def write_parquet_to_staging(
    table, 
    session_id: str, 
    batch_id: int,
    metadata: Optional[Dict[str, Any]] = None
) -> str:
    """
    Write PyArrow table to S3 staging area
    
    Returns:
        str: S3 path where file was written
    """
    # Import PyArrow inside function to prevent segfaults in multiprocessing
    import pyarrow.parquet as pq
    
    s3_path = generate_staging_path(session_id, batch_id)
    s3fs = get_s3_filesystem()
    
    # Parse S3 path
    bucket_and_key = s3_path.replace('s3://', '')
    
    try:
        # Write parquet file to S3 using s3fs
        with s3fs.open(bucket_and_key, 'wb') as f:
            # Add metadata if provided (attach to table schema, not write_table function)
            if metadata:
                # Store metadata as custom key-value pairs in table schema
                schema_metadata = {f"staging_{k}": str(v) for k, v in metadata.items()}
                # Replace table schema with metadata
                new_schema = table.schema.with_metadata(schema_metadata)
                table = table.cast(new_schema)
            
            pq.write_table(
                table, 
                f, 
                compression='snappy',
                write_statistics=True
            )
        
        logger.info(f"✅ Staged {len(table):,} rows to {s3_path}")
        return s3_path
        
    except Exception as e:
        logger.error(f"❌ Failed to stage batch {batch_id} for session {session_id}: {str(e)}")
        raise


def list_staged_files(session_id: str) -> List[str]:
    """
    List all staged Parquet files for a session
    
    Returns:
        List[str]: List of S3 paths to staged files
    """
    s3fs = get_s3_filesystem()
    staging_prefix = f"{S3_BUCKET}/{S3_STAGING_PREFIX}/{session_id}/"
    
    try:
        # List files using s3fs
        files = s3fs.ls(staging_prefix, detail=False)
        
        staged_files = []
        for file_path in files:
            if file_path.endswith('.parquet'):
                staged_files.append(f"s3://{file_path}")
        
        logger.info(f"📂 Found {len(staged_files)} staged files for session {session_id}")
        return sorted(staged_files)  # Sort for consistent ordering
        
    except Exception as e:
        logger.warning(f"⚠️ Failed to list staged files for session {session_id}: {str(e)}")
        return []


def write_staging_manifest(session_id: str, staged_files: List[str], metadata: Dict[str, Any]) -> str:
    """
    Write manifest.json with staging information
    
    Args:
        session_id: Session identifier
        staged_files: List of S3 paths to staged files
        metadata: Additional metadata to store
    
    Returns:
        str: S3 path to manifest file
    """
    manifest_path = generate_manifest_path(session_id)
    s3fs = get_s3_filesystem()
    
    manifest_data = {
        "session_id": session_id,
        "created_at": datetime.now().isoformat(),
        "staged_files": staged_files,
        "total_files": len(staged_files),
        "metadata": metadata,
        "staging_complete": True
    }
    
    try:
        bucket_and_key = manifest_path.replace('s3://', '')
        with s3fs.open(bucket_and_key, 'w') as f:
            json.dump(manifest_data, f, indent=2)
        
        logger.info(f"📋 Wrote staging manifest to {manifest_path}")
        return manifest_path
        
    except Exception as e:
        logger.error(f"❌ Failed to write staging manifest for session {session_id}: {str(e)}")
        raise


def read_staging_manifest(session_id: str) -> Optional[Dict[str, Any]]:
    """
    Read staging manifest for a session
    
    Returns:
        Dict or None if manifest doesn't exist
    """
    manifest_path = generate_manifest_path(session_id)
    s3fs = get_s3_filesystem()
    
    try:
        bucket_and_key = manifest_path.replace('s3://', '')
        
        # Check if manifest exists
        if not s3fs.exists(bucket_and_key):
            logger.debug(f"📋 No staging manifest found for session {session_id}")
            return None
        
        # Read manifest
        with s3fs.open(bucket_and_key, 'r') as f:
            manifest_data = json.load(f)
        
        logger.debug(f"📋 Read staging manifest for session {session_id}: {len(manifest_data.get('staged_files', []))} files")
        return manifest_data
        
    except Exception as e:
        logger.warning(f"⚠️ Failed to read staging manifest for session {session_id}: {str(e)}")
        return None


def cleanup_staging_files(session_id: str, keep_manifest: bool = False) -> bool:
    """
    Clean up staging files after successful commit
    
    Args:
        session_id: Session identifier
        keep_manifest: Whether to keep the manifest file
    
    Returns:
        bool: True if cleanup was successful
    """
    s3fs = get_s3_filesystem()
    staging_prefix = f"{S3_BUCKET}/{S3_STAGING_PREFIX}/{session_id}/"
    
    try:
        # List all files in the staging directory using s3fs
        files = s3fs.ls(staging_prefix, detail=False)
        
        deleted_count = 0
        for file_path in files:
            # Skip manifest if requested
            if keep_manifest and file_path.endswith('manifest.json'):
                continue
            
            try:
                s3fs.delete(file_path)
                deleted_count += 1
            except Exception as e:
                logger.warning(f"⚠️ Failed to delete {file_path}: {str(e)}")
        
        # Try to delete the empty directory
        try:
            s3fs.rmdir(staging_prefix.rstrip('/'))
        except:
            pass  # Directory might not be empty or deletion might fail
        
        logger.info(f"🧹 Cleaned up {deleted_count} staging files for session {session_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to cleanup staging files for session {session_id}: {str(e)}")
        return False


def get_staging_statistics(session_id: str) -> Dict[str, Any]:
    """
    Get statistics about staged files for a session
    
    Returns:
        Dict with statistics about staged files
    """
    staged_files = list_staged_files(session_id)
    manifest = read_staging_manifest(session_id)
    
    if not staged_files:
        return {
            "session_id": session_id,
            "total_files": 0,
            "total_size_bytes": 0,
            "manifest_exists": manifest is not None,
            "staging_complete": False
        }
    
    s3fs = get_s3_filesystem()
    total_size = 0
    
    # Calculate total size
    for s3_path in staged_files:
        try:
            bucket_and_key = s3_path.replace('s3://', '')
            file_info = s3fs.info(bucket_and_key)
            total_size += file_info['size']
        except:
            pass
    
    return {
        "session_id": session_id,
        "total_files": len(staged_files),
        "total_size_bytes": total_size,
        "total_size_mb": round(total_size / (1024 * 1024), 2),
        "staged_files": staged_files,
        "manifest_exists": manifest is not None,
        "staging_complete": manifest.get('staging_complete', False) if manifest else False,
        "manifest_data": manifest
    }


def validate_staged_files(session_id: str) -> Dict[str, Any]:
    """
    Validate that all staged files are readable and valid Parquet
    
    Returns:
        Dict with validation results
    """
    # Import PyArrow inside function to prevent segfaults in multiprocessing
    import pyarrow.parquet as pq
    
    staged_files = list_staged_files(session_id)
    s3fs = get_s3_filesystem()
    
    validation_results = {
        "session_id": session_id,
        "total_files": len(staged_files),
        "valid_files": 0,
        "invalid_files": 0,
        "total_rows": 0,
        "errors": []
    }
    
    for s3_path in staged_files:
        try:
            bucket_and_key = s3_path.replace('s3://', '')
            
            # Try to read parquet file
            parquet_file = pq.ParquetFile(bucket_and_key, filesystem=s3fs)
            num_rows = parquet_file.metadata.num_rows
            
            validation_results["valid_files"] += 1
            validation_results["total_rows"] += num_rows
            
            logger.debug(f"✅ Validated {s3_path}: {num_rows:,} rows")
            
        except Exception as e:
            validation_results["invalid_files"] += 1
            validation_results["errors"].append({
                "file": s3_path,
                "error": str(e)
            })
            logger.warning(f"❌ Invalid staged file {s3_path}: {str(e)}")
    
    validation_results["is_valid"] = validation_results["invalid_files"] == 0
    
    logger.info(f"🔍 Validation complete for session {session_id}: "
                f"{validation_results['valid_files']} valid, "
                f"{validation_results['invalid_files']} invalid, "
                f"{validation_results['total_rows']:,} total rows")
    
    return validation_results