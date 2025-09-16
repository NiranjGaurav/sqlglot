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

# AWS Configuration - ONLY use environment variables (no fallback credentials)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_WAREHOUSE_PATH = os.getenv("S3_WAREHOUSE_PATH", "s3://batch-transpiler/testing-batch-processing/")
ICEBERG_CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "glue_catalog")

# Validate required AWS credentials
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    raise EnvironmentError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables are required")

# Global catalog instance (initialized once per process)
_iceberg_catalog = None


def get_iceberg_catalog():
    """Get or initialize the Iceberg catalog (singleton pattern)"""
    global _iceberg_catalog

    if _iceberg_catalog is None:
        try:
            logger.info(
                f"Initializing AWS Glue Iceberg catalog with warehouse: {S3_WAREHOUSE_PATH}"
            )
            logger.info(f"Using AWS_REGION: {AWS_REGION}")
            logger.info(f"Using AWS_ACCESS_KEY_ID: {AWS_ACCESS_KEY_ID[:10]}...")

            catalog_config = {
                "type": "glue",
                # PyIceberg AWS Glue catalog configuration (correct parameter names)
                "glue.region": AWS_REGION,
                "client.region": AWS_REGION,
                "glue.access-key-id": AWS_ACCESS_KEY_ID,
                "glue.secret-access-key": AWS_SECRET_ACCESS_KEY,
                "glue.session-token": AWS_SESSION_TOKEN,
                "warehouse": S3_WAREHOUSE_PATH,
                # S3 specific configurations
                "s3.access-key-id": AWS_ACCESS_KEY_ID,
                "s3.secret-access-key": AWS_SECRET_ACCESS_KEY,
                "s3.session-token": AWS_SESSION_TOKEN,
                "s3.region": AWS_REGION,
            }
            
            logger.info(f"Catalog config keys: {list(catalog_config.keys())}")
            
            _iceberg_catalog = load_catalog(
                name=ICEBERG_CATALOG_NAME,
                **catalog_config
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


from datetime import datetime
import logging
from typing import List, Dict, Any
from pyiceberg.manifest import DataFile, FileFormat, DataFileContent

logger = logging.getLogger(__name__)


def commit_staged_files_streaming(
    session_id: str,
    staged_files: List[
        Dict[str, Any]
    ],  # list of dicts: {"path": str, "row_count": int, "file_size": int, "partition": Optional[dict]}
    table_name: str = "default.batch_statistics",
    chunk_size: int = 10,
) -> Dict[str, Any]:
    """
    Commit staged files in streaming chunks (commit every 10 files immediately)

    This happens AFTER transpilation → manifest creation → validation.
    Instead of committing all files at once, commit in chunks of 10 for better performance.

    Args:
        session_id: Session identifier
        staged_files: List of dicts containing S3 paths and metadata
        table_name: Iceberg table identifier
        chunk_size: Number of files to commit per chunk (default: 10)

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
            "total_rows": 0,
        }

    table = get_table(table_name)
    overall_commit_start = datetime.now()

    # Split files into chunks of 10
    file_chunks = [
        staged_files[i : i + chunk_size] for i in range(0, len(staged_files), chunk_size)
    ]

    logger.info(
        f"🚀 Starting streaming commit for session {session_id}: {len(staged_files)} files in {len(file_chunks)} chunks"
    )

    total_committed_files = 0
    total_committed_rows = 0
    successful_chunks = 0
    failed_chunks = []

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        import s3fs

        # Set up S3 filesystem
        s3_fs = s3fs.S3FileSystem(
            key=AWS_ACCESS_KEY_ID,
            secret=AWS_SECRET_ACCESS_KEY,
            token=AWS_SESSION_TOKEN,
            client_kwargs={"region_name": AWS_REGION},
        )

        # Process each chunk immediately
        for chunk_idx, chunk_files in enumerate(file_chunks):
            chunk_start = datetime.now()
            chunk_id = f"{session_id}_chunk_{chunk_idx + 1}"

            logger.info(
                f"📦 Committing chunk {chunk_idx + 1}/{len(file_chunks)}: {len(chunk_files)} files"
            )

            try:
                # Read data from this chunk's files
                tables = []
                chunk_rows = 0

                for file_info in chunk_files:
                    file_path = file_info["path"]
                    logger.debug(f"📖 Reading file: {file_path}")

                    # Read parquet file from S3
                    table_data = pq.read_table(file_path, filesystem=s3_fs)
                    tables.append(table_data)
                    chunk_rows += len(table_data)

                # Concatenate and commit this chunk
                if tables:
                    combined_table = pa.concat_tables(tables)
                    logger.info(
                        f"📊 Chunk {chunk_idx + 1}: Combined {len(tables)} files with {chunk_rows:,} rows"
                    )

                    # Append to Iceberg table (copies data to final location)
                    table.append(combined_table)

                    # Update counters
                    total_committed_files += len(chunk_files)
                    total_committed_rows += chunk_rows
                    successful_chunks += 1

                    chunk_duration = (datetime.now() - chunk_start).total_seconds()
                    logger.info(
                        f"✅ Chunk {chunk_idx + 1} committed: {len(chunk_files)} files, {chunk_rows:,} rows in {chunk_duration:.2f}s"
                    )

            except Exception as chunk_error:
                chunk_duration = (datetime.now() - chunk_start).total_seconds()
                logger.error(
                    f"❌ Chunk {chunk_idx + 1} failed after {chunk_duration:.2f}s: {chunk_error}"
                )

                failed_chunks.append(
                    {
                        "chunk_index": chunk_idx + 1,
                        "chunk_id": chunk_id,
                        "files_count": len(chunk_files),
                        "error": str(chunk_error),
                        "duration": chunk_duration,
                    }
                )

        # Calculate overall results
        overall_duration = (datetime.now() - overall_commit_start).total_seconds()
        success = len(failed_chunks) == 0

        if success:
            logger.info(
                f"✅ Streaming commit completed successfully: {total_committed_files} files, {total_committed_rows:,} rows in {overall_duration:.2f}s"
            )
        else:
            logger.error(
                f"❌ Streaming commit partially failed: {len(failed_chunks)} chunks failed out of {len(file_chunks)}"
            )

        return {
            "session_id": session_id,
            "success": success,
            "committed_files": total_committed_files,
            "total_rows": total_committed_rows,
            "commit_duration_seconds": overall_duration,
            "table_name": table_name,
            "committed_at": datetime.now().isoformat(),
            "commit_type": "streaming",
            "total_chunks": len(file_chunks),
            "successful_chunks": successful_chunks,
            "failed_chunks": len(failed_chunks),
            "chunk_size": chunk_size,
            "failed_chunk_details": failed_chunks if failed_chunks else [],
        }

    except Exception as e:
        overall_duration = (datetime.now() - overall_commit_start).total_seconds()
        logger.error(f"❌ Streaming commit failed after {overall_duration:.2f}s: {e}")

        return {
            "session_id": session_id,
            "success": False,
            "error": str(e),
            "committed_files": total_committed_files,
            "total_rows": total_committed_rows,
            "commit_duration_seconds": overall_duration,
            "failed_at": datetime.now().isoformat(),
            "commit_type": "streaming_failed",
        }


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
            "query_id",
            "batch_id",
            "company_name",
            "event_date",
            "batch_number",
            "timestamp",
            "status",
            "executable",
            "from_dialect",
            "to_dialect",
            "original_query",
            "converted_query",
            "supported_functions",
            "unsupported_functions",
            "udf_list",
            "tables_list",
            "processing_time_ms",
            "error_message",
            "unsupported_functions_after_transpilation",
            "joins_list",
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
            "partition_spec": str(table.spec()) if hasattr(table, "spec") else None,
        }

        if validation_result["schema_valid"]:
            logger.info(f"✅ Table {table_name} schema validation passed")
        else:
            logger.warning(
                f"⚠️ Table {table_name} schema validation failed: missing {missing_columns}"
            )

        return validation_result

    except Exception as e:
        logger.error(f"❌ Table validation failed for {table_name}: {str(e)}")
        return {
            "table_name": table_name,
            "table_exists": False,
            "schema_valid": False,
            "error": str(e),
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
            "partition_spec": str(table.spec()) if hasattr(table, "spec") else None,
            "schema_columns": len(table.schema().fields),
        }

        if current_snapshot:
            stats.update(
                {
                    "snapshot_timestamp": current_snapshot.timestamp_ms,
                    "total_data_files": len(current_snapshot.manifest_list)
                    if hasattr(current_snapshot, "manifest_list")
                    else None,
                    "operation": current_snapshot.summary.get("operation")
                    if hasattr(current_snapshot, "summary")
                    else None,
                }
            )

        logger.debug(f"📊 Retrieved statistics for table {table_name}")
        return stats

    except Exception as e:
        logger.error(f"❌ Failed to get table statistics for {table_name}: {str(e)}")
        return {"table_name": table_name, "error": str(e)}
