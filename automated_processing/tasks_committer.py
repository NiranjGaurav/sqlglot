"""
Celery Committer Tasks for Iceberg Operations
Single-threaded tasks that commit staged files to Iceberg tables atomically
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from celery import Task

from . import staging
from . import iceberg_io

logger = logging.getLogger(__name__)


class CommitterTask(Task):
    """Base task class for Iceberg commit operations"""

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Handle committer task failure"""
        logger.error(f"❌ Committer task {task_id} failed: {str(exc)}")
        # Don't clean up staging files on failure - they can be retried
        super().on_failure(exc, task_id, args, kwargs, einfo)

    def on_success(self, retval, task_id, args, kwargs):
        """Handle committer task success"""
        if isinstance(retval, dict) and retval.get("success"):
            session_id = retval.get("session_id", "unknown")
            committed_files = retval.get("committed_files", 0)
            logger.info(
                f"✅ Committer task {task_id} completed: {committed_files} files committed for session {session_id}"
            )
        super().on_success(retval, task_id, args, kwargs)


def commit_session_to_iceberg(
    session_id: str,
    session_metadata: Dict[str, Any],
    table_name: str = "default.batch_statistics",
    cleanup_staging: bool = True,
    use_chunked_commit: bool = False,
    chunk_size: int = 50,
) -> Dict[str, Any]:
    """
    Commit all staged files for a session to Iceberg table atomically

    This task MUST run on the single-threaded iceberg_commit queue

    Args:
        session_id: Unique session identifier
        session_metadata: Session processing metadata
        table_name: Iceberg table to commit to
        cleanup_staging: Whether to clean up staging files after successful commit
        use_chunked_commit: Whether to use chunked commits for large sessions
        chunk_size: Number of files per chunk (if using chunked commit)

    Returns:
        Dict with commit results
    """
    commit_start_time = datetime.now()

    logger.info(f"🚀 Starting Iceberg commit for session {session_id}")

    try:
        # Step 1: Validate staged files exist
        staged_files = staging.list_staged_files(session_id)

        if not staged_files:
            # Check if session was already completed by looking for manifest with commit data
            try:
                from .staging import read_staging_manifest
                manifest = read_staging_manifest(session_id)
                
                if manifest and manifest.get("iceberg_commit"):
                    # Session was already committed successfully
                    iceberg_commit = manifest["iceberg_commit"]
                    logger.info(f"✅ Session {session_id} already completed - no action needed")
                    return {
                        "session_id": session_id,
                        "success": True,
                        "error": "Session already completed",
                        "committed_files": iceberg_commit.get("committed_files", 0),
                        "total_rows": iceberg_commit.get("total_rows", 0),
                        "commit_duration_seconds": 0,
                        "already_completed": True,
                    }
            except Exception as e:
                logger.debug(f"Could not check manifest for session {session_id}: {e}")
            
            # No staged files and no evidence of completion - this is an error
            logger.warning(f"⚠️ No staged files found for session {session_id}")
            return {
                "session_id": session_id,
                "success": False,
                "error": "No staged files found",
                "committed_files": 0,
                "total_rows": 0,
                "commit_duration_seconds": 0,
            }

        logger.info(f"📋 Found {len(staged_files)} staged files for session {session_id}")

        # Step 2: Validate staged files are readable
        validation_result = staging.validate_staged_files(session_id)

        if not validation_result["is_valid"]:
            logger.error(
                f"❌ Staged file validation failed for session {session_id}: "
                f"{validation_result['invalid_files']} invalid files"
            )
            return {
                "session_id": session_id,
                "success": False,
                "error": f"Invalid staged files: {validation_result['errors']}",
                "validation_errors": validation_result["errors"],
                "committed_files": 0,
                "total_rows": 0,
                "commit_duration_seconds": (datetime.now() - commit_start_time).total_seconds(),
            }

        logger.info(
            f"✅ Validation passed: {validation_result['valid_files']} files, "
            f"{validation_result['total_rows']:,} total rows"
        )

        # Step 3: Convert file paths to metadata format for new transaction-based API
        import s3fs
        import pyarrow.parquet as pq
        from automated_processing.iceberg_io import (
            AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY,
            AWS_SESSION_TOKEN,
            AWS_REGION,
            CREDENTIAL_SOURCE,
        )

        # Use credential detection similar to other files
        if CREDENTIAL_SOURCE == "environment":
            s3_fs = s3fs.S3FileSystem(
                key=AWS_ACCESS_KEY_ID,
                secret=AWS_SECRET_ACCESS_KEY,
                token=AWS_SESSION_TOKEN,
                client_kwargs={"region_name": AWS_REGION},
            )
        else:
            # Use IAM role (automatic credential detection)
            s3_fs = s3fs.S3FileSystem(
                client_kwargs={"region_name": AWS_REGION},
            )

        # Convert file paths to metadata format expected by new transaction API
        staged_files_with_metadata = []
        for s3_path in staged_files:
            try:
                pq_file = pq.ParquetFile(s3_path, filesystem=s3_fs)
                row_count = pq_file.metadata.num_rows
                file_size = s3_fs.size(s3_path)

                staged_files_with_metadata.append(
                    {
                        "path": s3_path,
                        "row_count": row_count,
                        "file_size": file_size,
                        "partition": {
                            "company_name": session_metadata.get("company_name", "unknown")
                        },
                    }
                )
            except Exception as e:
                logger.error(f"❌ Failed to read metadata for {s3_path}: {e}")
                return {
                    "session_id": session_id,
                    "success": False,
                    "error": f"Failed to read file metadata: {e}",
                    "committed_files": 0,
                    "total_rows": 0,
                    "commit_duration_seconds": (datetime.now() - commit_start_time).total_seconds(),
                }

        logger.info(f"📊 Prepared {len(staged_files_with_metadata)} files with metadata")

        # Step 4: Use streaming commit (commit every 10 files for better performance)
        logger.info(
            f"💾 Using streaming commit approach: {len(staged_files_with_metadata)} files in chunks of 10"
        )

        commit_result = iceberg_io.commit_staged_files_streaming(
            session_id=session_id,
            staged_files=staged_files_with_metadata,
            table_name=table_name,
            chunk_size=10,  # Commit every 10 files immediately
        )

        # Step 4: Handle commit results
        if not commit_result["success"]:
            logger.error(
                f"❌ Iceberg commit failed for session {session_id}: {commit_result.get('error')}"
            )
            return {**commit_result, "cleanup_attempted": False, "staging_files_preserved": True}

        logger.info(
            f"✅ Iceberg commit successful for session {session_id}: "
            f"{commit_result['committed_files']} files, "
            f"{commit_result['total_rows']:,} rows"
        )

        # Step 5: Clean up staging files (optional)
        cleanup_success = True
        if cleanup_staging:
            try:
                cleanup_success = staging.cleanup_staging_files(
                    session_id=session_id,
                    keep_manifest=True,  # Keep manifest for audit trail
                )

                if cleanup_success:
                    logger.info(f"🧹 Staging cleanup completed for session {session_id}")
                else:
                    logger.warning(f"⚠️ Staging cleanup partially failed for session {session_id}")

            except Exception as cleanup_error:
                logger.warning(
                    f"⚠️ Staging cleanup error for session {session_id}: {str(cleanup_error)}"
                )
                cleanup_success = False

        # Step 6: Update manifest with commit information
        try:
            manifest = staging.read_staging_manifest(session_id)
            if manifest:
                # Update manifest with commit results
                manifest["iceberg_commit"] = {
                    "committed_at": datetime.now().isoformat(),
                    "table_name": table_name,
                    "committed_files": commit_result["committed_files"],
                    "total_rows": commit_result["total_rows"],
                    "commit_duration_seconds": commit_result["commit_duration_seconds"],
                    "cleanup_performed": cleanup_staging,
                    "cleanup_success": cleanup_success,
                }

                # Write updated manifest
                staging.write_staging_manifest(
                    session_id=session_id,
                    staged_files=manifest.get("staged_files", []),
                    metadata=manifest,
                )
        except Exception as manifest_error:
            logger.warning(
                f"⚠️ Failed to update manifest for session {session_id}: {str(manifest_error)}"
            )

        # Return comprehensive results
        total_duration = (datetime.now() - commit_start_time).total_seconds()

        return {
            **commit_result,
            "cleanup_attempted": cleanup_staging,
            "cleanup_success": cleanup_success,
            "total_commit_duration_seconds": total_duration,
            "staging_files_preserved": not cleanup_staging or not cleanup_success,
            "validation_results": validation_result,
            "commit_type": commit_result.get("commit_type", "streaming"),
        }

    except Exception as e:
        total_duration = (datetime.now() - commit_start_time).total_seconds()
        error_msg = str(e)

        logger.error(
            f"❌ Critical error during Iceberg commit for session {session_id}: {error_msg}"
        )

        return {
            "session_id": session_id,
            "success": False,
            "error": error_msg,
            "committed_files": 0,
            "total_rows": 0,
            "commit_duration_seconds": 0,
            "total_commit_duration_seconds": total_duration,
            "cleanup_attempted": False,
            "staging_files_preserved": True,
            "failed_at": datetime.now().isoformat(),
        }


def retry_failed_commit(
    session_id: str,
    original_error: str,
    retry_attempt: int = 1,
    max_retries: int = 3,
    table_name: str = "default.batch_statistics",
) -> Dict[str, Any]:
    """
    Retry a failed Iceberg commit

    Args:
        session_id: Session that failed to commit
        original_error: Error message from original commit attempt
        retry_attempt: Current retry attempt number
        max_retries: Maximum number of retry attempts
        table_name: Iceberg table name

    Returns:
        Dict with retry results
    """
    if retry_attempt > max_retries:
        logger.error(f"❌ Max retries ({max_retries}) exceeded for session {session_id}")
        return {
            "session_id": session_id,
            "success": False,
            "error": f"Max retries exceeded. Original error: {original_error}",
            "retry_attempt": retry_attempt,
            "max_retries_exceeded": True,
        }

    logger.info(
        f"🔄 Retrying Iceberg commit for session {session_id} (attempt {retry_attempt}/{max_retries})"
    )

    try:
        # Check if staged files still exist
        staged_files = staging.list_staged_files(session_id)

        if not staged_files:
            logger.error(f"❌ No staged files found for retry of session {session_id}")
            return {
                "session_id": session_id,
                "success": False,
                "error": "No staged files found for retry",
                "retry_attempt": retry_attempt,
                "staged_files_missing": True,
            }

        # Get original session metadata from manifest
        manifest = staging.read_staging_manifest(session_id)
        session_metadata = manifest.get("metadata", {}) if manifest else {}

        # Attempt commit with more conservative settings
        commit_result = commit_session_to_iceberg(
            session_id=session_id,
            session_metadata=session_metadata,
            table_name=table_name,
            cleanup_staging=True,  # Clean up on successful retry
            use_chunked_commit=True,  # Use chunked commit for reliability
            chunk_size=10,  # Smaller chunks for retry
        )

        if commit_result["success"]:
            logger.info(f"✅ Retry successful for session {session_id} on attempt {retry_attempt}")
            commit_result["retry_attempt"] = retry_attempt
            commit_result["retry_successful"] = True
        else:
            logger.warning(
                f"⚠️ Retry attempt {retry_attempt} failed for session {session_id}: {commit_result.get('error')}"
            )
            commit_result["retry_attempt"] = retry_attempt
            commit_result["retry_successful"] = False

        return commit_result

    except Exception as e:
        logger.error(
            f"❌ Error during retry attempt {retry_attempt} for session {session_id}: {str(e)}"
        )

        return {
            "session_id": session_id,
            "success": False,
            "error": str(e),
            "retry_attempt": retry_attempt,
            "retry_successful": False,
            "original_error": original_error,
        }


def commit_multiple_sessions(
    session_ids: List[str],
    table_name: str = "default.batch_statistics",
    cleanup_staging: bool = True,
) -> Dict[str, Any]:
    """
    Commit multiple sessions sequentially (for batch cleanup operations)

    Args:
        session_ids: List of session identifiers to commit
        table_name: Iceberg table name
        cleanup_staging: Whether to clean up staging after commits

    Returns:
        Dict with results for all sessions
    """
    logger.info(f"🔄 Starting multi-session commit: {len(session_ids)} sessions")

    results = {
        "total_sessions": len(session_ids),
        "successful_commits": 0,
        "failed_commits": 0,
        "session_results": {},
        "summary": {
            "total_files_committed": 0,
            "total_rows_committed": 0,
            "total_duration_seconds": 0,
        },
    }

    start_time = datetime.now()

    for i, session_id in enumerate(session_ids):
        try:
            logger.info(f"📝 Committing session {i + 1}/{len(session_ids)}: {session_id}")

            # Get session metadata from manifest
            manifest = staging.read_staging_manifest(session_id)
            session_metadata = manifest.get("metadata", {}) if manifest else {}

            # Commit this session
            commit_result = commit_session_to_iceberg(
                session_id=session_id,
                session_metadata=session_metadata,
                table_name=table_name,
                cleanup_staging=cleanup_staging,
                use_chunked_commit=False,  # Use atomic commits for multi-session
            )

            results["session_results"][session_id] = commit_result

            if commit_result["success"]:
                results["successful_commits"] += 1
                results["summary"]["total_files_committed"] += commit_result.get(
                    "committed_files", 0
                )
                results["summary"]["total_rows_committed"] += commit_result.get("total_rows", 0)
            else:
                results["failed_commits"] += 1
                logger.warning(
                    f"⚠️ Failed to commit session {session_id}: {commit_result.get('error')}"
                )

        except Exception as e:
            results["failed_commits"] += 1
            results["session_results"][session_id] = {
                "success": False,
                "error": str(e),
                "session_id": session_id,
            }
            logger.error(f"❌ Error committing session {session_id}: {str(e)}")

    total_duration = (datetime.now() - start_time).total_seconds()
    results["summary"]["total_duration_seconds"] = total_duration

    logger.info(
        f"✅ Multi-session commit complete: {results['successful_commits']} successful, "
        f"{results['failed_commits']} failed, {total_duration:.2f}s total"
    )

    return results


def get_commit_queue_status() -> Dict[str, Any]:
    """
    Get status information about the Iceberg commit queue

    Returns:
        Dict with queue status information
    """
    try:
        # This would typically inspect the Celery queue
        # For now, return basic information
        return {
            "queue_name": "iceberg_commit",
            "queue_type": "single_threaded",
            "concurrency": 1,
            "pool_type": "solo",
            "status": "active",
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error(f"❌ Error getting commit queue status: {str(e)}")
        return {
            "queue_name": "iceberg_commit",
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat(),
        }
