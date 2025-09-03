"""
Orchestration Example: Group/Chord Pattern for Staging-Based Pipeline
Shows how to coordinate workers and committer with staging architecture
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from celery import group, chord
from celery.result import AsyncResult, GroupResult

# Import Celery app and tasks
from worker import celery
from worker import (
    process_batch_to_staging_task,
    create_staging_manifest_task_wrapper,
    commit_session_to_iceberg_task
)
from staging import get_staging_statistics, validate_staged_files
from iceberg_io import validate_table_schema

logger = logging.getLogger(__name__)


def orchestrate_staging_based_processing(
    session_id: str,
    batches_data: List[Dict[str, Any]],
    session_metadata: Dict[str, Any],
    use_chord: bool = True,
    cleanup_staging: bool = True
) -> Dict[str, Any]:
    """
    Orchestrate the complete staging-based pipeline
    
    Pipeline Flow:
    1. Workers process batches in parallel → stage Parquet files in S3
    2. Manifest task collects staging results → writes manifest.json
    3. Committer task atomically commits all staged files → Iceberg table
    4. Optional cleanup of staging files
    
    Args:
        session_id: Unique session identifier
        batches_data: List of batch configurations for workers
        session_metadata: Overall session metadata
        use_chord: Whether to use chord pattern (recommended)
        cleanup_staging: Whether to clean up staging files after commit
    
    Returns:
        Dict with orchestration results and task IDs
    """
    logger.info(f"🚀 Starting staging-based orchestration for session {session_id}: {len(batches_data)} batches")
    
    if not batches_data:
        return {
            "session_id": session_id,
            "success": False,
            "error": "No batches provided",
            "total_batches": 0
        }
    
    orchestration_start = datetime.now()
    
    try:
        # Step 1: Create worker tasks (parallel processing)
        worker_tasks = []
        for i, batch_data in enumerate(batches_data):
            worker_task = process_batch_to_staging_task.s(
                session_id=session_id,
                batch_id=i,
                queries_list=batch_data['queries_list'],
                metadata={**session_metadata, 'batch_id': i},
                testing=batch_data.get('testing', False)
            )
            worker_tasks.append(worker_task)
        
        if use_chord:
            # Use chord pattern: workers run in parallel, then committer runs once
            logger.info(f"📝 Using chord pattern: {len(worker_tasks)} workers → manifest → committer")
            
            # Create chord: workers in parallel, then manifest creation
            workers_group = group(worker_tasks)
            # IMPORTANT: Celery chord passes worker results as the first argument to the callback
            # The callback signature is: callback(worker_results, *args_from_signature)
            # So we don't include worker_results in .s() - it's automatically injected
            manifest_task = create_staging_manifest_task_wrapper.s(
                session_id,        # This becomes session_id (2nd parameter)
                session_metadata   # This becomes session_metadata (3rd parameter)
            )
            
            # Create the committer callback
            committer_task = commit_session_to_iceberg_task.s(
                session_id=session_id,
                session_metadata=session_metadata,
                cleanup_staging=cleanup_staging
            )
            
            # Chain: Workers → Manifest → Committer
            chord_result = chord(workers_group)(manifest_task)
            
            # Wait for manifest, then start committer
            manifest_result = chord_result.get()  # This blocks until all workers complete
            
            if manifest_result.get("ready_for_commit", False):
                logger.info(f"✅ Manifest ready, starting committer for session {session_id}")
                committer_result = committer_task.apply_async()
                committer_task_id = committer_result.id
            else:
                logger.error(f"❌ Manifest not ready for commit: {manifest_result}")
                return {
                    "session_id": session_id,
                    "success": False,
                    "error": "Manifest creation failed",
                    "manifest_result": manifest_result,
                    "orchestration_duration": (datetime.now() - orchestration_start).total_seconds()
                }
            
            return {
                "session_id": session_id,
                "orchestration_type": "chord",
                "total_batches": len(batches_data),
                "workers_group_id": workers_group.id if hasattr(workers_group, 'id') else None,
                "manifest_task_id": chord_result.id,
                "committer_task_id": committer_task_id,
                "manifest_result": manifest_result,
                "status": "orchestrated",
                "orchestration_duration": (datetime.now() - orchestration_start).total_seconds(),
                "expected_flow": "workers → manifest → committer"
            }
            
        else:
            # Manual orchestration without chord
            logger.info(f"⚙️ Using manual orchestration: {len(worker_tasks)} workers")
            
            # Start all workers
            workers_group = group(worker_tasks)
            workers_result = workers_group.apply_async()
            
            return {
                "session_id": session_id,
                "orchestration_type": "manual",
                "total_batches": len(batches_data),
                "workers_group_id": workers_result.id,
                "status": "workers_started",
                "orchestration_duration": (datetime.now() - orchestration_start).total_seconds(),
                "next_steps": [
                    "Monitor workers completion",
                    "Call create_manifest_and_commit() when ready"
                ]
            }
            
    except Exception as e:
        logger.error(f"❌ Orchestration failed for session {session_id}: {str(e)}")
        
        return {
            "session_id": session_id,
            "success": False,
            "error": str(e),
            "orchestration_duration": (datetime.now() - orchestration_start).total_seconds()
        }


def create_manifest_and_commit(
    session_id: str,
    session_metadata: Dict[str, Any],
    worker_results: Optional[List[Dict[str, Any]]] = None,
    cleanup_staging: bool = True
) -> Dict[str, Any]:
    """
    Create manifest and commit staged files (for manual orchestration)
    
    Args:
        session_id: Session identifier
        session_metadata: Session metadata
        worker_results: Results from worker tasks (optional - will auto-discover if None)
        cleanup_staging: Whether to clean up staging after commit
    
    Returns:
        Dict with manifest and commit results
    """
    logger.info(f"📋 Creating manifest and committing session {session_id}")
    
    try:
        # Step 1: Get worker results (if not provided)
        if worker_results is None:
            # Auto-discover staged files
            staging_stats = get_staging_statistics(session_id)
            
            if staging_stats["total_files"] == 0:
                return {
                    "session_id": session_id,
                    "success": False,
                    "error": "No staged files found",
                    "staging_stats": staging_stats
                }
            
            # Create synthetic worker results from staging stats
            worker_results = [{
                "session_id": session_id,
                "status": "completed",
                "staged_file": staged_file
            } for staged_file in staging_stats["staged_files"]]
        
        # Step 2: Create manifest
        manifest_task = create_staging_manifest_task_wrapper.apply_async(
            args=[worker_results, session_id, session_metadata]
        )
        manifest_result = manifest_task.get()
        
        if not manifest_result.get("ready_for_commit", False):
            return {
                "session_id": session_id,
                "success": False,
                "error": "Manifest creation failed",
                "manifest_result": manifest_result
            }
        
        # Step 3: Commit to Iceberg
        logger.info(f"💾 Starting Iceberg commit for session {session_id}")
        
        committer_task = commit_session_to_iceberg_task.apply_async(
            args=[session_id, session_metadata],
            kwargs={"cleanup_staging": cleanup_staging}
        )
        commit_result = committer_task.get()
        
        return {
            "session_id": session_id,
            "success": commit_result.get("success", False),
            "manifest_result": manifest_result,
            "commit_result": commit_result,
            "committer_task_id": committer_task.id,
            "total_duration": manifest_result.get("duration", 0) + commit_result.get("total_commit_duration_seconds", 0)
        }
        
    except Exception as e:
        logger.error(f"❌ Manual manifest and commit failed for session {session_id}: {str(e)}")
        
        return {
            "session_id": session_id,
            "success": False,
            "error": str(e)
        }


def monitor_session_progress(
    session_id: str,
    workers_group_id: Optional[str] = None,
    committer_task_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Monitor the progress of a staging-based session
    
    Args:
        session_id: Session identifier
        workers_group_id: Celery group ID for workers (optional)
        committer_task_id: Celery task ID for committer (optional)
    
    Returns:
        Dict with current session status
    """
    try:
        # Get staging statistics
        staging_stats = get_staging_statistics(session_id)
        
        # Check worker group status
        workers_status = {"status": "unknown"}
        if workers_group_id:
            try:
                group_result = GroupResult.restore(workers_group_id, backend=celery.backend)
                if group_result:
                    workers_status = {
                        "status": "completed" if group_result.ready() else "running",
                        "completed_tasks": group_result.completed_count(),
                        "total_tasks": len(group_result),
                        "successful_tasks": group_result.successful() if group_result.ready() else 0,
                        "failed_tasks": group_result.failed() if group_result.ready() else 0
                    }
            except Exception as e:
                workers_status = {"status": "error", "error": str(e)}
        
        # Check committer status
        committer_status = {"status": "not_started"}
        if committer_task_id:
            try:
                committer_result = AsyncResult(committer_task_id, backend=celery.backend)
                committer_status = {
                    "status": committer_result.status,
                    "result": committer_result.result if committer_result.ready() else None
                }
            except Exception as e:
                committer_status = {"status": "error", "error": str(e)}
        
        # Validate staged files if any exist
        validation_result = None
        if staging_stats["total_files"] > 0:
            try:
                validation_result = validate_staged_files(session_id)
            except Exception as e:
                validation_result = {"error": str(e)}
        
        return {
            "session_id": session_id,
            "timestamp": datetime.now().isoformat(),
            "staging_stats": staging_stats,
            "workers_status": workers_status,
            "committer_status": committer_status,
            "validation_result": validation_result,
            "overall_status": _determine_overall_status(workers_status, committer_status, staging_stats)
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to monitor session {session_id}: {str(e)}")
        
        return {
            "session_id": session_id,
            "status": "monitoring_error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


def _determine_overall_status(workers_status: Dict, committer_status: Dict, staging_stats: Dict) -> str:
    """Determine overall session status from component statuses"""
    
    # Check committer first (final stage)
    if committer_status["status"] == "SUCCESS":
        return "completed"
    elif committer_status["status"] in ["FAILURE", "REVOKED"]:
        return "failed"
    elif committer_status["status"] in ["PENDING", "STARTED", "RETRY"]:
        return "committing"
    
    # Check workers
    if workers_status["status"] == "completed":
        if staging_stats.get("total_files", 0) > 0:
            return "staged_ready_for_commit"
        else:
            return "workers_completed_no_files"
    elif workers_status["status"] == "running":
        return "processing"
    elif workers_status["status"] in ["failed", "error"]:
        return "failed"
    
    # Default
    return "unknown"


def cleanup_failed_session(session_id: str, keep_logs: bool = True) -> Dict[str, Any]:
    """
    Clean up staging files for a failed session
    
    Args:
        session_id: Session identifier
        keep_logs: Whether to keep manifest.json for debugging
    
    Returns:
        Dict with cleanup results
    """
    logger.info(f"🧹 Cleaning up failed session {session_id}")
    
    try:
        from staging import cleanup_staging_files
        
        # Get staging stats before cleanup
        staging_stats = get_staging_statistics(session_id)
        
        if staging_stats["total_files"] == 0:
            return {
                "session_id": session_id,
                "cleanup_needed": False,
                "message": "No staged files to clean up"
            }
        
        # Perform cleanup
        cleanup_success = cleanup_staging_files(
            session_id=session_id,
            keep_manifest=keep_logs
        )
        
        return {
            "session_id": session_id,
            "cleanup_needed": True,
            "cleanup_success": cleanup_success,
            "files_before_cleanup": staging_stats["total_files"],
            "size_before_cleanup_mb": staging_stats["total_size_mb"],
            "manifest_preserved": keep_logs
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to clean up session {session_id}: {str(e)}")
        
        return {
            "session_id": session_id,
            "cleanup_success": False,
            "error": str(e)
        }


# Example usage functions
def example_simple_session():
    """Example: Simple session with 3 batches"""
    
    session_id = f"example_session_{int(datetime.now().timestamp())}"
    
    # Sample batch data
    batches_data = [
        {"queries_list": ["SELECT 1", "SELECT 2"], "testing": False},
        {"queries_list": ["SELECT 3", "SELECT 4"], "testing": False},
        {"queries_list": ["SELECT 5"], "testing": False}
    ]
    
    session_metadata = {
        "company_name": "example_company",
        "from_dialect": "snowflake",
        "to_dialect": "e6",
        "created_at": datetime.now().isoformat()
    }
    
    # Start orchestration
    result = orchestrate_staging_based_processing(
        session_id=session_id,
        batches_data=batches_data,
        session_metadata=session_metadata,
        use_chord=True,
        cleanup_staging=True
    )
    
    print(f"Orchestration started for session {session_id}")
    print(f"Result: {result}")
    
    return result


def example_monitor_session(session_id: str, workers_group_id: str = None, committer_task_id: str = None):
    """Example: Monitor session progress"""
    
    status = monitor_session_progress(
        session_id=session_id,
        workers_group_id=workers_group_id,
        committer_task_id=committer_task_id
    )
    
    print(f"Session {session_id} status:")
    print(f"Overall: {status['overall_status']}")
    print(f"Staged files: {status['staging_stats']['total_files']}")
    print(f"Workers: {status['workers_status']['status']}")
    print(f"Committer: {status['committer_status']['status']}")
    
    return status


if __name__ == "__main__":
    # Run example
    print("Starting staging-based pipeline example...")
    result = example_simple_session()
    
    if result.get("committer_task_id"):
        print(f"\nMonitoring session {result['session_id']}...")
        status = example_monitor_session(
            session_id=result["session_id"],
            workers_group_id=result.get("workers_group_id"),
            committer_task_id=result.get("committer_task_id")
        )