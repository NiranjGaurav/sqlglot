"""
Simplified Orchestrator using Celery's built-in features
No explicit Redis management needed - Celery handles it all
"""
import logging
import os
import re
from typing import Dict, Any, List, Optional
import uuid
from datetime import datetime
import dateutil.parser
from celery import group
from .worker import celery as celery_app
from .tasks import discover_parquet_files, extract_unique_queries_from_file, create_query_batch_configs

logger = logging.getLogger(__name__)


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable format
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        remaining_seconds = int(seconds % 60)
        return f"{minutes}m {remaining_seconds}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        remaining_seconds = int(seconds % 60)
        return f"{hours}h {minutes}m {remaining_seconds}s"


def orchestrate_processing(
    directory_path: str,
    company_name: str,
    from_dialect: str,
    to_dialect: str,
    query_column: str,
    batch_size: int = 10000,
    filters: Dict[str, Any] = None,
    name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Orchestrate the entire processing pipeline using Celery with optimized file reading
    NEW APPROACH: Read each file once in orchestrator, distribute queries to workers
    
    Returns immediately with task group ID that can be monitored
    Following TestDriven.io pattern - let Celery handle all the complexity
    """
    if name and name.strip():
        # Use custom name with fallback to short UUID
        clean_name = re.sub(r'[^a-zA-Z0-9_-]', '_', name.strip())  # Sanitize and limit length
        clean_name = clean_name.strip('_')  # Remove leading/trailing underscores
        session_id = f"session_{clean_name}_{uuid.uuid4().hex[:8]}"
    else:
        # Default behavior
        session_id = f"session_{uuid.uuid4().hex[:8]}"
    
    logger.info(f"🚀 Starting orchestration for session {session_id}")
    logger.info(f"📂 Processing path: {directory_path}")
    
    # Check if it's an S3 path
    is_s3_path = directory_path.startswith('s3://')
    if is_s3_path:
        logger.info("📦 Using S3 filesystem with temporary credentials")
    
    try:
        # Discover parquet files
        file_paths = discover_parquet_files(
            directory_path,
            query_column
        )
        
        if not file_paths:
            return {
                'error': f'No valid parquet files found in {directory_path}',
                'session_id': session_id
            }
        
        # Process all files and collect batch configs
        all_batch_configs = []
        
        for file_path in file_paths:
            file_name = os.path.basename(file_path)
            logger.info(f"📖 Reading and processing file: {file_name}")
            
            # Extract unique queries from this file as PyArrow table
            unique_table = extract_unique_queries_from_file(
                file_path,
                query_column,
                filters or {}
            )
            
            if len(unique_table) == 0:
                logger.warning(f"No queries found in {file_name}")
                continue
            
            logger.info(f"✅ Extracted {len(unique_table):,} unique queries from {file_name}")
            
            # Create batch configurations - get PyArrow table + metadata
            batch_table, metadata = create_query_batch_configs(
                unique_table,
                session_id,
                company_name,
                from_dialect,
                to_dialect,
                query_column,
                batch_size,
                {'file_path': file_path, 'file_name': file_name}
            )
            
            # Each row is a job - extract queries as Python list for JSON serialization
            for i in range(len(batch_table)):
                batch_id = batch_table['batch_id'][i].as_py()  # Extract batch ID
                queries_array = batch_table['queries_array'][i].as_py()  # Extract queries as Python list
                
                all_batch_configs.append({
                    'batch_id': batch_id,
                    'queries_list': queries_array,  # Python list (JSON serializable)
                    'metadata': metadata
                })
        
        if not all_batch_configs:
            return {
                'error': 'No valid queries found in any files',
                'session_id': session_id
            }
        
        logger.info(f"📦 Created {len(all_batch_configs)} PyArrow jobs using table slicing")
        
        # Create a Celery group - each job gets one sliced PyArrow row
        job = group(
            celery_app.signature(
                'process_query_batch',
                args=[config],
                task_id=f"{session_id}_batch_{config['batch_id']}"  # Fixed task ID generation
            )
            for config in all_batch_configs
        )
        
        # Apply async and get the group result
        group_result = job.apply_async()
        
        # Store session metadata in Redis
        start_time = datetime.now().isoformat()
        try:
            import redis
            r = redis.Redis(host='localhost', port=6379, db=0)
            session_meta_key = f"session_meta_{session_id}"
            r.hset(session_meta_key, 'start_time', start_time)
            r.hset(session_meta_key, 'total_batches', len(all_batch_configs))
            r.expire(session_meta_key, 86400)  # Expire after 24 hours
        except Exception as e:
            logger.warning(f"Failed to store session metadata: {e}")
        
        logger.info(f"✅ Launched {len(all_batch_configs)} tasks for processing")
        
        # Return immediately with tracking information
        return {
            'session_id': session_id,
            'group_id': group_result.id if hasattr(group_result, 'id') else session_id,
            'status': 'processing',
            'total_files': len(file_paths),
            'total_batches': len(all_batch_configs),
            'task_ids': [f"{session_id}_batch_{config['batch_id']}" for config in all_batch_configs],
            'created_at': start_time,
            'start_time': start_time,  # Include start time immediately
            'configuration': {
                'directory_path': directory_path,
                'company_name': company_name,
                'from_dialect': from_dialect,
                'to_dialect': to_dialect,
                'batch_size': batch_size
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Orchestration failed: {str(e)}")
        return {
            'error': str(e),
            'session_id': session_id,
            'status': 'failed'
        }



def get_processing_status(session_id: str, task_ids: List[str] = None) -> Dict[str, Any]:
    """
    Get status of processing session using Celery's AsyncResult
    If no task_ids provided, search Redis for tasks matching session pattern
    Special case: if session_id is 'discover_all', return all unique session IDs
    """
    from celery.result import AsyncResult
    import redis
    
    # Special case: discover all active sessions
    if session_id == 'discover_all':
        try:
            r = redis.Redis(host='localhost', port=6379, db=0)
            keys = r.keys('celery-task-meta-*')
            discovered_sessions = set()
            
            for key in keys:
                task_id = key.decode().replace('celery-task-meta-', '')
                # Extract session ID from task ID (format: session_xxx_batch_xxx)
                if task_id.startswith('session_'):
                    # Split by '_batch_' to get session part
                    parts = task_id.split('_batch_')
                    if len(parts) >= 2:
                        session_part = parts[0]  # Everything before '_batch_'
                        discovered_sessions.add(session_part)
            
            # Also check for session metadata keys
            session_meta_keys = r.keys('session_meta_*')
            for key in session_meta_keys:
                session_meta_id = key.decode().replace('session_meta_', '')
                discovered_sessions.add(session_meta_id)
            
            logger.info(f"Discovered {len(discovered_sessions)} unique sessions in Redis")
            return {
                'session_id': 'discover_all',
                'discovered_sessions': list(discovered_sessions),
                'total_discovered': len(discovered_sessions)
            }
        except Exception as e:
            logger.error(f"Failed to discover sessions from Redis: {e}")
            return {
                'session_id': 'discover_all',
                'discovered_sessions': [],
                'total_discovered': 0,
                'error': str(e)
            }
    
    if not task_ids:
        # Search Redis for completed tasks matching session pattern
        try:
            r = redis.Redis(host='localhost', port=6379, db=0)
            keys = r.keys('celery-task-meta-*')
            task_ids = []
            
            for key in keys:
                task_id = key.decode().replace('celery-task-meta-', '')
                if task_id.startswith(session_id):
                    task_ids.append(task_id)
                    
            logger.info(f"Found {len(task_ids)} tasks in Redis for session {session_id}")
        except Exception as e:
            logger.error(f"Failed to search Redis for tasks: {e}")
            task_ids = []
    
    results = []
    completed = 0
    failed = 0
    pending = 0
    processing = 0
    
    # Track timing information
    start_times = []
    end_times = []
    
    for task_id in task_ids:
        result = AsyncResult(task_id)
        
        task_status = {
            'task_id': task_id,
            'state': result.state
        }
        
        if result.state == 'PENDING':
            pending += 1
            task_status['status'] = 'Waiting to be processed'
        elif result.state == 'STARTED':
            processing += 1
            task_status['status'] = 'Processing'
        elif result.state == 'PROGRESS':
            processing += 1
            if result.info:
                task_status.update(result.info)
        elif result.state == 'SUCCESS':
            completed += 1
            task_status['status'] = 'Completed'
            if result.result:
                # Only include essential result info to avoid huge responses
                if isinstance(result.result, dict):
                    task_status['processed_count'] = result.result.get('processed_count', 0)
                    task_status['successful_count'] = result.result.get('successful_count', 0)
                    # Track completion time
                    if 'completion_time' in result.result:
                        end_times.append(result.result['completion_time'])
        elif result.state == 'FAILURE':
            failed += 1
            task_status['status'] = 'Failed'
            task_status['error'] = str(result.info)
        
        results.append(task_status)
    
    total_tasks = len(task_ids)
    total_batches = total_tasks  # Each task represents a batch
    successful_batches = completed  # Only count successfully completed batches
    
    # Calculate percentage based on successful batches vs total batches
    progress_percentage = (successful_batches / total_batches * 100) if total_batches > 0 else 0
    
    # Calculate timing information and get actual total batches
    session_start_time = None
    session_end_time = None
    total_duration = None
    actual_total_batches = total_batches  # Default to calculated value
    
    # Try to get timing info and total batches from Redis metadata
    try:
        r = redis.Redis(host='localhost', port=6379, db=0)
        session_meta_key = f"session_meta_{session_id}"
        session_meta = r.hgetall(session_meta_key)
        
        if session_meta:
            if b'start_time' in session_meta:
                session_start_time = session_meta[b'start_time'].decode()
            if b'end_time' in session_meta:
                session_end_time = session_meta[b'end_time'].decode()
            if b'total_batches' in session_meta:
                actual_total_batches = int(session_meta[b'total_batches'].decode())
        
        # If session is completed and no end time recorded, set it now
        if (completed + failed >= actual_total_batches) and session_start_time and not session_end_time:
            session_end_time = datetime.now().isoformat()
            r.hset(session_meta_key, 'end_time', session_end_time)
            r.expire(session_meta_key, 86400)  # Expire after 24 hours
        
        # Calculate duration if we have both times
        if session_start_time and session_end_time:
            try:
                start_dt = dateutil.parser.parse(session_start_time)
                end_dt = dateutil.parser.parse(session_end_time)
                duration_seconds = (end_dt - start_dt).total_seconds()
                total_duration = format_duration(duration_seconds)
            except Exception as e:
                logger.warning(f"Failed to calculate duration: {e}")
                
    except Exception as e:
        logger.warning(f"Failed to get session timing info: {e}")
    
    # Recalculate progress percentage with actual total batches
    progress_percentage = (successful_batches / actual_total_batches * 100) if actual_total_batches > 0 else 0
    
    return {
        'session_id': session_id,
        'total_tasks': total_tasks,
        'total_batches': actual_total_batches,  # Use actual total batches from Redis
        'completed': completed,
        'failed': failed,
        'pending': pending,
        'processing': processing,
        'successful_batches': successful_batches,  # Add this field
        'progress_percentage': round(progress_percentage, 1),
        'task_details': results,  # Return all task details
        'overall_status': 'completed' if completed + failed >= actual_total_batches else 'processing',
        'start_time': session_start_time,
        'end_time': session_end_time,
        'duration': total_duration
    }


def get_task_result(task_id: str) -> Dict[str, Any]:
    """
    Get result of a specific task using Celery's AsyncResult
    """
    from celery.result import AsyncResult
    
    result = AsyncResult(task_id)
    
    response = {
        'task_id': task_id,
        'state': result.state,
        'ready': result.ready()
    }
    
    if result.state == 'PENDING':
        response['status'] = 'Task is waiting to be processed'
    elif result.state == 'STARTED':
        response['status'] = 'Task has started processing'
    elif result.state == 'PROGRESS':
        response['status'] = 'Task is in progress'
        if result.info:
            response['progress'] = result.info
    elif result.state == 'SUCCESS':
        response['status'] = 'Task completed successfully'
        response['result'] = result.result
    elif result.state == 'FAILURE':
        response['status'] = 'Task failed'
        response['error'] = str(result.info)
        response['traceback'] = result.traceback
    elif result.state == 'RETRY':
        response['status'] = 'Task is being retried'
    else:
        response['status'] = f'Unknown state: {result.state}'
    
    return response