# Staging-Based Celery + PyIceberg Pipeline

Production-ready Python implementation of a scalable, fault-tolerant pipeline that stages Parquet files in S3 before committing them atomically to an Iceberg table with Glue catalog.

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   FastAPI       │    │   Celery        │    │   S3 Staging    │
│   Orchestrator  │───▶│   Workers       │───▶│   Area          │
│                 │    │   (Parallel)    │    │   /staging/     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Iceberg       │◀───│   Single-Thread │◀───│   Manifest      │
│   Table         │    │   Committer     │    │   Creation      │
│   (Glue+S3)     │    │   (Queue=1)     │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Key Features

✅ **Scalable Workers**: Process batches in parallel, write Parquet to S3 staging  
✅ **Single-Threaded Commits**: Serialize Iceberg metadata updates for safety  
✅ **Fault Tolerance**: Staged files survive worker failures, retryable commits  
✅ **Memory Efficient**: Stream processing with PyArrow, no in-memory accumulation  
✅ **AWS Integration**: S3 + Glue catalog with proper credential handling  
✅ **Monitoring**: Comprehensive progress tracking and validation  

## File Structure

```
automated_processing/
├── worker.py                    # Main Celery app with task routing
├── staging.py                   # S3 staging helper functions
├── iceberg_io.py               # Safe Iceberg commit operations  
├── tasks_workers.py            # Worker batch processing tasks
├── tasks_committer.py          # Single-threaded committer tasks
├── orchestration_example.py    # Group/chord orchestration examples
└── STAGING_ARCHITECTURE_README.md
```

## Quick Start

### 1. Start Celery Workers

```bash
# Terminal 1: Standard workers (parallel processing)
celery -A worker.celery worker -Q processing_queue --concurrency=4 --loglevel=info

# Terminal 2: Single-threaded committer (serialized Iceberg commits)  
celery -A worker.celery worker -Q iceberg_commit --concurrency=1 --pool=solo --loglevel=info
```

### 2. Basic Usage Example

```python
from orchestration_example import orchestrate_staging_based_processing

# Define session data
session_id = "test_session_001"
batches_data = [
    {"queries_list": ["SELECT * FROM table1", "SELECT COUNT(*) FROM table2"]},
    {"queries_list": ["SELECT AVG(col) FROM table3"]},
    # ... more batches
]

session_metadata = {
    "company_name": "acme_corp", 
    "from_dialect": "snowflake",
    "to_dialect": "e6",
    "created_at": "2025-01-15T10:00:00Z"
}

# Start processing (non-blocking)
result = orchestrate_staging_based_processing(
    session_id=session_id,
    batches_data=batches_data, 
    session_metadata=session_metadata,
    use_chord=True,  # Recommended: automatic flow control
    cleanup_staging=True
)

print(f"Processing started: {result['status']}")
print(f"Committer task: {result['committer_task_id']}")
```

### 3. Monitor Progress

```python
from orchestration_example import monitor_session_progress

status = monitor_session_progress(
    session_id="test_session_001",
    workers_group_id=result['workers_group_id'],
    committer_task_id=result['committer_task_id']
)

print(f"Overall status: {status['overall_status']}")
print(f"Staged files: {status['staging_stats']['total_files']}")
print(f"Total size: {status['staging_stats']['total_size_mb']} MB")
```

## Pipeline Flow

### Step 1: Worker Processing (Parallel)
```python
# Each worker processes a batch of queries
process_batch_to_staging_task.s(
    session_id="session_001",
    batch_id=0,
    queries_list=["SELECT 1", "SELECT 2", ...],
    metadata={...}
)
```

**Worker Output**: Parquet file staged at `s3://bucket/staging/session_001/batch_0000.parquet`

### Step 2: Manifest Creation  
```python
# After all workers complete, create staging manifest
create_staging_manifest_task.s(
    session_id="session_001",
    worker_results=[...],  # Results from all workers
    session_metadata={...}
)
```

**Manifest Output**: `s3://bucket/staging/session_001/manifest.json` with file inventory

### Step 3: Atomic Commit (Single-Threaded)
```python  
# Committer runs on dedicated single-threaded queue
commit_session_to_iceberg_task.s(
    session_id="session_001",
    session_metadata={...},
    cleanup_staging=True
)
```

**Commit Process**:
1. Validate all staged files are readable
2. Start Iceberg transaction  
3. Add all files atomically
4. Commit transaction
5. Clean up staging area

## Configuration

### Environment Variables

```bash
# Celery Configuration
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0

# S3 Configuration  
S3_BUCKET=batch-transpiler
S3_STAGING_PREFIX=staging

# AWS Credentials (handled in code for now)
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...
AWS_REGION=us-east-1

# Iceberg Configuration
S3_WAREHOUSE_PATH=s3://batch-transpiler/testing-batch-processing/
ICEBERG_CATALOG_NAME=glue_catalog
```

### Celery Queue Configuration

```python
# In worker.py - task routing
task_routes = {
    # Worker tasks (parallel processing)
    'process_batch_to_staging': {'queue': 'processing_queue'},
    'create_staging_manifest': {'queue': 'processing_queue'},
    
    # Committer tasks (single-threaded)  
    'commit_session_to_iceberg': {'queue': 'iceberg_commit'},
    'retry_failed_commit': {'queue': 'iceberg_commit'},
    'commit_multiple_sessions': {'queue': 'iceberg_commit'}
}
```

## Advanced Usage

### Large Sessions (Chunked Commits)

For sessions with 500+ staged files, use chunked commits:

```python
commit_result = commit_session_to_iceberg_task.apply_async(
    args=[session_id, session_metadata],
    kwargs={
        "use_chunked_commit": True,
        "chunk_size": 50,  # Files per chunk
        "cleanup_staging": True
    }
)
```

### Manual Orchestration

For custom workflows, skip the chord pattern:

```python
# Start workers manually
from celery import group
from worker import process_batch_to_staging_task

worker_tasks = [
    process_batch_to_staging_task.s(session_id, i, batch_data['queries_list'], metadata)
    for i, batch_data in enumerate(batches_data)
]

workers_group = group(worker_tasks)
result = workers_group.apply_async()

# Later, when workers complete:
from orchestration_example import create_manifest_and_commit

final_result = create_manifest_and_commit(
    session_id=session_id,
    session_metadata=session_metadata,
    cleanup_staging=True
)
```

### Error Handling & Retry

```python
# Retry failed commits
from worker import retry_failed_commit_task

retry_result = retry_failed_commit_task.apply_async(
    args=[session_id, "original_error_message"],
    kwargs={
        "retry_attempt": 1,
        "max_retries": 3
    }
)
```

### Batch Cleanup Operations

```python
# Commit multiple sessions at once (for batch maintenance)
from worker import commit_multiple_sessions_task

cleanup_result = commit_multiple_sessions_task.apply_async(
    args=[["session_001", "session_002", "session_003"]],
    kwargs={"cleanup_staging": True}
)
```

## Monitoring & Debugging

### Staging Statistics

```python
from staging import get_staging_statistics

stats = get_staging_statistics("session_001")
print(f"Files: {stats['total_files']}")
print(f"Size: {stats['total_size_mb']} MB") 
print(f"Files: {stats['staged_files']}")
```

### File Validation

```python  
from staging import validate_staged_files

validation = validate_staged_files("session_001")
print(f"Valid: {validation['valid_files']}")
print(f"Invalid: {validation['invalid_files']}")
print(f"Total rows: {validation['total_rows']:,}")
```

### Iceberg Table Health

```python
from iceberg_io import validate_table_schema, get_table_statistics

# Check table schema
schema_check = validate_table_schema("default.batch_statistics")
print(f"Schema valid: {schema_check['schema_valid']}")

# Get table stats  
stats = get_table_statistics("default.batch_statistics")
print(f"Snapshots: {stats['total_snapshots']}")
print(f"Format version: {stats['format_version']}")
```

## Performance Tuning

### Worker Scaling
- **CPU-bound**: Scale workers to `2 × CPU_cores + 1`
- **Memory**: Limit to 4-8GB per worker process  
- **Restart**: Use `worker_max_tasks_per_child=5` to prevent memory leaks

### S3 Optimization
- **File Size**: Target 100-500MB per staged Parquet file
- **Compression**: Use Snappy compression (balance speed/size)
- **Batch Size**: 1,000-10,000 queries per batch optimal

### Iceberg Commits  
- **Atomic vs Chunked**: Use atomic for <100 files, chunked for larger
- **Chunk Size**: 10-50 files per chunk for large sessions
- **Cleanup**: Always clean staging after successful commit

## Troubleshooting

### Common Issues

**"No staged files found"**
- Check S3 credentials and permissions
- Verify staging prefix configuration  
- Check if workers completed successfully

**"Iceberg commit failed"**
- Validate table schema matches expected format
- Check Glue catalog permissions
- Verify warehouse S3 path is accessible

**"Workers stuck in processing"**
- Check Redis broker connectivity
- Verify worker processes are not OOM killed
- Look for SQL parsing errors in worker logs

**"Committer timeout"**  
- Large sessions may need longer commit timeout
- Use chunked commits for 200+ files
- Check S3 bandwidth and Glue API limits

### Debug Commands

```bash
# Check Celery queues
celery -A worker.celery inspect active_queues

# Monitor task progress  
celery -A worker.celery events

# Check Redis broker
redis-cli monitor

# Validate S3 staging area
aws s3 ls s3://bucket/staging/session_id/ --recursive
```

## Production Deployment

### Container Setup

```dockerfile
# Dockerfile.worker
FROM python:3.9-slim

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY automated_processing/ /app/
WORKDIR /app

# Start workers
CMD ["celery", "-A", "worker.celery", "worker", "-Q", "processing_queue", "--concurrency=4"]
```

```dockerfile  
# Dockerfile.committer  
FROM python:3.9-slim

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY automated_processing/ /app/
WORKDIR /app

# Start single-threaded committer
CMD ["celery", "-A", "worker.celery", "worker", "-Q", "iceberg_commit", "--concurrency=1", "--pool=solo"]
```

### Kubernetes Deployment

```yaml
# k8s-workers.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sql-workers
spec:
  replicas: 4
  selector:
    matchLabels:
      app: sql-workers
  template:
    metadata:
      labels:
        app: sql-workers  
    spec:
      containers:
      - name: worker
        image: sql-transpiler-worker:latest
        env:
        - name: CELERY_BROKER_URL
          value: "redis://redis-service:6379/0"
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi" 
            cpu: "2000m"

---
# k8s-committer.yaml
apiVersion: apps/v1  
kind: Deployment
metadata:
  name: sql-committer
spec:
  replicas: 1  # MUST be 1 for single-threaded commits
  selector:
    matchLabels:
      app: sql-committer
  template:
    metadata:
      labels:
        app: sql-committer
    spec:
      containers:
      - name: committer
        image: sql-transpiler-committer:latest
        env:
        - name: CELERY_BROKER_URL
          value: "redis://redis-service:6379/0"
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
```

## Security Considerations

- **AWS Credentials**: Use IAM roles instead of hardcoded keys in production
- **S3 Permissions**: Restrict to specific bucket and prefixes  
- **Glue Access**: Use least-privilege IAM policies
- **Redis**: Enable AUTH and use TLS for broker connections
- **Network**: Use VPC endpoints for S3 and Glue API calls

This architecture provides a robust, scalable solution for processing large volumes of SQL queries while maintaining data consistency and fault tolerance through S3 staging and single-threaded Iceberg commits.