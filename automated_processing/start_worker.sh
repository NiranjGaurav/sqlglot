#!/bin/bash

# Start Celery Workers for Staging-Based Architecture
# Starts both processing queue and iceberg commit queue workers

echo "🚀 Starting Celery workers for staging-based SQL processing..."
echo "📁 Working directory: $(pwd)"
echo ""

# Change to the parent directory (sqlglot project root)
cd "$(dirname "$0")/.."

# Check if Redis is running and start if needed
if ! redis-cli ping > /dev/null 2>&1; then
    echo "⚠️  Redis is not running. Starting Redis..."
    
    # Try to start Redis with brew services first
    if command -v brew > /dev/null 2>&1; then
        echo "🔧 Starting Redis with brew services..."
        brew services start redis
        sleep 2  # Give Redis time to start
        
        # Check if Redis is now running
        if redis-cli ping > /dev/null 2>&1; then
            echo "✅ Redis started successfully with brew"
        else
            echo "❌ Failed to start Redis with brew services"
            echo "   Try manually: brew services start redis"
            exit 1
        fi
    else
        # Try to start Redis directly
        echo "🔧 Starting Redis server directly..."
        redis-server --daemonize yes
        sleep 2  # Give Redis time to start
        
        # Check if Redis is now running
        if redis-cli ping > /dev/null 2>&1; then
            echo "✅ Redis started successfully"
        else
            echo "❌ Failed to start Redis"
            echo "   Please install Redis or start it manually"
            exit 1
        fi
    fi
else
    echo "✅ Redis is already running"
fi

# Function to cleanup background processes
cleanup() {
    echo ""
    echo "🛑 Stopping workers..."
    if [[ -n $PROCESSING_WORKER_PID ]]; then
        kill $PROCESSING_WORKER_PID 2>/dev/null
        echo "   Stopped processing queue worker (PID: $PROCESSING_WORKER_PID)"
    fi
    if [[ -n $COMMIT_WORKER_PID ]]; then
        kill $COMMIT_WORKER_PID 2>/dev/null
        echo "   Stopped iceberg commit worker (PID: $COMMIT_WORKER_PID)"
    fi
    echo "👋 All workers stopped"
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

echo "🔧 Starting dual-queue Celery workers for staging architecture..."
echo ""
echo "📝 Queue Architecture:"
echo "   • processing_queue: Parallel workers for SQL analysis and S3 staging"
echo "   • iceberg_commit: Single-threaded worker for atomic Iceberg commits"
echo "   • Using s3fs library instead of PyArrow S3FileSystem to prevent segfaults"
echo ""

# Start processing queue workers (parallel)
echo "🚀 Starting processing queue worker..."
echo "   Command: celery -A automated_processing.worker.celery worker --queues=processing_queue --concurrency=4"
echo "   Using prefork pool with PyArrow segfault fixes"
# Set environment variables to prevent PyArrow segfaults
export PYTHONFAULTHANDLER=1
export ARROW_ENABLE_NULL_CHECK_FOR_GET=0
export ARROW_ENABLE_TIMING_TESTS=0  
export OMP_NUM_THREADS=1
celery -A automated_processing.worker.celery worker \
    --loglevel=info \
    --queues=processing_queue \
    --concurrency=4 \
    --pool=prefork \
    --autoscale=4,1 \
    --hostname="processing_worker@%h" &

PROCESSING_WORKER_PID=$!
echo "   Processing worker started (PID: $PROCESSING_WORKER_PID)"

# Give the first worker time to start
sleep 3

# Start iceberg commit queue worker (single-threaded)
echo "🚀 Starting iceberg commit queue worker..."
echo "   Command: celery -A automated_processing.worker.celery worker --queues=iceberg_commit --concurrency=1"
celery -A automated_processing.worker.celery worker \
    --loglevel=info \
    --queues=iceberg_commit \
    --concurrency=1 \
    --pool=prefork \
    --hostname="iceberg_committer@%h" &

COMMIT_WORKER_PID=$!
echo "   Iceberg commit worker started (PID: $COMMIT_WORKER_PID)"

echo ""
echo "✅ Both workers are running!"
echo ""
echo "📊 Worker Status:"
echo "   • Processing Queue: PID $PROCESSING_WORKER_PID (parallel, concurrency=4)"
echo "   • Iceberg Commit:   PID $COMMIT_WORKER_PID (single-threaded, concurrency=1)"
echo ""
echo "💡 Usage Tips:"
echo "   • Use orchestration_example.py for staging-based processing"
echo "   • Use orchestrator.py for legacy direct-commit processing"
echo "   • Monitor with: celery -A worker.celery inspect active"
echo "   • Press Ctrl+C to stop all workers"
echo ""

# Wait for both workers
wait