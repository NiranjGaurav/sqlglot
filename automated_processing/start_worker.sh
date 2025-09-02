#!/bin/bash

# Start Celery Worker for Automated Processing
# Following TestDriven.io FastAPI+Celery patterns

echo "🚀 Starting Celery worker for automated SQL processing..."
echo "📁 Working directory: $(pwd)"
echo ""

# Change to the automated_processing directory
cd "$(dirname "$0")"

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

# Start the Celery worker with prefork pool for maximum CPU performance
echo "🔧 Starting Celery worker with prefork pool for maximum CPU performance..."
echo "   PyIceberg storage handled by separate service (no SIGSEGV)"
echo ""

# Start the Celery worker with prefork pool and autoscaling
echo "🚀 Starting Celery worker with prefork pool and autoscaling..."
echo "   Command: celery -A worker.celery worker --loglevel=info --pool=prefork --autoscale=8,2"
echo "   This will scale from 2 to 8 processes based on workload"
echo "   Iceberg writes queued to Redis for separate processing"
echo ""

# Get CPU cores for optimal scaling
CPU_CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
MAX_WORKERS=$((CPU_CORES))  # 2x CPU cores for I/O bound tasks
MIN_WORKERS=1

echo "🔥 Detected $CPU_CORES CPU cores"
echo "📈 Scaling: $MIN_WORKERS-$MAX_WORKERS workers, prefetch=4 (faster task pickup)"

celery -A worker.celery worker \
    --loglevel=info \
    --pool=prefork \
    --autoscale=$MAX_WORKERS,$MIN_WORKERS \
    --prefetch-multiplier=4 \
    --max-tasks-per-child=50 \
    -Q processing_queue

echo ""
echo "👋 Worker stopped"