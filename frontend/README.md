# SQLGlot Batch Processor Frontend

A Next.js frontend for the SQLGlot batch processing system with distributed SQL transpilation capabilities.

## Features

- **🚀 Start Processing**: Submit batch processing jobs for Parquet files containing SQL queries
- **📊 Query Conversion**: Monitor real-time processing status with detailed metrics
- **📋 Select Session**: Browse and manage active/completed processing sessions

## Tech Stack

- **Frontend**: Next.js 14, React 18, TypeScript
- **Styling**: Tailwind CSS
- **Backend Integration**: FastAPI (converter_api.py) proxy
- **State Management**: React hooks with local state

## Getting Started

### Prerequisites

- Node.js 18+ installed
- Backend API running on `http://localhost:8100` (converter_api.py)
- Redis and Celery workers running for session management

### Installation

```bash
# Install dependencies
npm install

# Run development server
npm run dev
```

The application will be available at `http://localhost:3000`.

### Backend API Proxy

The frontend proxies API requests to the FastAPI backend:
- Frontend: `http://localhost:3000`
- Backend API: `http://localhost:8100`

API endpoints are automatically proxied via Next.js rewrites in `next.config.js`.

## Application Structure

### Main Components

1. **BatchProcessor** - Form for starting new processing sessions
   - S3 path validation
   - Dialect selection (Snowflake, BigQuery, etc. → E6)
   - Batch configuration (size, filters, query column)

2. **ProcessingResults** - Real-time status monitoring
   - Overall progress tracking
   - Staging statistics (files, sizes)
   - Worker status (completed/failed tasks)
   - Committer status (rows committed, duration)
   - Auto-refresh for active sessions

3. **ActiveSessions** - Session management
   - Redis-based session discovery
   - Search and filter capabilities
   - Session status overview

### API Integration

The frontend integrates with the following backend endpoints:

- `POST /api/process-parquet-directory-automated` - Start batch processing
- `GET /api/processing-status/{session_id}` - Get session status
- `GET /api/processing-status/discover_all` - Discover all sessions
- `POST /api/validate-s3-bucket` - Validate S3 paths and get columns

### Data Flow

1. **Session Creation**: User submits processing job → Backend creates session → Frontend shows progress
2. **Status Monitoring**: Frontend polls session status → Backend queries Redis/Celery → Real-time updates
3. **Session Discovery**: Frontend discovers sessions from Redis metadata → Shows in session list

## Environment Configuration

### Development

```bash
npm run dev    # Start development server
npm run lint   # Run ESLint
npm run build  # Build for production
```

### Production

```bash
npm run build  # Build optimized bundle
npm start      # Start production server
```

## Architecture Details

### Processing Pipeline

The frontend interfaces with a staging-based processing pipeline:

```
Parquet Files → Workers (Celery) → S3 Staging → Manifest → Iceberg Commit
```

### Status Flow

```
processing → staged_ready_for_commit → committing → completed
           ↘                                      ↗
            failed ←---------------------------- failed
```

### Session Storage

- **Redis**: Session metadata storage for discovery
- **S3**: Staged Parquet files during processing
- **Iceberg**: Final committed data with partitioning

## UI Features

### Tab Interface

- **Start Processing**: Configure and launch batch jobs
- **Query Conversion**: Monitor progress with detailed metrics
- **Select Session**: Browse and select sessions for monitoring

### Real-time Updates

- Auto-refresh for active sessions (10-second intervals)
- Progress bars with status-based coloring
- Polling stops automatically when sessions complete

### Session Management

- Persistent session discovery via Redis
- Search and filter capabilities
- Session status indicators with icons

## Error Handling

- Form validation with user feedback
- API error display with retry options
- Graceful handling of network failures
- Auto-cleanup of completed sessions

## Responsive Design

- Mobile-friendly interface
- Responsive grid layouts
- Optimized for desktop and tablet usage
- Consistent spacing and typography

---

For backend setup and API documentation, see the main project README.