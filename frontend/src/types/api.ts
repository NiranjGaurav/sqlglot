// API types for the SQLGlot Batch Processor

export interface ProcessingSession {
  session_id: string
  total_files: number
  total_batches: number
  task_ids: string[]
  status: 'processing' | 'completed' | 'failed' | 'committing' | 'staged_ready_for_commit'
  created_at: string
  status_url: string
  configuration: {
    directory_path: string
    company_name: string
    query_column: string
    batch_size: number
    dialect_conversion: string
  }
  iceberg_storage: {
    table: string
    partition_structure: string
    storage_pattern: string
  }
  message: string
}

export interface ProcessingStatus {
  session_id: string
  timestamp: string
  staging_stats: StagingStats
  workers_status: WorkersStatus
  committer_status: CommitterStatus
  validation_result?: ValidationResult
  overall_status: string
}

export interface StagingStats {
  session_id: string
  total_files: number
  total_size_bytes: number
  total_size_mb: number
  staged_files: string[]
  manifest_exists: boolean
  staging_complete: boolean
  manifest_data?: {
    created_at?: string
    metadata?: {
      total_batches?: number
      successful_batches?: number
      failed_batches_count?: number
      metadata?: {
        company_name?: string
        from_dialect?: string
        to_dialect?: string
        created_at?: string
        staging_completed_at?: string
        total_queries_processed?: number
        total_queries_successful?: number
      }
      iceberg_commit?: {
        committed_at?: string
        total_rows?: number
        committed_files?: number
        commit_duration_seconds?: number
        cleanup_performed?: boolean
        cleanup_success?: boolean
      }
    }
  }
}

export interface WorkersStatus {
  status: 'unknown' | 'running' | 'completed' | 'error'
  completed_tasks?: number
  total_tasks?: number
  successful_tasks?: number
  failed_tasks?: number
  error?: string
}

export interface CommitterStatus {
  status: 'not_started' | 'PENDING' | 'STARTED' | 'SUCCESS' | 'FAILURE' | 'RETRY' | 'error'
  result?: CommitterResult
  error?: string
}

export interface CommitterResult {
  session_id: string
  success: boolean
  committed_files: number
  total_rows: number
  commit_duration_seconds: number
  total_commit_duration_seconds?: number
  commit_type?: 'atomic' | 'chunked' | 'streaming'
  total_chunks?: number
  successful_chunks?: number
  failed_chunks?: number
  chunk_size?: number
  failed_chunk_details?: Array<{
    chunk_index: number
    chunk_id: string
    files_count: number
    error: string
    duration: number
  }>
  cleanup_success?: boolean
  error?: string
}

export interface ValidationResult {
  error?: string
  files_found?: number
  total_size_mb?: number
  query_column?: string
}

export interface TaskResult {
  task_id: string
  state: 'PENDING' | 'PROGRESS' | 'SUCCESS' | 'FAILURE'
  ready: boolean
  result?: any
  status: string
  error?: string
}

export interface S3ValidationResult {
  authenticated: boolean
  error?: string
  columns?: string[]
}

export interface SessionDiscoveryResult {
  session_id: string
  discovered_sessions: string[]
  total_discovered: number
  discovery_method: string
}

// Form data interfaces
export interface BatchProcessingFormData {
  directory_path: string
  company_name: string
  from_dialect: string
  to_dialect: string
  query_column: string
  batch_size: number
  filters?: string
  session_name?: string
}

// Statistics for individual batches
export interface BatchStatistics {
  batch_id: number
  processed_queries: number
  successful_queries: number
  failed_queries: number
  processing_duration: number
  status: 'completed' | 'failed' | 'processing'
  error_details?: string[]
}