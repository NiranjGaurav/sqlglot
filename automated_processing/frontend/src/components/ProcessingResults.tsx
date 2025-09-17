'use client'

import { useState, useEffect } from 'react'
import { ProcessingStatus } from '@/types/api'

interface ProcessingResultsProps {
  sessionId: string | null
  refreshTrigger: number
  onRefresh: () => void
}

export default function ProcessingResults({ sessionId, refreshTrigger, onRefresh }: ProcessingResultsProps) {
  const [status, setStatus] = useState<ProcessingStatus | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [autoRefresh, setAutoRefresh] = useState(true)

  const fetchStatus = async () => {
    if (!sessionId) return

    setLoading(true)
    setError(null)

    try {
      // Add timeout to prevent hanging requests
      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), 30000) // 30 second timeout
      
      const response = await fetch(`/api/processing-status/${sessionId}?_=${Date.now()}`, {
        signal: controller.signal,
        cache: 'no-cache',
        headers: {
          'Cache-Control': 'no-cache, no-store, must-revalidate',
          'Pragma': 'no-cache'
        }
      })
      clearTimeout(timeoutId)
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`)
      }
      
      const data: ProcessingStatus = await response.json()
      setStatus(data)
    } catch (err) {
      if (err instanceof Error && err.name === 'AbortError') {
        setError('Request timed out - session data is taking too long to load')
      } else {
        setError(err instanceof Error ? err.message : 'Failed to fetch status')
      }
    } finally {
      setLoading(false)
    }
  }

  // Initial fetch and manual refresh
  useEffect(() => {
    if (sessionId) {
      fetchStatus()
    }
  }, [sessionId, refreshTrigger])

  // Auto-refresh for active sessions
  useEffect(() => {
    if (!autoRefresh || !sessionId || !status) return

    // Only auto-refresh if session is still active
    const isActive = status.overall_status === 'processing' || 
                    status.overall_status === 'committing' || 
                    status.overall_status === 'staged_ready_for_commit'

    if (!isActive) return

    const interval = setInterval(() => {
      fetchStatus()
    }, 30000) // Refresh every 30 seconds

    return () => clearInterval(interval)
  }, [autoRefresh, sessionId, status?.overall_status])

  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case 'completed':
      case 'success':
        return 'status-completed'
      case 'failed':
      case 'failure':
      case 'error':
        return 'status-failed'
      case 'processing':
      case 'running':
        return 'status-processing'
      case 'committing':
      case 'pending':
      case 'started':
        return 'status-committing'
      case 'staged_ready_for_commit':
        return 'status-staged-ready'
      default:
        return 'bg-gray-100 text-gray-800 border-gray-200'
    }
  }

  const formatBytes = (bytes: number) => {
    if (bytes === 0) return '0 Bytes'
    const k = 1024
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
  }

  const calculateProgress = () => {
    if (!status) return 0
    
    const { workers_status, committer_status, overall_status } = status
    
    if (overall_status === 'completed') return 100
    if (overall_status === 'failed') return 0
    
    // Committing phase (80-100%)
    if (committer_status.status === 'SUCCESS') return 100
    if (committer_status.status === 'STARTED' || committer_status.status === 'PENDING') {
      return 80
    }
    
    // Worker phase (0-80%)
    if (workers_status.completed_tasks && workers_status.total_tasks) {
      const workerProgress = (workers_status.completed_tasks / workers_status.total_tasks) * 80
      return Math.min(workerProgress, 80)
    }
    
    return 10 // Some progress if we have status
  }

  if (!sessionId) {
    return (
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-8 text-center">
        <div className="text-4xl mb-4">🎯</div>
        <h3 className="text-lg font-medium text-gray-900 mb-2">No Session Selected</h3>
        <p className="text-gray-500">
          Please start a new processing session or select an existing one from the sessions tab.
        </p>
      </div>
    )
  }

  if (loading && !status) {
    return (
      <div className="space-y-6">
        {/* Loading Header */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h2 className="text-xl font-semibold text-gray-800">
                <span className="mr-2">📊</span>
                Processing Results
              </h2>
              <p className="text-sm text-gray-500 mt-1">Session: {sessionId}</p>
            </div>
            <div className="animate-spin text-primary-600">⏳</div>
          </div>
          
          {/* Loading Progress Bar */}
          <div className="mb-6">
            <div className="flex justify-between text-sm text-gray-600 mb-2">
              <span>Loading session data...</span>
              <span className="animate-pulse">⏳</span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-3">
              <div className="h-3 rounded-full bg-primary-500 animate-pulse" style={{ width: '45%' }}></div>
            </div>
          </div>

          {/* Loading Status Cards */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            {[1,2,3,4].map((i) => (
              <div key={i} className="text-center">
                <div className="animate-pulse bg-gray-200 h-8 w-16 mx-auto rounded mb-2"></div>
                <div className="animate-pulse bg-gray-200 h-4 w-20 mx-auto rounded"></div>
              </div>
            ))}
          </div>
        </div>
        
        <div className="text-center text-gray-500 py-4">
          <p>⏳ Loading detailed session information...</p>
          <p className="text-sm mt-1">This may take up to 30 seconds for large sessions</p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-8 text-center">
        <div className="text-4xl mb-4">❌</div>
        <h3 className="text-lg font-medium text-gray-900 mb-2">Error Loading Status</h3>
        <p className="text-gray-500 mb-4">{error}</p>
        <button
          onClick={fetchStatus}
          className="px-4 py-2 bg-primary-600 text-white rounded-md hover:bg-primary-700 transition-colors duration-200"
        >
          🔄 Retry
        </button>
      </div>
    )
  }

  if (!status) {
    return (
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-8 text-center">
        <div className="text-4xl mb-4">🔍</div>
        <p className="text-gray-500">No status data available</p>
      </div>
    )
  }

  const progress = calculateProgress()

  return (
    <div className="space-y-6">
      {/* Header with Session Info */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-xl font-semibold text-gray-800">
              <span className="mr-2">📊</span>
              Processing Results
            </h2>
            <p className="text-sm text-gray-500 mt-1">Session: {sessionId}</p>
          </div>
          <div className="flex items-center space-x-3">
            <label className="flex items-center">
              <input
                type="checkbox"
                checked={autoRefresh}
                onChange={(e) => setAutoRefresh(e.target.checked)}
                className="mr-2 rounded border-gray-300 text-primary-600 focus:ring-primary-500"
              />
              <span className="text-sm text-gray-600">Auto-refresh</span>
            </label>
            <button
              onClick={() => { onRefresh(); fetchStatus(); }}
              className="p-2 text-gray-500 hover:text-gray-700 border border-gray-300 rounded-md hover:bg-gray-50 transition-colors duration-200"
              disabled={loading}
              title="Refresh status"
            >
              <span className={loading ? 'animate-spin' : ''}>🔄</span>
            </button>
          </div>
        </div>

        {/* Overall Progress */}
        <div className="mb-6">
          <div className="flex justify-between text-sm text-gray-600 mb-2">
            <span>Overall Progress</span>
            <span>{progress.toFixed(0)}%</span>
          </div>
          <div className="w-full bg-gray-200 rounded-full h-3">
            <div 
              className={`h-3 rounded-full transition-all duration-500 ${
                progress === 100 ? 'bg-success-500' : 
                progress === 0 ? 'bg-error-500' : 'bg-primary-500'
              }`}
              style={{ width: `${progress}%` }}
            ></div>
          </div>
        </div>

        {/* Status Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          {/* Overall Status */}
          <div className="text-center">
            <div className={`inline-flex items-center px-3 py-1 rounded-full text-sm font-medium border ${getStatusColor(status.overall_status)}`}>
              {status.overall_status === 'completed' && '✅'}
              {status.overall_status === 'failed' && '❌'}
              {status.overall_status === 'processing' && '⏳'}
              {status.overall_status === 'committing' && '💾'}
              {status.overall_status === 'staged_ready_for_commit' && '📋'}
              <span className="ml-1 capitalize">{status.overall_status.replace('_', ' ')}</span>
            </div>
            <p className="text-xs text-gray-500 mt-1">Overall Status</p>
          </div>

          {/* Staged Files */}
          <div className="text-center">
            <div className="text-2xl font-bold text-primary-600">
              {status.staging_stats.total_files}
            </div>
            <p className="text-xs text-gray-500">Staged Files</p>
            <p className="text-xs text-gray-400">
              {formatBytes(status.staging_stats.total_size_bytes)}
            </p>
          </div>

          {/* Worker Tasks */}
          <div className="text-center">
            <div className="text-2xl font-bold text-success-600">
              {status.workers_status.completed_tasks || 0}/{status.workers_status.total_tasks || 0}
            </div>
            <p className="text-xs text-gray-500">Tasks Completed</p>
            <div className={`text-xs px-2 py-1 rounded ${getStatusColor(status.workers_status.status)}`}>
              {status.workers_status.status}
            </div>
          </div>

          {/* Committed Rows */}
          <div className="text-center">
            <div className="text-2xl font-bold text-purple-600">
              {status.committer_status.result?.total_rows?.toLocaleString() || 0}
            </div>
            <p className="text-xs text-gray-500">Total Rows</p>
            <div className={`text-xs px-2 py-1 rounded ${getStatusColor(status.committer_status.status)}`}>
              {status.committer_status.status}
            </div>
          </div>
        </div>
      </div>

      {/* Detailed Status Information */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Staging Information */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-medium text-gray-900 mb-4">
            📂 Staging Details
          </h3>
          <div className="space-y-3 text-sm">
            <div className="flex justify-between">
              <span className="text-gray-600">Total Files:</span>
              <span className="font-mono">{status.staging_stats.total_files}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Total Size:</span>
              <span className="font-mono">{status.staging_stats.total_size_mb.toFixed(2)} MB</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Manifest:</span>
              <span className={`px-2 py-1 rounded text-xs ${
                status.staging_stats.manifest_exists 
                  ? 'bg-success-100 text-success-800' 
                  : 'bg-gray-100 text-gray-600'
              }`}>
                {status.staging_stats.manifest_exists ? '✅ Exists' : '⏳ Pending'}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Staging Complete:</span>
              <span className={`px-2 py-1 rounded text-xs ${
                status.staging_stats.staging_complete 
                  ? 'bg-success-100 text-success-800' 
                  : 'bg-warning-100 text-warning-800'
              }`}>
                {status.staging_stats.staging_complete ? '✅ Complete' : '⏳ In Progress'}
              </span>
            </div>
            
            {status.staging_stats.manifest_data?.metadata?.metadata && (
              <div className="mt-4 pt-4 border-t border-gray-200">
                <h4 className="font-medium text-gray-700 mb-2">Manifest Metadata</h4>
                <div className="space-y-2">
                  <div className="flex justify-between">
                    <span className="text-gray-600">Successful Batches:</span>
                    <span className="font-mono text-success-600">
                      {status.staging_stats.manifest_data.metadata.successful_batches || 0}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-600">Failed Batches:</span>
                    <span className="font-mono text-error-600">
                      {status.staging_stats.manifest_data.metadata.failed_batches_count || 0}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-600">Total Queries:</span>
                    <span className="font-mono text-primary-600">
                      {status.staging_stats.manifest_data.metadata.metadata?.total_queries_processed?.toLocaleString() || 0}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-600">Successful Queries:</span>
                    <span className="font-mono text-success-600">
                      {status.staging_stats.manifest_data.metadata.metadata?.total_queries_successful?.toLocaleString() || 0}
                    </span>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Worker Status */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-medium text-gray-900 mb-4">
            ⚡ Worker Status
          </h3>
          <div className="space-y-3 text-sm">
            <div className="flex justify-between">
              <span className="text-gray-600">Status:</span>
              <span className={`px-2 py-1 rounded text-xs ${getStatusColor(status.workers_status.status)}`}>
                {status.workers_status.status}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Completed Tasks:</span>
              <span className="font-mono text-success-600">
                {status.workers_status.completed_tasks || 0}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Total Tasks:</span>
              <span className="font-mono">{status.workers_status.total_tasks || 0}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Successful Tasks:</span>
              <span className="font-mono text-success-600">
                {status.workers_status.successful_tasks || 0}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Failed Tasks:</span>
              <span className="font-mono text-error-600">
                {status.workers_status.failed_tasks || 0}
              </span>
            </div>
            
            {status.workers_status.error && (
              <div className="mt-4 p-3 bg-error-50 border border-error-200 rounded-md">
                <p className="text-error-700 text-xs">{status.workers_status.error}</p>
              </div>
            )}
          </div>
        </div>

        {/* Committer Status */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-medium text-gray-900 mb-4">
            💾 Committer Status
          </h3>
          <div className="space-y-3 text-sm">
            <div className="flex justify-between">
              <span className="text-gray-600">Status:</span>
              <span className={`px-2 py-1 rounded text-xs ${getStatusColor(status.committer_status.status)}`}>
                {status.committer_status.status}
              </span>
            </div>
            
            {status.committer_status.result && (
              <>
                <div className="flex justify-between">
                  <span className="text-gray-600">Success:</span>
                  <span className={`px-2 py-1 rounded text-xs ${
                    status.committer_status.result.success 
                      ? 'bg-success-100 text-success-800' 
                      : 'bg-error-100 text-error-800'
                  }`}>
                    {status.committer_status.result.success ? '✅ Yes' : '❌ No'}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-600">Committed Files:</span>
                  <span className="font-mono">{status.committer_status.result.committed_files}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-600">Total Rows:</span>
                  <span className="font-mono font-bold text-primary-600">
                    {status.committer_status.result.total_rows?.toLocaleString()}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-600">Duration:</span>
                  <span className="font-mono">
                    {status.committer_status.result.total_commit_duration_seconds?.toFixed(2)}s
                  </span>
                </div>
                
                {status.committer_status.result.commit_type && (
                  <div className="flex justify-between">
                    <span className="text-gray-600">Commit Type:</span>
                    <span className="font-mono capitalize">
                      {status.committer_status.result.commit_type}
                    </span>
                  </div>
                )}
                
                {/* Chunk Visualization */}
                {(status.committer_status.result.commit_type === 'streaming' && 
                  status.committer_status.result.total_chunks && 
                  status.committer_status.result.total_chunks > 1) && (
                    <div className="mt-4 border-t pt-4">
                      <h4 className="text-sm font-semibold text-gray-700 mb-3 flex items-center">
                        <span className="mr-2">📦</span>
                        Commit Chunks Progress
                      </h4>
                      
                      {/* Chunk Summary */}
                      <div className="grid grid-cols-3 gap-2 mb-3 text-xs">
                        <div className="bg-success-50 border border-success-200 rounded p-2 text-center">
                          <div className="font-bold text-success-600">
                            {status.committer_status.result.successful_chunks}
                          </div>
                          <div className="text-success-500">Successful</div>
                        </div>
                        <div className="bg-error-50 border border-error-200 rounded p-2 text-center">
                          <div className="font-bold text-error-600">
                            {status.committer_status.result.failed_chunks || 0}
                          </div>
                          <div className="text-error-500">Failed</div>
                        </div>
                        <div className="bg-gray-50 border border-gray-200 rounded p-2 text-center">
                          <div className="font-bold text-gray-600">
                            {status.committer_status.result.chunk_size}
                          </div>
                          <div className="text-gray-500">Files/Chunk</div>
                        </div>
                      </div>
                      
                      {/* Visual Progress Bar for Chunks */}
                      <div className="mb-2">
                        <div className="flex items-center justify-between text-xs text-gray-600 mb-1">
                          <span>Chunks Progress</span>
                          <span>
                            {status.committer_status.result.successful_chunks || 0} / {status.committer_status.result.total_chunks || 0}
                          </span>
                        </div>
                        <div className="w-full bg-gray-200 rounded-full h-2">
                          <div 
                            className="bg-success-500 h-2 rounded-full transition-all duration-300"
                            style={{
                              width: `${((status.committer_status.result.successful_chunks || 0) / (status.committer_status.result.total_chunks || 1)) * 100}%`
                            }}
                          ></div>
                        </div>
                      </div>
                      
                      {/* Individual Chunk Indicators */}
                      <div className="flex flex-wrap gap-1 mb-2">
                        {status.committer_status.result.total_chunks && Array.from({ length: Math.min(status.committer_status.result.total_chunks, 20) }, (_, i) => {
                          const chunkNumber = i + 1;
                          const result = status.committer_status.result!;
                          const isSuccessful = chunkNumber <= (result.successful_chunks || 0);
                          const isFailed = result.failed_chunk_details?.some(
                            (chunk: any) => chunk.chunk_index === chunkNumber
                          );
                          
                          return (
                            <div
                              key={i}
                              className={`w-3 h-3 rounded-sm text-xs flex items-center justify-center ${
                                isFailed 
                                  ? 'bg-error-500 text-white' 
                                  : isSuccessful 
                                  ? 'bg-success-500 text-white' 
                                  : 'bg-gray-300'
                              }`}
                              title={`Chunk ${chunkNumber}: ${
                                isFailed ? 'Failed' : isSuccessful ? 'Success' : 'Pending'
                              }`}
                            >
                            </div>
                          );
                        })}
                        {status.committer_status.result.total_chunks && status.committer_status.result.total_chunks > 20 && (
                          <div className="text-xs text-gray-500 flex items-center ml-2">
                            +{status.committer_status.result.total_chunks - 20} more
                          </div>
                        )}
                      </div>
                      
                      {/* Failed Chunk Details */}
                      {status.committer_status.result.failed_chunk_details && 
                       status.committer_status.result.failed_chunk_details.length > 0 && (
                        <div className="mt-3 border-t pt-3">
                          <h5 className="text-xs font-medium text-error-600 mb-2">Failed Chunks:</h5>
                          <div className="space-y-1 max-h-24 overflow-y-auto">
                            {status.committer_status.result.failed_chunk_details.map((chunk: any, idx: number) => (
                              <div key={idx} className="text-xs bg-error-50 border border-error-200 rounded p-2">
                                <div className="flex justify-between items-start">
                                  <span className="font-mono text-error-600">
                                    Chunk {chunk.chunk_index} ({chunk.files_count} files)
                                  </span>
                                  <span className="text-error-500">
                                    {chunk.duration?.toFixed(1)}s
                                  </span>
                                </div>
                                <div className="text-error-600 mt-1 break-words">
                                  {chunk.error && chunk.error.length > 60 
                                    ? `${chunk.error.substring(0, 60)}...` 
                                    : chunk.error}
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                )}
              </>
            )}
            
            {status.committer_status.error && (
              <div className="mt-4 p-3 bg-error-50 border border-error-200 rounded-md">
                <p className="text-error-700 text-xs">{status.committer_status.error}</p>
              </div>
            )}
          </div>
        </div>

        {/* Validation Results */}
        {(status.validation_result || status.overall_status === 'completed') && (
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <h3 className="text-lg font-medium text-gray-900 mb-4">
              🔍 Validation Results
            </h3>
            <div className="space-y-3 text-sm">
              {status.overall_status === 'completed' ? (
                // Show completion message for completed sessions
                <div className="p-3 bg-success-50 border border-success-200 rounded-md">
                  <div className="flex items-center">
                    <span className="text-success-600 mr-2">✅</span>
                    <div>
                      <p className="text-success-800 font-medium">Session completed successfully</p>
                      <p className="text-success-700 text-xs mt-1">
                        All staged files were validated, committed to Iceberg, and cleaned up
                      </p>
                    </div>
                  </div>
                </div>
              ) : status.validation_result?.error ? (
                <div className="p-3 bg-error-50 border border-error-200 rounded-md">
                  <p className="text-error-700">{status.validation_result.error}</p>
                </div>
              ) : (
                <>
                  {status.validation_result?.files_found !== undefined && (
                    <div className="flex justify-between">
                      <span className="text-gray-600">Files Found:</span>
                      <span className="font-mono">{status.validation_result.files_found}</span>
                    </div>
                  )}
                  {status.validation_result?.total_size_mb !== undefined && (
                    <div className="flex justify-between">
                      <span className="text-gray-600">Total Size:</span>
                      <span className="font-mono">{status.validation_result.total_size_mb} MB</span>
                    </div>
                  )}
                  {status.validation_result?.query_column && (
                    <div className="flex justify-between">
                      <span className="text-gray-600">Query Column:</span>
                      <span className="font-mono">{status.validation_result.query_column}</span>
                    </div>
                  )}
                </>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Session Details */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-medium text-gray-900 mb-4">
          ℹ️ Session Details
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
          <div>
            <span className="font-medium text-gray-700">Session ID:</span>
            <span className="ml-2 text-gray-600 font-mono">{status.session_id}</span>
          </div>
          <div>
            <span className="font-medium text-gray-700">Last Updated:</span>
            <span className="ml-2 text-gray-600">
              {new Date(status.timestamp).toLocaleString('en-IN', {
                year: 'numeric',
                month: '2-digit', 
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
                hour12: false,
                timeZone: 'Asia/Kolkata'
              })}
            </span>
          </div>
        </div>
      </div>
    </div>
  )
}