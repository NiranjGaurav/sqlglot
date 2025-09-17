'use client'

import { useState, useEffect } from 'react'
import { SessionDiscoveryResult, ProcessingStatus } from '@/types/api'

interface ActiveSessionsProps {
  refreshTrigger: number
  onRefresh: () => void
  onSessionSelect: (sessionId: string) => void
  selectedSession: string | null
}

interface SessionData {
  id: string
  company_name: string
  session_name?: string
  status: 'processing' | 'completed' | 'failed' | 'committing' | 'staged_ready_for_commit'
  completed_tasks?: number
  total_tasks?: number
  created_at: string
  currentStatus?: ProcessingStatus
}

export default function ActiveSessions({ 
  refreshTrigger, 
  onRefresh, 
  onSessionSelect, 
  selectedSession 
}: ActiveSessionsProps) {
  const [sessions, setSessions] = useState<SessionData[]>([])
  const [loading, setLoading] = useState(false)
  const [searchTerm, setSearchTerm] = useState('')

  useEffect(() => {
    let intervalId: NodeJS.Timeout | null = null
    let isMounted = true
    
    const fetchSessionsWithPolling = async () => {
      if (!isMounted) return
      
      setLoading(true)
      try {
        // Discover active sessions from Redis
        const activeSessionIds = await discoverSessionsFromRedis()
        
        if (activeSessionIds.length === 0) {
          setSessions([])
          return
        }
        
        // Show immediate feedback that sessions are being loaded
        if (sessions.length === 0) {
          // Create placeholder sessions while loading
          const placeholderSessions = activeSessionIds.map(sessionId => ({
            id: sessionId,
            company_name: 'Loading...',
            status: 'processing' as const,
            created_at: new Date().toISOString()
          }))
          setSessions(placeholderSessions)
        }
        
        // Fetch status for each discovered session (but show loading state immediately)
        const sessionPromises = activeSessionIds.map(async (sessionId: string, index: number) => {
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
            
            if (response.ok) {
              const status = await response.json()
              
              // Use the API overall_status directly from staging-based pipeline
              let sessionStatus: 'processing' | 'completed' | 'failed' | 'committing' | 'staged_ready_for_commit' = 'processing'
              
              if (status.overall_status) {
                const apiStatus = status.overall_status.toLowerCase().trim()
                if (apiStatus === 'completed') {
                  sessionStatus = 'completed'
                } else if (apiStatus === 'failed') {
                  sessionStatus = 'failed'
                } else if (apiStatus === 'committing') {
                  sessionStatus = 'committing'
                } else if (apiStatus === 'staged_ready_for_commit') {
                  sessionStatus = 'staged_ready_for_commit'
                } else {
                  // For "unknown" status, check if staging is complete and successful
                  if (status.staging_stats?.staging_complete && 
                      status.staging_stats?.manifest_data?.metadata?.successful_batches > 0 &&
                      status.staging_stats?.manifest_data?.metadata?.failed_batches_count === 0) {
                    sessionStatus = 'completed' // Treat as completed if all batches successful
                  } else {
                    sessionStatus = 'processing' // Default for processing, unknown, etc.
                  }
                }
              } else {
                // Fallback logic if no overall_status (shouldn't happen with new pipeline)
                if (status.workers_status?.failed_tasks > 0) {
                  sessionStatus = 'failed'
                } else if (status.committer_status?.status === 'SUCCESS') {
                  sessionStatus = 'completed'
                } else if (status.staging_stats?.staging_complete && 
                          status.staging_stats?.manifest_data?.metadata?.successful_batches > 0) {
                  sessionStatus = 'completed'
                }
              }
              
              // Get actual session start time from manifest metadata
              const sessionStartTime = status.staging_stats?.manifest_data?.metadata?.metadata?.created_at || 
                                       status.staging_stats?.manifest_data?.created_at || 
                                       status.timestamp ||
                                       new Date().toISOString() // fallback to current time
              
              return {
                id: sessionId,
                company_name: getCompanyNameFromStatus(status, sessionStatus),
                session_name: status.session_name,
                status: sessionStatus,
                completed_tasks: status.workers_status?.completed_tasks,
                total_tasks: status.workers_status?.total_tasks,
                created_at: sessionStartTime,
                currentStatus: status
              }
            }
            return null
          } catch (error) {
            console.warn(`Failed to get status for session ${sessionId}:`, error)
            return null
          }
        })
        
        const sessionsData = await Promise.all(sessionPromises)
        const validSessions = sessionsData.filter(Boolean) as SessionData[]
        
        if (isMounted) {
          setSessions(validSessions)
          
          // Update localStorage to match Redis reality
          const redisSessionIds = validSessions.map(s => s.id)
          localStorage.setItem('processing_sessions', JSON.stringify(redisSessionIds))
          
          // Check if any session is still processing
          const hasActiveSession = validSessions.some(s => {
            // Check current status from the API response
            if (s.currentStatus?.overall_status) {
              const apiStatus = s.currentStatus.overall_status
              return (
                apiStatus === 'processing' || 
                apiStatus === 'committing' ||
                apiStatus === 'staged_ready_for_commit' ||
                apiStatus === 'unknown'
              )
            }
            
            // Fallback to session status
            return (
              s.status === 'processing' || 
              s.status === 'committing' ||
              s.status === 'staged_ready_for_commit'
            )
          })
          
          // Schedule next poll only if there are active sessions
          if (hasActiveSession && isMounted) {
            intervalId = setTimeout(fetchSessionsWithPolling, 10000)
          } else {
            // Clean up completed sessions from localStorage
            const activeSessionIds = validSessions
              .filter(s => s.status !== 'completed' && s.status !== 'failed')
              .map(s => s.id)
            if (activeSessionIds.length === 0) {
              localStorage.removeItem('processing_sessions')
            } else {
              localStorage.setItem('processing_sessions', JSON.stringify(activeSessionIds))
            }
          }
        }
      } catch (error) {
        console.error('Failed to discover sessions from Redis:', error)
        if (isMounted) {
          setSessions([])
        }
      } finally {
        if (isMounted) {
          setLoading(false)
        }
      }
    }
    
    // Start initial fetch
    fetchSessionsWithPolling().catch(error => {
      console.error('Initial fetch failed:', error)
    })
    
    // Cleanup
    return () => {
      isMounted = false
      if (intervalId) {
        clearTimeout(intervalId)
      }
    }
  }, [refreshTrigger, sessions.length])

  const discoverSessionsFromRedis = async (): Promise<string[]> => {
    try {
      // Use the special 'discover_all' session ID to get all active sessions from Redis
      const response = await fetch(`/api/processing-status/discover_all?_=${Date.now()}`, {
        cache: 'no-cache',
        headers: {
          'Cache-Control': 'no-cache, no-store, must-revalidate',
          'Pragma': 'no-cache'
        }
      })
      if (response.ok) {
        const data: SessionDiscoveryResult = await response.json()
        return data.discovered_sessions || []
      } else {
        console.error('Failed to discover sessions from Redis')
        return []
      }
    } catch (error) {
      console.error('Error discovering sessions from Redis:', error)
      return []
    }
  }

  const getCompanyNameFromStatus = (status: ProcessingStatus, sessionStatus: string): string => {
    // Try to get company name from staging stats manifest metadata (nested structure)
    if (status.staging_stats?.manifest_data?.metadata?.metadata?.company_name) {
      return status.staging_stats.manifest_data.metadata.metadata.company_name
    }
    
    // Fallback to descriptive names based on status
    return sessionStatus === 'completed' ? 'Completed Session' : 
           sessionStatus === 'failed' ? 'Failed Session' : 'Processing Session'
  }

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed':
        return 'bg-success-100 text-success-800'
      case 'failed':
        return 'bg-error-100 text-error-800'
      case 'committing':
        return 'bg-warning-100 text-warning-800'
      case 'staged_ready_for_commit':
        return 'bg-purple-100 text-purple-800'
      default:
        return 'bg-primary-100 text-primary-800'
    }
  }

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'completed':
        return '✅'
      case 'failed':
        return '❌'
      case 'committing':
        return '💾'
      case 'staged_ready_for_commit':
        return '📋'
      default:
        return '⏳'
    }
  }

  // Filter sessions based on search term
  const filteredSessions = sessions.filter((session) => {
    if (!searchTerm.trim()) return true
    
    const searchLower = searchTerm.toLowerCase()
    return (
      session.id.toLowerCase().includes(searchLower) ||
      session.company_name.toLowerCase().includes(searchLower) ||
      session.status.toLowerCase().includes(searchLower) ||
      (session.session_name && session.session_name.toLowerCase().includes(searchLower))
    )
  })

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-xl font-semibold text-gray-800">
            <span className="mr-2">📋</span>
            Select Session
          </h2>
          <p className="text-sm text-gray-500 mt-1">
            Click on a session to view detailed batch status
          </p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={onRefresh}
            className="p-2 text-gray-500 hover:text-gray-700 border border-gray-300 rounded-md hover:bg-gray-50 transition-colors duration-200"
            disabled={loading}
            title="Refresh sessions"
          >
            <span className={loading ? 'animate-spin' : ''}>🔄</span>
          </button>
        </div>
      </div>

      {/* Search Box */}
      <div className="mb-4">
        <div className="relative">
          <input
            type="text"
            placeholder="Search sessions by name, ID, or status..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full px-4 py-2 pl-10 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
          />
          <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
            <span className="text-gray-400">🔍</span>
          </div>
          {searchTerm && (
            <button
              onClick={() => setSearchTerm('')}
              className="absolute inset-y-0 right-0 pr-3 flex items-center text-gray-400 hover:text-gray-600"
              title="Clear search"
            >
              ✕
            </button>
          )}
        </div>
        {searchTerm && (
          <p className="text-sm text-gray-500 mt-2">
            Showing {filteredSessions.length} of {sessions.length} sessions
          </p>
        )}
      </div>

      <div className="max-h-96 overflow-y-auto">
        {sessions.length === 0 ? (
          <div className="text-center text-gray-500 py-8">
            <div className="text-4xl mb-2">⏳</div>
            <p>No active sessions</p>
            <p className="text-sm mt-1">Start a new processing session to see it here</p>
          </div>
        ) : filteredSessions.length === 0 ? (
          <div className="text-center text-gray-500 py-8">
            <div className="text-4xl mb-2">🔍</div>
            <p>No sessions match your search</p>
            <button
              onClick={() => setSearchTerm('')}
              className="mt-2 px-3 py-1 text-sm text-primary-600 hover:text-primary-800"
            >
              Clear search
            </button>
          </div>
        ) : (
          <div className="space-y-3">
            {filteredSessions.map((session: SessionData) => {
              const isSelected = selectedSession === session.id
              return (
                <div 
                  key={session.id} 
                  onClick={() => onSessionSelect(session.id)}
                  className={`border rounded-lg p-4 cursor-pointer transition-all duration-200 hover:shadow-md ${
                    isSelected 
                      ? 'border-primary-500 bg-primary-50 shadow-md' 
                      : 'border-gray-200 hover:border-gray-300'
                  }`}
                >
                  <div className="flex justify-between items-start">
                    <div className="flex-1">
                      <div className="flex items-center gap-2">
                        <h3 className={`font-medium ${
                          isSelected ? 'text-primary-900' : 'text-gray-800'
                        }`}>
                          {session.session_name || session.id}
                        </h3>
                        {isSelected && (
                          <span className="text-primary-600 text-sm">✓</span>
                        )}
                      </div>
                      <p className={`text-sm ${
                        isSelected ? 'text-primary-700' : 'text-gray-600'
                      }`}>
                        {session.company_name}
                      </p>
                      {session.session_name && (
                        <p className={`text-xs font-mono ${
                          isSelected ? 'text-primary-600' : 'text-gray-500'
                        }`}>
                          ID: {session.id}
                        </p>
                      )}
                    </div>
                    <span className={`px-2 py-1 text-xs rounded-full flex items-center ${getStatusColor(session.status)}`}>
                      <span className="mr-1">{getStatusIcon(session.status)}</span>
                      {session.status.replace('_', ' ')}
                    </span>
                  </div>
                  <div className={`mt-2 text-sm ${
                    isSelected ? 'text-primary-600' : 'text-gray-500'
                  }`}>
                    <div>
                      Tasks: {session.completed_tasks || 0}/{session.total_tasks || 0}
                    </div>
                    <div>
                      Started: {(() => {
                        try {
                          const date = new Date(session.created_at)
                          return isNaN(date.getTime()) ? 'Unknown' : date.toLocaleString()
                        } catch {
                          return 'Unknown'
                        }
                      })()}
                    </div>
                    {session.currentStatus && (
                      <div className="mt-1 space-y-1">
                        {session.currentStatus.staging_stats && (
                          <div>
                            Files: {session.currentStatus.staging_stats.total_files} 
                            ({session.currentStatus.staging_stats.total_size_mb.toFixed(1)} MB)
                          </div>
                        )}
                        {session.currentStatus.committer_status?.result?.total_rows && (
                          <div className="font-medium">
                            ✅ {session.currentStatus.committer_status.result.total_rows.toLocaleString()} rows committed
                          </div>
                        )}
                        {session.currentStatus.workers_status?.failed_tasks && session.currentStatus.workers_status.failed_tasks > 0 && (
                          <div className="text-error-600">
                            ❌ {session.currentStatus.workers_status.failed_tasks} failed tasks
                          </div>
                        )}
                        {session.status === 'completed' && session.currentStatus.committer_status?.result && (
                          <div className="mt-2 text-xs space-y-1 border-t pt-2">
                            {/* Calculate timing information */}
                            {(() => {
                              const startTime = session.currentStatus.staging_stats?.manifest_data?.metadata?.metadata?.created_at
                              const stagingCompleted = session.currentStatus.staging_stats?.manifest_data?.metadata?.metadata?.staging_completed_at
                              const commitTime = session.currentStatus.committer_status.result.commit_duration_seconds
                              
                              if (startTime) {
                                const start = new Date(startTime)
                                const stagingEnd = stagingCompleted ? new Date(stagingCompleted) : null
                                const transpilationTime = stagingEnd ? Math.round((stagingEnd.getTime() - start.getTime()) / 1000) : null
                                const icebergWriteTime = commitTime ? Math.round(commitTime) : null
                                const totalTime = (transpilationTime || 0) + (icebergWriteTime || 0)
                                
                                return (
                                  <>
                                    {transpilationTime && (
                                      <div>⚡ Transpilation: {transpilationTime}s</div>
                                    )}
                                    {icebergWriteTime && (
                                      <div>💾 Iceberg Write: {icebergWriteTime}s</div>
                                    )}
                                    {totalTime > 0 && (
                                      <div>🕒 Total Time: {totalTime}s</div>
                                    )}
                                  </>
                                )
                              }
                              return null
                            })()}
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                </div>
              )
            })}
          </div>
        )}
      </div>

      {/* Session Summary */}
      {sessions.length > 0 && (
        <div className="mt-4 pt-4 border-t border-gray-200">
          <div className="flex justify-between text-sm text-gray-500">
            <span>Total Sessions: {sessions.length}</span>
            <div className="flex space-x-4">
              <span>✅ {sessions.filter(s => s.status === 'completed').length} completed</span>
              <span>⏳ {sessions.filter(s => ['processing', 'committing', 'staged_ready_for_commit'].includes(s.status)).length} active</span>
              <span>❌ {sessions.filter(s => s.status === 'failed').length} failed</span>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}