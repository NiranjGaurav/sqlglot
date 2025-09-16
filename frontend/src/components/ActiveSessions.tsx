'use client'

import { useState, useEffect } from 'react'
import { ProcessingStatus } from '@/types/api'

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
  created_at: string | null
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
  const [error, setError] = useState<string | null>(null)
  const [searchTerm, setSearchTerm] = useState('')

  // Fetch lightweight session list
  const fetchSessions = async () => {
    setLoading(true)
    setError(null)
    
    try {
      const response = await fetch('/api/sessions/list', {
        cache: 'no-cache',
        headers: {
          'Cache-Control': 'no-cache, no-store, must-revalidate',
          'Pragma': 'no-cache'
        }
      })
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`)
      }
      
      const data = await response.json()
      const sessions = data.sessions || []
      
      // Convert to our SessionData format
      const sessionData: SessionData[] = sessions.map((session: any) => ({
        id: session.session_id,
        company_name: session.company_name,
        session_name: session.session_name,
        status: session.status,
        created_at: session.created_at, // Keep null if not available
        // Don't load full status until selected
        currentStatus: undefined
      }))
      
      setSessions(sessionData)
      
    } catch (error) {
      console.error('Failed to fetch sessions:', error)
      setError(error instanceof Error ? error.message : 'Failed to load sessions')
    } finally {
      setLoading(false)
    }
  }

  // Load full status for selected session only
  const loadSessionStatus = async (sessionId: string) => {
    try {
      const response = await fetch(`/api/processing-status/${sessionId}?_=${Date.now()}`, {
        cache: 'no-cache',
        headers: {
          'Cache-Control': 'no-cache, no-store, must-revalidate',
          'Pragma': 'no-cache'
        }
      })
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`)
      }
      
      const status = await response.json()
      
      // Update the specific session with full status
      setSessions(prev => prev.map(session => 
        session.id === sessionId 
          ? { 
              ...session, 
              status: status.overall_status || 'processing',
              completed_tasks: status.workers_status?.completed_tasks,
              total_tasks: status.workers_status?.total_tasks,
              currentStatus: status
            }
          : session
      ))
      
    } catch (error) {
      console.error(`Failed to load status for session ${sessionId}:`, error)
    }
  }

  // Enhanced session select handler
  const handleSessionSelect = (sessionId: string) => {
    onSessionSelect(sessionId)
    // Load full status when session is selected
    loadSessionStatus(sessionId)
  }

  useEffect(() => {
    fetchSessions()
  }, [refreshTrigger])

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

  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case 'completed':
        return 'bg-success-100 text-success-800 border-success-200'
      case 'failed':
        return 'bg-error-100 text-error-800 border-error-200'
      case 'processing':
        return 'bg-primary-100 text-primary-800 border-primary-200'
      case 'committing':
        return 'bg-warning-100 text-warning-800 border-warning-200'
      case 'staged_ready_for_commit':
        return 'bg-purple-100 text-purple-800 border-purple-200'
      default:
        return 'bg-gray-100 text-gray-800 border-gray-200'
    }
  }

  const getStatusIcon = (status: string) => {
    switch (status.toLowerCase()) {
      case 'completed': return '✅'
      case 'failed': return '❌'
      case 'processing': return '⏳'
      case 'committing': return '💾'
      case 'staged_ready_for_commit': return '📋'
      default: return '❓'
    }
  }

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-xl font-semibold text-gray-800">
            <span className="mr-2">📋</span>
            Active Sessions
          </h2>
          <p className="text-sm text-gray-600 mt-1">
            {filteredSessions.length} session{filteredSessions.length !== 1 ? 's' : ''} found
          </p>
        </div>
        <div className="flex items-center space-x-3">
          <div className="relative">
            <input
              type="text"
              placeholder="Search sessions..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-64 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
            />
            <span className="absolute right-3 top-2.5 text-gray-400">🔍</span>
          </div>
          <button
            onClick={() => { onRefresh(); fetchSessions(); }}
            className="p-2 text-gray-500 hover:text-gray-700 border border-gray-300 rounded-md hover:bg-gray-50 transition-colors duration-200"
            disabled={loading}
            title="Refresh sessions"
          >
            <span className={loading ? 'animate-spin' : ''}>🔄</span>
          </button>
        </div>
      </div>

      {loading && sessions.length === 0 ? (
        <div className="text-center py-8">
          <div className="animate-spin text-4xl mb-4">⏳</div>
          <p className="text-gray-500">Loading sessions...</p>
        </div>
      ) : error ? (
        <div className="text-center py-8">
          <div className="text-4xl mb-4">❌</div>
          <p className="text-gray-500 mb-4">{error}</p>
          <button
            onClick={fetchSessions}
            className="px-4 py-2 bg-primary-600 text-white rounded-md hover:bg-primary-700 transition-colors duration-200"
          >
            🔄 Retry
          </button>
        </div>
      ) : filteredSessions.length === 0 ? (
        <div className="text-center py-8">
          <div className="text-4xl mb-4">📭</div>
          <p className="text-gray-500">No active sessions found</p>
          {searchTerm && (
            <button
              onClick={() => setSearchTerm('')}
              className="mt-2 text-primary-600 hover:text-primary-700 underline"
            >
              Clear search
            </button>
          )}
        </div>
      ) : (
        <div className="space-y-3">
          {filteredSessions.map((session: SessionData) => {
            const isSelected = selectedSession === session.id
            return (
              <div 
                key={session.id} 
                onClick={() => handleSessionSelect(session.id)}
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
                    <div className={`text-xs mt-2 ${
                      isSelected ? 'text-primary-600' : 'text-gray-500'
                    }`}>
                      {session.completed_tasks !== undefined && session.total_tasks !== undefined && (
                        <div>
                          Tasks: {session.completed_tasks}/{session.total_tasks}
                        </div>
                      )}
                      <div>
                        Started: {(() => {
                          if (!session.created_at) return 'Unknown'
                          try {
                            const date = new Date(session.created_at)
                            return isNaN(date.getTime()) ? 'Unknown' : date.toLocaleString()
                          } catch {
                            return 'Unknown'
                          }
                        })()}
                      </div>
                    </div>
                  </div>
                  <span className={`px-2 py-1 text-xs rounded-full flex items-center ${getStatusColor(session.status)}`}>
                    <span className="mr-1">{getStatusIcon(session.status)}</span>
                    {session.status.replace('_', ' ')}
                  </span>
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}