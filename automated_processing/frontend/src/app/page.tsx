'use client'

import { useState } from 'react'
import BatchProcessor from '@/components/BatchProcessor'
import ProcessingResults from '@/components/ProcessingResults'
import ActiveSessions from '@/components/ActiveSessions'

type TabType = 'start-processing' | 'query-conversion' | 'select-session'

export default function HomePage() {
  const [activeTab, setActiveTab] = useState<TabType>('start-processing')
  const [refreshTrigger, setRefreshTrigger] = useState(0)
  const [selectedSession, setSelectedSession] = useState<string | null>(null)

  const handleRefresh = () => {
    setRefreshTrigger(prev => prev + 1)
  }

  const handleTabSwitch = (tabId: TabType) => {
    setActiveTab(tabId)
    
    // Auto-trigger session discovery when switching to select-session tab
    if (tabId === 'select-session') {
      handleRefresh()
    }
  }

  const handleSessionSelect = (sessionId: string) => {
    setSelectedSession(sessionId)
    setActiveTab('query-conversion') // Switch to results tab
  }

  const tabs = [
    { id: 'start-processing', label: '🚀 Start Processing', icon: '⚡' },
    { id: 'query-conversion', label: '📊 Query Conversion', icon: '🔄' },
    { id: 'select-session', label: '📋 Select Session', icon: '🎯' },
  ] as const

  return (
    <div className="space-y-6">
      {/* Tab Navigation */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200">
        <div className="border-b border-gray-200">
          <nav className="-mb-px flex">
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => handleTabSwitch(tab.id)}
                className={`flex-1 py-4 px-6 text-center border-b-2 font-medium text-sm transition-colors duration-200 ${
                  activeTab === tab.id
                    ? 'border-primary-500 text-primary-600 bg-primary-50'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300 hover:bg-gray-50'
                }`}
              >
                <div className="flex items-center justify-center space-x-2">
                  <span className="text-lg">{tab.icon}</span>
                  <span>{tab.label}</span>
                </div>
              </button>
            ))}
          </nav>
        </div>
      </div>

      {/* Tab Content */}
      <div className="animate-fadeIn">
        {activeTab === 'start-processing' && (
          <div className="space-y-6">
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
              <div className="mb-4">
                <h2 className="text-lg font-semibold text-gray-900">
                  Batch SQL Processing
                </h2>
                <p className="text-sm text-gray-600 mt-1">
                  Process parquet files containing SQL queries through distributed transpilation
                </p>
              </div>
              <BatchProcessor onSessionCreated={handleSessionSelect} />
            </div>
          </div>
        )}

        {activeTab === 'query-conversion' && (
          <div className="space-y-6">
            <ProcessingResults 
              sessionId={selectedSession} 
              refreshTrigger={refreshTrigger}
              onRefresh={handleRefresh}
            />
          </div>
        )}

        {activeTab === 'select-session' && (
          <div className="space-y-6">
            <ActiveSessions 
              refreshTrigger={refreshTrigger}
              onRefresh={handleRefresh}
              onSessionSelect={handleSessionSelect}
              selectedSession={selectedSession}
            />
          </div>
        )}
      </div>

      {/* Footer Info */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
        <div className="flex items-center justify-between text-sm text-gray-500">
          <div className="flex items-center space-x-4">
            <span>📡 Backend: FastAPI + Celery</span>
            <span>🗄️ Storage: Apache Iceberg</span>
            <span>⚡ Workers: Redis + S3 Staging</span>
          </div>
          <div>
            <span>v1.0.0</span>
          </div>
        </div>
      </div>
    </div>
  )
}