'use client'

import { useState, useEffect } from 'react'
import { BatchProcessingFormData, ProcessingSession, S3ValidationResult } from '@/types/api'

interface BatchProcessorProps {
  onSessionCreated?: (sessionId: string) => void
}

export default function BatchProcessor({ onSessionCreated }: BatchProcessorProps) {
  const [formData, setFormData] = useState<BatchProcessingFormData>({
    directory_path: '',
    company_name: '',
    from_dialect: 'snowflake',
    to_dialect: 'e6',
    query_column: '',
    batch_size: 10000,
    filters: '',
    session_name: ''
  })

  const [isProcessing, setIsProcessing] = useState(false)
  const [validationStatus, setValidationStatus] = useState<{
    isValidating: boolean
    isValid: boolean
    columns: string[]
    error?: string
  }>({
    isValidating: false,
    isValid: false,
    columns: []
  })

  const [result, setResult] = useState<ProcessingSession | null>(null)
  const [error, setError] = useState<string | null>(null)

  const dialectOptions = [
    { value: 'snowflake', label: 'Snowflake' },
    { value: 'bigquery', label: 'BigQuery' },
    { value: 'redshift', label: 'Amazon Redshift' },
    { value: 'athena', label: 'Amazon Athena' },
    { value: 'databricks', label: 'Databricks' },
    { value: 'postgres', label: 'PostgreSQL' },
    { value: 'mysql', label: 'MySQL' },
    { value: 'oracle', label: 'Oracle' },
    { value: 'tsql', label: 'SQL Server' },
  ]

  // Validate S3 bucket/path and get columns
  const validateS3Path = async () => {
    if (!formData.directory_path || !formData.directory_path.startsWith('s3://')) {
      return
    }

    setValidationStatus({ isValidating: true, isValid: false, columns: [] })

    try {
      const response = await fetch('/api/validate-s3-bucket', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: new URLSearchParams({
          s3_path: formData.directory_path
        })
      })

      const data: S3ValidationResult = await response.json()
      
      if (data.authenticated && data.columns) {
        setValidationStatus({
          isValidating: false,
          isValid: true,
          columns: data.columns
        })
      } else {
        setValidationStatus({
          isValidating: false,
          isValid: false,
          columns: [],
          error: data.error || 'Validation failed'
        })
      }
    } catch (err) {
      setValidationStatus({
        isValidating: false,
        isValid: false,
        columns: [],
        error: err instanceof Error ? err.message : 'Unknown error'
      })
    }
  }

  // Auto-validate when S3 path changes
  useEffect(() => {
    const timeoutId = setTimeout(() => {
      if (formData.directory_path.startsWith('s3://') && formData.directory_path.length > 5) {
        validateS3Path()
      }
    }, 1000) // Debounce for 1 second

    return () => clearTimeout(timeoutId)
  }, [formData.directory_path])

  const handleInputChange = (field: keyof BatchProcessingFormData, value: string | number) => {
    setFormData(prev => ({ ...prev, [field]: value }))
    setError(null) // Clear any previous errors
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    
    if (!formData.directory_path || !formData.company_name || !formData.query_column) {
      setError('Please fill in all required fields')
      return
    }

    if (formData.directory_path.startsWith('s3://') && !validationStatus.isValid) {
      setError('Please wait for S3 path validation to complete')
      return
    }

    setIsProcessing(true)
    setError(null)
    setResult(null)

    try {
      const response = await fetch('/api/process-parquet-directory-automated', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: new URLSearchParams({
          directory_path: formData.directory_path,
          company_name: formData.company_name,
          from_dialect: formData.from_dialect,
          to_dialect: formData.to_dialect,
          query_column: formData.query_column,
          batch_size: formData.batch_size.toString(),
          ...(formData.filters && { filters: formData.filters }),
          ...(formData.session_name && { session_name: formData.session_name })
        })
      })

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.detail || `HTTP ${response.status}`)
      }

      const sessionData: ProcessingSession = await response.json()
      setResult(sessionData)
      
      // Notify parent component
      if (onSessionCreated) {
        onSessionCreated(sessionData.session_id)
      }

    } catch (err) {
      setError(err instanceof Error ? err.message : 'Processing failed')
    } finally {
      setIsProcessing(false)
    }
  }

  return (
    <div className="space-y-6">
      <form onSubmit={handleSubmit} className="space-y-6">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Directory Path */}
          <div className="md:col-span-2">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Directory Path *
            </label>
            <input
              type="text"
              value={formData.directory_path}
              onChange={(e) => handleInputChange('directory_path', e.target.value)}
              placeholder="s3://your-bucket/path/to/parquet-files/ or /local/path/to/files/"
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
              required
            />
            {formData.directory_path.startsWith('s3://') && (
              <div className="mt-2">
                {validationStatus.isValidating ? (
                  <div className="flex items-center text-sm text-gray-500">
                    <div className="animate-spin mr-2">⏳</div>
                    Validating S3 path...
                  </div>
                ) : validationStatus.isValid ? (
                  <div className="flex items-center text-sm text-success-600">
                    <span className="mr-2">✅</span>
                    S3 path validated successfully
                  </div>
                ) : validationStatus.error ? (
                  <div className="text-sm text-error-600">
                    ❌ {validationStatus.error}
                  </div>
                ) : null}
              </div>
            )}
          </div>

          {/* Company Name */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Company Name *
            </label>
            <input
              type="text"
              value={formData.company_name}
              onChange={(e) => handleInputChange('company_name', e.target.value)}
              placeholder="test3"
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
              required
            />
            <p className="text-xs text-gray-500 mt-1">Used for Iceberg partitioning</p>
          </div>

          {/* Session Name */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Session Name (Optional)
            </label>
            <input
              type="text"
              value={formData.session_name}
              onChange={(e) => handleInputChange('session_name', e.target.value)}
              placeholder="My SQL Migration Session"
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
            />
          </div>

          {/* From Dialect */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Source Dialect *
            </label>
            <select
              value={formData.from_dialect}
              onChange={(e) => handleInputChange('from_dialect', e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
              required
            >
              {dialectOptions.map(option => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </div>

          {/* To Dialect */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Target Dialect
            </label>
            <select
              value={formData.to_dialect}
              onChange={(e) => handleInputChange('to_dialect', e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent bg-gray-50"
            >
              <option value="e6">E6 (default)</option>
            </select>
          </div>

          {/* Query Column */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Query Column *
            </label>
            {validationStatus.columns.length > 0 ? (
              <select
                value={formData.query_column}
                onChange={(e) => handleInputChange('query_column', e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                required
              >
                <option value="">Select column containing SQL queries</option>
                {validationStatus.columns.map(column => (
                  <option key={column} value={column}>{column}</option>
                ))}
              </select>
            ) : (
              <input
                type="text"
                value={formData.query_column}
                onChange={(e) => handleInputChange('query_column', e.target.value)}
                placeholder="query_string"
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                required
              />
            )}
          </div>

          {/* Batch Size */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Batch Size
            </label>
            <input
              type="number"
              value={formData.batch_size}
              onChange={(e) => handleInputChange('batch_size', parseInt(e.target.value) || 10000)}
              min="100"
              max="50000"
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
            />
            <p className="text-xs text-gray-500 mt-1">Number of queries per batch</p>
          </div>

          {/* Filters */}
          <div className="md:col-span-2">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Filters (JSON) - Optional
            </label>
            <textarea
              value={formData.filters}
              onChange={(e) => handleInputChange('filters', e.target.value)}
              placeholder='{"statement_type": "SELECT", "client_application": "PowerBI"}'
              rows={2}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
            />
            <p className="text-xs text-gray-500 mt-1">JSON string of column filters</p>
          </div>
        </div>

        {/* Submit Button */}
        <div className="flex items-center justify-between">
          <button
            type="submit"
            disabled={isProcessing || (formData.directory_path.startsWith('s3://') && !validationStatus.isValid)}
            className="flex items-center px-6 py-3 bg-primary-600 text-white rounded-md hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed transition-colors duration-200"
          >
            {isProcessing ? (
              <>
                <div className="animate-spin mr-2">⏳</div>
                Processing...
              </>
            ) : (
              <>
                <span className="mr-2">🚀</span>
                Start Processing
              </>
            )}
          </button>

          {formData.directory_path.startsWith('s3://') && (
            <button
              type="button"
              onClick={validateS3Path}
              disabled={validationStatus.isValidating}
              className="px-4 py-2 text-primary-600 border border-primary-300 rounded-md hover:bg-primary-50 transition-colors duration-200"
            >
              🔍 Validate S3 Path
            </button>
          )}
        </div>
      </form>

      {/* Error Display */}
      {error && (
        <div className="bg-error-50 border border-error-200 rounded-md p-4">
          <div className="flex items-center">
            <span className="text-error-600 mr-2">❌</span>
            <span className="text-error-700">{error}</span>
          </div>
        </div>
      )}

      {/* Success Result */}
      {result && (
        <div className="bg-success-50 border border-success-200 rounded-md p-6">
          <div className="flex items-center mb-4">
            <span className="text-success-600 mr-2 text-lg">✅</span>
            <h3 className="text-lg font-medium text-success-800">Processing Started Successfully!</h3>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
            <div>
              <span className="font-medium text-success-700">Session ID:</span>
              <span className="ml-2 text-success-600 font-mono">{result.session_id}</span>
            </div>
            <div>
              <span className="font-medium text-success-700">Total Batches:</span>
              <span className="ml-2 text-success-600">{result.total_batches}</span>
            </div>
            <div>
              <span className="font-medium text-success-700">Status:</span>
              <span className="ml-2 text-success-600 capitalize">{result.status}</span>
            </div>
            <div>
              <span className="font-medium text-success-700">Storage Table:</span>
              <span className="ml-2 text-success-600 font-mono">{result.iceberg_storage.table}</span>
            </div>
          </div>
          
          <div className="mt-4 space-y-3">
            <p className="text-success-700">{result.message}</p>
            
            <div className="bg-blue-50 border border-blue-200 rounded-md p-3">
              <div className="flex items-start">
                <span className="text-blue-600 mr-2 text-lg">📋</span>
                <div>
                  <p className="text-blue-800 font-medium text-sm">
                    {result.total_batches} batches created and queued for processing
                  </p>
                  <p className="text-blue-700 text-xs mt-1">
                    Switch to the <strong>&ldquo;📊 Processing Results&rdquo;</strong> tab to monitor progress in detail, 
                    including chunk visualization and real-time status updates.
                  </p>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}