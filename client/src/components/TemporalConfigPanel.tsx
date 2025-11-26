import React, { useState } from 'react'
import './TemporalConfigPanel.css'

interface ColumnMetadata {
  column_name: string
  column_type: string
  display_name: string
  temporal_role?: 'none' | 'start_date' | 'stop_date' | 'duration'
  temporal_paired_column?: string
  temporal_unit?: 'days' | 'months' | 'years'
}

interface TemporalConfigPanelProps {
  datasetId: string
  tableId: string
  columns: ColumnMetadata[]
  onUpdate: () => void
}

interface TemporalConfig {
  temporalRole: 'none' | 'start_date' | 'stop_date' | 'duration'
  temporalPairedColumn: string | null
  temporalUnit: 'days' | 'months' | 'years'
  displayType?: string
}

// Auto-detect temporal role based on column name patterns
const detectTemporalRole = (columnName: string, currentRole?: string): 'none' | 'start_date' | 'stop_date' | 'duration' => {
  if (currentRole && currentRole !== 'none') {
    return currentRole as any
  }

  const nameLower = columnName.toLowerCase()

  // Check for start date patterns
  if (
    nameLower.includes('start_date') ||
    nameLower.includes('start_day') ||
    nameLower.includes('begin_date') ||
    nameLower.includes('begin_day') ||
    nameLower.endsWith('_start') ||
    nameLower.startsWith('start_') ||
    nameLower === 'start'
  ) {
    return 'start_date'
  }

  // Check for stop date patterns
  if (
    nameLower.includes('stop_date') ||
    nameLower.includes('stop_day') ||
    nameLower.includes('end_date') ||
    nameLower.includes('end_day') ||
    nameLower.endsWith('_stop') ||
    nameLower.endsWith('_end') ||
    nameLower.startsWith('stop_') ||
    nameLower.startsWith('end_') ||
    nameLower === 'stop' ||
    nameLower === 'end'
  ) {
    return 'stop_date'
  }

  // Check for duration patterns
  if (
    nameLower.includes('duration') ||
    nameLower.includes('length') ||
    nameLower.includes('period')
  ) {
    return 'duration'
  }

  return 'none'
}

// Determine appropriate display_type based on column type and temporal role
const getDisplayTypeForTemporal = (columnType: string, temporalRole: string): string => {
  if (temporalRole === 'none') {
    return 'auto'
  }

  // If column type is Date/DateTime, use datetime display
  if (columnType.includes('Date')) {
    return 'datetime'
  }

  // For numeric types (Int*, Float*), use numeric display
  if (columnType.includes('Int') || columnType.includes('Float')) {
    return 'numeric'
  }

  return 'numeric' // Default for temporal columns
}

export default function TemporalConfigPanel({ datasetId, tableId, columns, onUpdate }: TemporalConfigPanelProps) {
  const [saving, setSaving] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const updateColumnTemporal = async (columnName: string, columnType: string, config: TemporalConfig) => {
    setSaving(columnName)
    setError(null)

    try {
      // Auto-determine display_type based on column type and temporal role
      const displayType = getDisplayTypeForTemporal(columnType, config.temporalRole)

      const response = await fetch(
        `/api/datasets/${datasetId}/tables/${tableId}/columns/${encodeURIComponent(columnName)}`,
        {
          method: 'PATCH',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            ...config,
            displayType
          })
        }
      )

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.message || 'Failed to update column')
      }

      onUpdate()
    } catch (err: any) {
      setError(`Failed to update ${columnName}: ${err.message}`)
      console.error('Error updating column temporal config:', err)
    } finally {
      setSaving(null)
    }
  }

  // Filter out columns that might be temporal (numeric or datetime types)
  const potentialTemporalColumns = columns.filter(col => {
    const nameLower = col.column_name.toLowerCase()
    return (
      col.column_type.includes('Int') ||
      col.column_type.includes('Float') ||
      col.column_type.includes('Date') ||
      nameLower.includes('date') ||
      nameLower.includes('day') ||
      nameLower.includes('month') ||
      nameLower.includes('year') ||
      nameLower.includes('time')
    )
  })

  // Get available columns for pairing (start_date and stop_date columns)
  const getPairableColumns = (currentColumn: string, role: string) => {
    return columns
      .filter(col =>
        col.column_name !== currentColumn &&
        (col.temporal_role === 'start_date' || col.temporal_role === 'stop_date')
      )
      .map(col => col.column_name)
  }

  return (
    <div className="temporal-config-panel">
      <div className="panel-header">
        <h3>Temporal Column Configuration</h3>
        <p className="help-text">
          Configure which columns represent temporal data (dates relative to an anchor date, like days since diagnosis).
          Pair start and stop date columns to enable duration-based filtering.
        </p>
      </div>

      {error && (
        <div className="error-message">
          {error}
          <button onClick={() => setError(null)} className="close-error">×</button>
        </div>
      )}

      <div className="columns-table">
        <table>
          <thead>
            <tr>
              <th>Column Name</th>
              <th>Type</th>
              <th>Temporal Role</th>
              <th>Paired Column</th>
              <th>Unit</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {potentialTemporalColumns.map(column => {
              // Auto-detect temporal role if not already set
              const detectedRole = detectTemporalRole(column.column_name, column.temporal_role)
              const [tempRole, setTempRole] = useState(detectedRole)
              const [tempPairedColumn, setTempPairedColumn] = useState(column.temporal_paired_column || '')
              const [tempUnit, setTempUnit] = useState(column.temporal_unit || 'days')

              const hasChanges =
                tempRole !== (column.temporal_role || 'none') ||
                tempPairedColumn !== (column.temporal_paired_column || '') ||
                tempUnit !== (column.temporal_unit || 'days')

              const handleSave = () => {
                updateColumnTemporal(column.column_name, column.column_type, {
                  temporalRole: tempRole,
                  temporalPairedColumn: tempPairedColumn || null,
                  temporalUnit: tempUnit
                })
              }

              const isSaving = saving === column.column_name
              const pairableColumns = getPairableColumns(column.column_name, tempRole)
              const suggestedRole = detectedRole !== 'none' && detectedRole !== column.temporal_role

              return (
                <tr key={column.column_name} className={tempRole !== 'none' ? 'temporal-active' : ''}>
                  <td>
                    <span className="column-name">{column.column_name}</span>
                    <span className="display-name">{column.display_name}</span>
                    {suggestedRole && (
                      <span className="suggested-badge" title="Auto-detected based on column name">
                        ✨ Suggested
                      </span>
                    )}
                  </td>
                  <td>{column.column_type}</td>
                  <td>
                    <select
                      value={tempRole}
                      onChange={e => setTempRole(e.target.value as any)}
                      disabled={isSaving}
                      className={`role-select ${suggestedRole ? 'suggested' : ''}`}
                    >
                      <option value="none">None</option>
                      <option value="start_date">Start Date</option>
                      <option value="stop_date">Stop Date</option>
                      <option value="duration">Duration</option>
                    </select>
                  </td>
                  <td>
                    {(tempRole === 'start_date' || tempRole === 'stop_date') && (
                      <select
                        value={tempPairedColumn}
                        onChange={e => setTempPairedColumn(e.target.value)}
                        disabled={isSaving}
                        className="paired-select"
                      >
                        <option value="">None</option>
                        {pairableColumns.map(col => (
                          <option key={col} value={col}>
                            {col}
                          </option>
                        ))}
                      </select>
                    )}
                    {tempRole === 'none' && <span className="disabled-field">—</span>}
                    {tempRole === 'duration' && <span className="disabled-field">N/A</span>}
                  </td>
                  <td>
                    {tempRole !== 'none' && (
                      <select
                        value={tempUnit}
                        onChange={e => setTempUnit(e.target.value as any)}
                        disabled={isSaving}
                        className="unit-select"
                      >
                        <option value="days">Days</option>
                        <option value="months">Months</option>
                        <option value="years">Years</option>
                      </select>
                    )}
                    {tempRole === 'none' && <span className="disabled-field">—</span>}
                  </td>
                  <td>
                    {hasChanges && (
                      <button
                        onClick={handleSave}
                        disabled={isSaving}
                        className="save-button"
                      >
                        {isSaving ? 'Saving...' : 'Save'}
                      </button>
                    )}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>

        {potentialTemporalColumns.length === 0 && (
          <div className="no-columns-message">
            No potential temporal columns found. Temporal columns are typically numeric or date fields
            with names containing "date", "day", "month", "year", or "time".
          </div>
        )}
      </div>
    </div>
  )
}
