import React, { useState } from 'react'
import './TemporalFilterBuilder.css'

interface ColumnMetadata {
  column_name: string
  display_name: string
  temporal_role?: 'none' | 'start_date' | 'stop_date' | 'duration'
  temporal_paired_column?: string
  temporal_unit?: 'days' | 'months' | 'years'
}

interface Table {
  name: string
  displayName: string
}

interface TemporalFilterBuilderProps {
  currentTable: string
  currentColumn: ColumnMetadata
  allTables: Table[]
  columnsByTable: Record<string, ColumnMetadata[]>
  onCreateFilter: (filter: any) => void
  onClose: () => void
}

export default function TemporalFilterBuilder({
  currentTable,
  currentColumn,
  allTables,
  columnsByTable,
  onCreateFilter,
  onClose
}: TemporalFilterBuilderProps) {
  const [operator, setOperator] = useState<'temporal_before' | 'temporal_after' | 'temporal_within'>('temporal_before')
  const [referenceTable, setReferenceTable] = useState<string>(currentTable)
  const [referenceColumn, setReferenceColumn] = useState<string>('')
  const [daysThreshold, setDaysThreshold] = useState<number>(30)

  // Get temporal columns from a specific table
  const getTemporalColumns = (tableName: string) => {
    const tableColumns = columnsByTable[tableName] || []
    return tableColumns.filter(col =>
      col.temporal_role &&
      col.temporal_role !== 'none' &&
      !(col.column_name === currentColumn.column_name && tableName === currentTable)
    )
  }

  const temporalColumns = getTemporalColumns(referenceTable)

  const handleCreate = () => {
    if (!referenceColumn) {
      alert('Please select a reference column')
      return
    }

    const filter: any = {
      column: currentColumn.column_name,
      operator,
      temporal_reference_column: referenceColumn,
      temporal_reference_table: referenceTable !== currentTable ? referenceTable : undefined,
      tableName: currentTable
    }

    // Add value for temporal_within operator
    if (operator === 'temporal_within') {
      filter.value = daysThreshold
    }

    onCreateFilter(filter)
    onClose()
  }

  return (
    <div className="temporal-filter-builder">
      <div className="builder-header">
        <h4>Create Temporal Filter</h4>
        <button className="close-btn" onClick={onClose}>×</button>
      </div>

      <div className="builder-content">
        <div className="filter-preview">
          <strong>{currentColumn.display_name || currentColumn.column_name}</strong>
          <span className="operator-text">
            {operator === 'temporal_before' && 'occurs before'}
            {operator === 'temporal_after' && 'occurs after'}
            {operator === 'temporal_within' && `within ${daysThreshold} days of`}
          </span>
          <span className="reference-text">
            {referenceColumn ? (
              <strong>{referenceColumn}</strong>
            ) : (
              <em>select column...</em>
            )}
          </span>
        </div>

        <div className="form-group">
          <label>Temporal Relationship</label>
          <select
            value={operator}
            onChange={e => setOperator(e.target.value as any)}
            className="operator-select"
          >
            <option value="temporal_before">Before</option>
            <option value="temporal_after">After</option>
            <option value="temporal_within">Within</option>
          </select>
          <div className="help-text">
            {operator === 'temporal_before' && 'Filter to rows where this event occurs before the reference event'}
            {operator === 'temporal_after' && 'Filter to rows where this event occurs after the reference event'}
            {operator === 'temporal_within' && 'Filter to rows where this event occurs within a specified number of days of the reference event'}
          </div>
        </div>

        {operator === 'temporal_within' && (
          <div className="form-group">
            <label>Days Threshold</label>
            <input
              type="number"
              value={daysThreshold}
              onChange={e => setDaysThreshold(parseInt(e.target.value) || 0)}
              min="0"
              className="days-input"
            />
            <div className="help-text">
              Maximum number of days between the two events (uses absolute difference)
            </div>
          </div>
        )}

        <div className="form-group">
          <label>Reference Table</label>
          <select
            value={referenceTable}
            onChange={e => {
              setReferenceTable(e.target.value)
              setReferenceColumn('') // Reset column selection when table changes
            }}
            className="table-select"
          >
            {allTables.map(table => (
              <option key={table.name} value={table.name}>
                {table.displayName}
              </option>
            ))}
          </select>
          <div className="help-text">
            {referenceTable === currentTable
              ? 'Same table (compares columns within each row)'
              : 'Cross-table (requires join via patient_id)'}
          </div>
        </div>

        <div className="form-group">
          <label>Reference Column</label>
          <select
            value={referenceColumn}
            onChange={e => setReferenceColumn(e.target.value)}
            className="column-select"
          >
            <option value="">Select temporal column...</option>
            {temporalColumns.map(col => (
              <option key={col.column_name} value={col.column_name}>
                {col.display_name || col.column_name} ({col.temporal_role})
              </option>
            ))}
          </select>
          {temporalColumns.length === 0 && (
            <div className="warning-text">
              No temporal columns found in {referenceTable}. Configure temporal columns in Dataset Management first.
            </div>
          )}
        </div>

        <div className="builder-actions">
          <button onClick={onClose} className="cancel-btn">
            Cancel
          </button>
          <button
            onClick={handleCreate}
            className="create-btn"
            disabled={!referenceColumn}
          >
            Create Filter
          </button>
        </div>
      </div>
    </div>
  )
}
