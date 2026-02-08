import React from 'react'
import type { Filter } from '../../../utils/filterHelpers'
import { FilterChip } from './FilterChip'

interface ActiveFiltersProps {
  filters: Filter[]
  setFilters: React.Dispatch<React.SetStateAction<Filter[]>>
  onSaveFilter: () => void
  onClearFilters: () => void
  getTableColor: (tableName: string) => string
  getFilterTableNameForCacheKey: (filter: Filter) => string | undefined
  targetFromCacheKey: (key?: string) => string | null
  formatRangeValue: (value: number) => string
  clearColumnFilter: (tableName: string, column: string, cacheKey?: string) => void
  datasetTables?: Array<{ name: string; displayName?: string }>
}

export function ActiveFilters({
  filters,
  setFilters,
  onSaveFilter,
  onClearFilters,
  getTableColor,
  getFilterTableNameForCacheKey,
  targetFromCacheKey,
  formatRangeValue,
  clearColumnFilter,
  datasetTables,
}: ActiveFiltersProps) {
  if (filters.length === 0) return null

  return (
    <div style={{
      marginBottom: '1rem',
      background: '#F5F5F5',
      padding: '1rem',
      borderRadius: '8px',
      border: '1px solid #E0E0E0'
    }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.5rem' }}>
        <strong style={{ fontSize: '0.875rem' }}>Active Filters:</strong>
        <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
          <button
            onClick={onSaveFilter}
            style={{
              padding: '0.25rem 0.75rem',
              background: '#4CAF50',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer',
              fontSize: '0.75rem'
            }}
            title="Save current filters"
          >
            Save Filter
          </button>
          <button
            onClick={onClearFilters}
            style={{
              padding: '0.25rem 0.75rem',
              background: '#f44336',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer',
              fontSize: '0.75rem'
            }}
          >
            Clear All
          </button>
        </div>
      </div>
      <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap', alignItems: 'center' }}>
        {filters.map((filter, idx) => (
          <FilterChip
            key={idx}
            filter={filter}
            index={idx}
            showAndSeparator={idx > 0}
            getTableColor={getTableColor}
            getFilterTableNameForCacheKey={getFilterTableNameForCacheKey}
            targetFromCacheKey={targetFromCacheKey}
            formatRangeValue={formatRangeValue}
            clearColumnFilter={clearColumnFilter}
            setFilters={setFilters}
            datasetTables={datasetTables}
          />
        ))}
      </div>
    </div>
  )
}
