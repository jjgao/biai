import React from 'react'
import type { Filter } from '../../../utils/filterHelpers'
import { unwrapNot, getFilterColumn, getFilterCountKey } from '../../../utils/filterHelpers'

interface FilterChipProps {
  filter: Filter
  index: number
  showAndSeparator: boolean
  getTableColor: (tableName: string) => string
  getFilterTableNameForCacheKey: (filter: Filter) => string | undefined
  targetFromCacheKey: (key?: string) => string | null
  formatRangeValue: (value: number) => string
  clearColumnFilter: (tableName: string, column: string, cacheKey?: string) => void
  setFilters: React.Dispatch<React.SetStateAction<Filter[]>>
  datasetTables?: Array<{ name: string; displayName?: string }>
}

export function FilterChip({
  filter,
  index,
  showAndSeparator,
  getTableColor,
  getFilterTableNameForCacheKey,
  targetFromCacheKey,
  formatRangeValue,
  clearColumnFilter,
  setFilters,
  datasetTables,
}: FilterChipProps) {
  const actualFilter = unwrapNot(filter)
  if (!actualFilter) return null
  const isNot = !!filter.not

  const columnName = getFilterColumn(actualFilter)
  const tableName = getFilterTableNameForCacheKey(actualFilter)
  const tableColor = tableName ? getTableColor(tableName) : '#9E9E9E'
  const table = datasetTables?.find(t => t.name === tableName)

  // Extract count-by table for border color
  const countByTable = targetFromCacheKey(filter.countByKey)
  const countByColor = countByTable ? getTableColor(countByTable) : tableColor

  let displayValue = String(actualFilter.value)
  let logicType = '' // For tooltip

  // Remove handler
  const removeHandler = () => {
    if (tableName && columnName) {
      clearColumnFilter(tableName, columnName, getFilterCountKey(filter))
    } else {
      setFilters(prev => prev.filter((_, i) => i !== index))
    }
  }

  // Toggle NOT wrapper
  const toggleNot = () => {
    setFilters(prev => {
      const updated = [...prev]
      if (isNot) {
        updated[index] = actualFilter
      } else {
        updated[index] = { not: actualFilter }
      }
      return updated
    })
  }

  if (actualFilter.operator === 'between' && Array.isArray(actualFilter.value)) {
    displayValue = `[${typeof actualFilter.value[0] === 'number' ? actualFilter.value[0].toFixed(2) : actualFilter.value[0]}, ${typeof actualFilter.value[1] === 'number' ? actualFilter.value[1].toFixed(2) : actualFilter.value[1]}]`
    logicType = 'Range'
  } else if (actualFilter.operator === 'in' && Array.isArray(actualFilter.value)) {
    const displayVals = actualFilter.value.map((v: string | number) => {
      if (v === '') return '(Empty)'
      if (v === ' ') return '(Space)'
      return v
    })
    if (actualFilter.value.length > 1) {
      displayValue = displayVals.slice(0, 3).join(' OR ')
      if (actualFilter.value.length > 3) {
        displayValue += ` OR ${actualFilter.value.length - 3} more...`
      }
    } else {
      displayValue = String(displayVals[0] || '')
    }
    logicType = actualFilter.value.length > 1 ? `OR (${actualFilter.value.length} values)` : 'Single value'
  } else if (actualFilter.operator === 'eq') {
    if (actualFilter.value === '') displayValue = '(Empty)'
    else if (actualFilter.value === ' ') displayValue = '(Space)'
    else displayValue = String(actualFilter.value)
    logicType = 'Equals'
  } else if (actualFilter.or && Array.isArray(actualFilter.or)) {
    const ranges = actualFilter.or
      .map(rangeFilter => rangeFilter as Filter)
      .filter(rangeFilter => rangeFilter.column === actualFilter.column && rangeFilter.operator === 'between' && Array.isArray(rangeFilter.value))
      .map(rangeFilter => {
        const [start, end] = rangeFilter.value
        const startLabel = typeof start === 'number' ? formatRangeValue(start) : String(start)
        const endLabel = typeof end === 'number' ? formatRangeValue(end) : String(end)
        return `${startLabel}–${endLabel}`
      })

    displayValue = ranges.join(' OR ')
    logicType = `OR (${ranges.length} ranges)`
  }
  const columnLabel = columnName ?? '(Column)'
  const notPrefix = isNot ? 'NOT: ' : ''
  const tooltipText = tableName
    ? `${table?.displayName || tableName}.${columnLabel}\n${notPrefix}${logicType}\nValue: ${displayValue}`
    : columnLabel

  return (
    <React.Fragment>
      {showAndSeparator && (
        <div style={{
          color: '#666',
          fontSize: '0.75rem',
          fontWeight: 600,
          padding: '0 0.25rem',
          userSelect: 'none'
        }}>
          AND
        </div>
      )}
      <div
        style={{
          background: isNot ? `linear-gradient(135deg, ${tableColor}DD, ${tableColor}BB)` : tableColor,
          padding: '0.25rem 0.75rem',
          borderRadius: '4px',
          fontSize: '0.875rem',
          display: 'flex',
          alignItems: 'center',
          gap: '0.5rem',
          outline: `4px solid ${countByColor}`,
          outlineOffset: '2px',
          border: isNot ? `2px dashed rgba(255,255,255,0.6)` : 'none',
          color: 'white',
          fontWeight: 500,
          opacity: isNot ? 0.9 : 1
        }}
        title={tooltipText}
      >
        {isNot && (
          <span style={{
            background: 'rgba(0,0,0,0.3)',
            padding: '0.1rem 0.35rem',
            borderRadius: '3px',
            fontSize: '0.7rem',
            fontWeight: 700,
            marginRight: '0.1rem'
          }}>
            NOT
          </span>
        )}
        <span style={{ textDecoration: isNot ? 'line-through' : 'none' }}>
          <strong>{columnLabel}:</strong> {displayValue}
        </span>
        <button
          onClick={toggleNot}
          style={{
            background: isNot ? 'rgba(0,0,0,0.3)' : 'rgba(255,255,255,0.3)',
            border: 'none',
            color: 'white',
            cursor: 'pointer',
            padding: '0 0.3rem',
            fontSize: '0.75rem',
            lineHeight: '1',
            borderRadius: '3px',
            fontWeight: 'bold'
          }}
          title={isNot ? 'Remove NOT' : 'Add NOT'}
        >
          ¬
        </button>
        <button
          onClick={removeHandler}
          style={{
            background: 'rgba(255,255,255,0.3)',
            border: 'none',
            color: 'white',
            cursor: 'pointer',
            padding: '0 0.25rem',
            fontSize: '1rem',
            lineHeight: '1',
            borderRadius: '3px',
            fontWeight: 'bold'
          }}
        >
          ×
        </button>
      </div>
    </React.Fragment>
  )
}
