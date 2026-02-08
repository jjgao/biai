import { unwrapNot, getFilterColumn, getFilterCountKey } from '../../../utils/filterHelpers'
import type { CategoryCount } from '../types'
import { useChartContext } from './ChartContext'

interface FilterMenuProps {
  tableName: string
  columnName: string
  categories?: CategoryCount[]
  cacheKeyOverride?: string
}

export function FilterMenu({ tableName, columnName, categories, cacheKeyOverride }: FilterMenuProps) {
  const {
    getEffectiveCacheKeyForChart,
    getAggregation,
    getMetricLabels,
    activeFilterMenu,
    filters,
    setFilters,
    hasColumnFilter,
    isValueFiltered,
    normalizeFilterValue,
    toggleFilter,
    clearColumnFilter,
  } = useChartContext()

  const cacheKey = cacheKeyOverride ?? getEffectiveCacheKeyForChart(tableName, columnName)
  const menuOpen =
    activeFilterMenu?.tableName === tableName &&
    activeFilterMenu.columnName === columnName &&
    activeFilterMenu.countKey === cacheKey
  if (!menuOpen) return null

  const aggregation = getAggregation(tableName, columnName, cacheKey)
  if (!aggregation || !categories || categories.length === 0) return null

  const metricLabels = getMetricLabels(aggregation)

  const columnHasFilter = hasColumnFilter(columnName, cacheKey)

  // Check if the current filter for this column has NOT wrapper
  const currentFilter = filters.find(f => {
    const actualF = unwrapNot(f)
    if (!actualF || getFilterColumn(actualF) !== columnName) return false
    return getFilterCountKey(f) === cacheKey
  })
  const isNot = !!currentFilter?.not

  // Toggle NOT for this column's filter
  const toggleColumnNot = () => {
    setFilters(prev => {
      const idx = prev.findIndex(f => {
        const actualF = unwrapNot(f)
        if (!actualF || getFilterColumn(actualF) !== columnName) return false
        return getFilterCountKey(f) === cacheKey
      })
      if (idx === -1) return prev

      const updated = [...prev]
      const filter = prev[idx]
      if (filter.not) {
        // Remove NOT wrapper
        updated[idx] = filter.not
      } else {
        // Add NOT wrapper
        updated[idx] = { not: filter }
      }
      return updated
    })
  }

  // Parent counting can double-book entities across wedges, so prefer bars
  if (aggregation.metric_type === 'parent') {
    return null
  }

  return (
    <div
      style={{
        position: 'absolute',
        top: '28px',
        right: 0,
        zIndex: 10,
        background: 'white',
        border: '1px solid #ddd',
        borderRadius: '6px',
        boxShadow: '0 4px 12px rgba(0,0,0,0.12)',
        padding: '0.5rem',
        maxHeight: '200px',
        overflowY: 'auto',
        minWidth: '140px',
        display: 'flex',
        flexDirection: 'column',
        gap: '0.25rem'
      }}
      onClick={(event) => event.stopPropagation()}
    >
      <div style={{ display: 'flex', gap: '0.25rem' }}>
        <button
          onClick={() => clearColumnFilter(tableName, columnName, cacheKey)}
          style={{
            border: 'none',
            background: columnHasFilter ? '#1976D2' : '#eee',
            color: columnHasFilter ? 'white' : '#555',
            borderRadius: '4px',
            padding: '0.25rem 0.5rem',
            fontSize: '0.7rem',
            cursor: columnHasFilter ? 'pointer' : 'default',
            opacity: columnHasFilter ? 1 : 0.6,
            flex: 1
          }}
          disabled={!columnHasFilter}
        >
          Reset
        </button>
        <button
          onClick={toggleColumnNot}
          style={{
            border: 'none',
            background: isNot ? '#333' : '#f0f0f0',
            color: isNot ? 'white' : '#555',
            borderRadius: '4px',
            padding: '0.25rem 0.5rem',
            fontSize: '0.7rem',
            cursor: columnHasFilter ? 'pointer' : 'default',
            opacity: columnHasFilter ? 1 : 0.6,
            fontWeight: isNot ? 'bold' : 'normal'
          }}
          disabled={!columnHasFilter}
          title={isNot ? 'Remove NOT' : 'Add NOT'}
        >
          ¬
        </button>
      </div>
      <div style={{ borderBottom: '1px solid #eee', margin: '0.25rem 0' }} />
      {categories.map(category => {
        const rawValue = normalizeFilterValue(category.value)
        const label = category.display_value ?? (category.value === '' ? '(Empty)' : String(category.value))
        const active = isValueFiltered(columnName, rawValue, cacheKey)

        return (
          <button
            key={`${tableName}-${columnName}-${label}`}
            onMouseDown={event => event.preventDefault()}
            onClick={() => toggleFilter(columnName, rawValue, tableName, cacheKey)}
            style={{
              border: active ? '1px solid #1976D2' : '1px solid #ccc',
              background: active ? '#E3F2FD' : '#fafafa',
              color: active ? '#0D47A1' : '#444',
              borderRadius: '999px',
              padding: '0.25rem 0.5rem',
              fontSize: '0.7rem',
              cursor: 'pointer',
              textAlign: 'left'
            }}
            title={`${label} (${category.count} ${metricLabels.short})`}
          >
            {label}
          </button>
        )
      })}
    </div>
  )
}
