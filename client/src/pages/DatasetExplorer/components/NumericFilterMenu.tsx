import { unwrapNot, getFilterColumn, getFilterCountKey, rangeKey, rangesEqual } from '../../../utils/filterHelpers'
import type { HistogramBin, NumericStats } from '../types'
import { useChartContext } from './ChartContext'

interface NumericFilterMenuProps {
  tableName: string
  columnName: string
  histogram?: HistogramBin[]
  stats?: NumericStats
  cacheKeyOverride?: string
}

export function NumericFilterMenu({ tableName, columnName, histogram, stats, cacheKeyOverride }: NumericFilterMenuProps) {
  const {
    getEffectiveCacheKeyForChart,
    getAggregation,
    getMetricLabels,
    activeFilterMenu,
    filters,
    setFilters,
    hasColumnFilter,
    isValueFiltered,
    toggleFilter,
    clearColumnFilter,
    rangeSelections,
    customRangeInputs,
    toggleRangeFilter,
    isRangeFiltered,
    handleCustomRangeChange,
    applyCustomRange,
    updateColumnRanges,
    formatRangeValue,
  } = useChartContext()

  const cacheKey = cacheKeyOverride ?? getEffectiveCacheKeyForChart(tableName, columnName)
  const menuOpen =
    activeFilterMenu?.tableName === tableName &&
    activeFilterMenu.columnName === columnName &&
    activeFilterMenu.countKey === cacheKey
  if (!menuOpen) return null

  const bins = histogram ?? []
  const aggregation = getAggregation(tableName, columnName, cacheKey)
  if (!aggregation) return null

  const metricLabels = getMetricLabels(aggregation)
  const key = rangeKey(tableName, columnName, cacheKey)
  const range = customRangeInputs[key] || { min: stats && stats.min !== null ? String(stats.min) : '', max: stats && stats.max !== null ? String(stats.max) : '' }
  const columnHasFilter = hasColumnFilter(columnName, cacheKey)
  const selectedRanges = rangeSelections[key] ?? []
  const customRanges = selectedRanges.filter(r => !bins.some(bin => rangesEqual(r, { start: bin.bin_start, end: bin.bin_end })))
  const minDisplay = stats && stats.min !== null ? formatRangeValue(stats.min) : '–'
  const maxDisplay = stats && stats.max !== null ? formatRangeValue(stats.max) : '–'
  const medianDisplay = stats && stats.median !== null ? formatRangeValue(stats.median) : '–'
  const stdDisplay = stats && stats.stddev !== undefined && stats.stddev !== null ? stats.stddev.toFixed(2) : '–'

  const minValue = Number(range.min)
  const maxValue = Number(range.max)
  const hasValidRange =
    range.min.trim() !== '' &&
    range.max.trim() !== '' &&
    Number.isFinite(minValue) &&
    Number.isFinite(maxValue) &&
    minValue <= maxValue

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
        updated[idx] = filter.not
      } else {
        updated[idx] = { not: filter }
      }
      return updated
    })
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
        maxHeight: '260px',
        overflowY: 'auto',
        minWidth: '180px',
        display: 'flex',
        flexDirection: 'column',
        gap: '0.35rem'
      }}
      onClick={(event) => event.stopPropagation()}
    >
      {stats && (
        <>
          <div style={{ fontSize: '0.7rem', color: '#555', display: 'flex', justifyContent: 'space-between', gap: '0.5rem' }}>
            <span>Min: {minDisplay}</span>
            <span>Max: {maxDisplay}</span>
          </div>
          <div style={{ fontSize: '0.7rem', color: '#555', display: 'flex', justifyContent: 'space-between', gap: '0.5rem' }}>
            <span>Median: {medianDisplay}</span>
            <span>Std: {stdDisplay}</span>
          </div>
        </>
      )}
      {(() => {
        const nullAgg = getAggregation(tableName, columnName, cacheKey)
        const nullMetricLabels = getMetricLabels(nullAgg)
        const nullCount = nullAgg?.null_count ?? 0
        if (nullCount === 0) return null

        const nullActive = isValueFiltered(columnName, '', cacheKey)
        return (
          <>
            <div style={{ borderBottom: '1px solid #eee', margin: '0.25rem 0' }} />
            <button
              onMouseDown={event => event.preventDefault()}
              onClick={() => toggleFilter(columnName, '', tableName, cacheKey)}
              style={{
                border: nullActive ? '1px solid #1976D2' : '1px solid #ccc',
                background: nullActive ? '#E3F2FD' : '#fafafa',
                color: nullActive ? '#0D47A1' : '#444',
                borderRadius: '999px',
                padding: '0.25rem 0.5rem',
                fontSize: '0.7rem',
                cursor: 'pointer',
                textAlign: 'left'
              }}
              title={`Null values (${nullCount} ${nullMetricLabels.short})`}
            >
              (Null) — {nullCount} {nullMetricLabels.short}
            </button>
          </>
        )
      })()}
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
      {bins.length > 0 && (
        <div style={{ borderTop: '1px solid #eee', paddingTop: '0.25rem', display: 'flex', flexWrap: 'wrap', gap: '0.25rem' }}>
          {bins.map((bin, index) => {
            const active = isRangeFiltered(tableName, columnName, bin.bin_start, bin.bin_end, cacheKey)
            const label = `${formatRangeValue(bin.bin_start)} – ${formatRangeValue(bin.bin_end)}`
            return (
              <button
                key={`${tableName}-${columnName}-bin-${index}`}
                onMouseDown={event => event.preventDefault()}
                onClick={() => toggleRangeFilter(tableName, columnName, bin.bin_start, bin.bin_end, cacheKey)}
                style={{
                  border: active ? '1px solid #1976D2' : '1px solid #ccc',
                  background: active ? '#E3F2FD' : '#fafafa',
                  color: active ? '#0D47A1' : '#444',
                  borderRadius: '999px',
                  padding: '0.25rem 0.5rem',
                  fontSize: '0.7rem',
                  cursor: 'pointer'
                }}
                title={`${label} (${bin.count} ${metricLabels.short})`}
              >
                {label}
              </button>
            )
          })}
        </div>
      )}
      {customRanges.length > 0 && (
        <div style={{ borderTop: '1px solid #eee', paddingTop: '0.25rem', display: 'flex', flexWrap: 'wrap', gap: '0.25rem' }}>
          {customRanges.map((customRange, index) => {
            const label = `${formatRangeValue(customRange.start)} – ${formatRangeValue(customRange.end)}`
            return (
              <button
                key={`${tableName}-${columnName}-custom-${index}`}
                onMouseDown={event => event.preventDefault()}
                onClick={() => updateColumnRanges(tableName, columnName, prev => prev.filter(r => !rangesEqual(r, customRange)), cacheKey)}
                style={{
                  border: '1px solid #1976D2',
                  background: '#E3F2FD',
                  color: '#0D47A1',
                  borderRadius: '999px',
                  padding: '0.25rem 0.5rem',
                  fontSize: '0.7rem',
                  cursor: 'pointer'
                }}
                title={`Remove ${label}`}
              >
                {label} ×
              </button>
            )
          })}
        </div>
      )}
      <div style={{ borderTop: '1px solid #eee', paddingTop: '0.35rem', display: 'flex', flexDirection: 'column', gap: '0.35rem' }}>
        <div style={{ display: 'flex', gap: '0.35rem' }}>
          <label style={{ fontSize: '0.7rem', color: '#555', flex: 1 }}>
            From
            <input
              type="number"
              value={range.min}
              onChange={(event) => handleCustomRangeChange(key, 'min', event.target.value)}
              placeholder={stats?.min !== null && stats?.min !== undefined ? String(stats.min) : ''}
              style={{ width: '100%', padding: '0.2rem 0.3rem', marginTop: '0.15rem' }}
            />
          </label>
          <label style={{ fontSize: '0.7rem', color: '#555', flex: 1 }}>
            To
            <input
              type="number"
              value={range.max}
              onChange={(event) => handleCustomRangeChange(key, 'max', event.target.value)}
              placeholder={stats?.max !== null && stats?.max !== undefined ? String(stats.max) : ''}
              style={{ width: '100%', padding: '0.2rem 0.3rem', marginTop: '0.15rem' }}
            />
          </label>
        </div>
        <button
          onClick={() => applyCustomRange(tableName, columnName, cacheKey)}
          style={{
            border: 'none',
            background: hasValidRange ? '#1976D2' : '#ccc',
            color: 'white',
            borderRadius: '4px',
            padding: '0.3rem 0.5rem',
            fontSize: '0.75rem',
            cursor: hasValidRange ? 'pointer' : 'default'
          }}
          disabled={!hasValidRange}
        >
          Apply
        </button>
      </div>
    </div>
  )
}
