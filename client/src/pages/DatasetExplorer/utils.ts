import type { MetricPathSegment } from '../../types'
import type { Filter } from '../../utils/filterHelpers'
import { encodeState, decodeState } from '../../utils/urlHelpers'
import {
  MAX_ANCESTOR_DEPTH,
  type ColumnAggregation,
  type Table,
  type AncestorOption,
  type HistogramBin,
  type NumericStats,
} from './types'
import type { CountBySelection } from '../../utils/presetHelpers'

// ── Pure utility functions (no React state dependencies) ────────────

export const chartKey = (tableName: string, columnName: string) => `${tableName}.${columnName}`

export const targetFromCacheKey = (key?: string): string | null => {
  if (!key) return null
  return key.startsWith('parent:') ? key.slice('parent:'.length) : null
}

export const parseSelectionFromCacheKey = (key?: string): CountBySelection | null => {
  if (!key) return null
  if (key.startsWith('parent:')) {
    return { mode: 'parent', targetTable: key.slice('parent:'.length) }
  }
  return null
}

export const normalizeFilterValue = (value: string | number | null | undefined): string => {
  if (value === null || value === undefined) return ''
  return String(value)
}

export const formatRangeValue = (value: number): string => {
  if (!Number.isFinite(value)) return '–'
  if (Number.isInteger(value)) return value.toString()
  return value.toFixed(2)
}

export const buildFiltersKey = (list?: Filter[]): string => JSON.stringify(list ?? [])

export const metricsMatch = (a?: ColumnAggregation, b?: ColumnAggregation) => {
  if (!a || !b) return false
  const typeA = a.metric_type || 'rows'
  const typeB = b.metric_type || 'rows'
  if (typeA !== typeB) return false
  if (typeA === 'parent') {
    const pathA = JSON.stringify(a.metric_path || [])
    const pathB = JSON.stringify(b.metric_path || [])
    return a.metric_parent_table === b.metric_parent_table && pathA === pathB
  }
  return true
}

// ── Serialization helpers ───────────────────────────────────────────

export const serializeFilters = (filters: Filter[]): string => encodeState(filters)

export const deserializeFilters = (encoded: string): Filter[] | null => decodeState<Filter[]>(encoded)

export const serializeCountBySelections = (selections: Record<string, CountBySelection>): string =>
  encodeState(selections)

export const deserializeCountBySelections = (encoded: string): Record<string, CountBySelection> | null =>
  decodeState<Record<string, CountBySelection>>(encoded)

// ── Ancestor options (graph traversal) ──────────────────────────────

export const buildAncestorOptions = (tables: Table[]): Record<string, AncestorOption[]> => {
  const tableMap = new Map(tables.map(t => [t.name, t]))
  const options: Record<string, AncestorOption[]> = {}

  tables.forEach(source => {
    const result: AncestorOption[] = []
    const queue: Array<{ tableName: string; path: MetricPathSegment[] }> = [{ tableName: source.name, path: [] }]
    const visited = new Set<string>([source.name])

    while (queue.length > 0) {
      const { tableName, path } = queue.shift()!
      const tableMeta = tableMap.get(tableName)
      if (!tableMeta) continue

      for (const rel of tableMeta.relationships || []) {
        const nextTable = rel.referenced_table
        const segment: MetricPathSegment = { from_table: tableName, via_column: rel.foreign_key, to_table: nextTable }
        const nextPath = [...path, segment]

        if (nextPath.length > MAX_ANCESTOR_DEPTH) {
          continue
        }

        if (!visited.has(nextTable)) {
          queue.push({ tableName: nextTable, path: nextPath })
          visited.add(nextTable)
        }

        if (nextPath.length > 0) {
          const targetMeta = tableMap.get(nextTable)
          const labelParts = nextPath.map(seg => `${seg.from_table}.${seg.via_column}`)
          const label = `${targetMeta?.displayName || targetMeta?.name || nextTable} via ${labelParts.join(' → ')}`
          const key = `parent:${nextTable}`
          result.push({ targetTable: nextTable, label, key, path: nextPath })
        }
      }
    }

    const unique = new Map<string, AncestorOption>()
    result.forEach(option => {
      if (!unique.has(option.targetTable)) {
        unique.set(option.targetTable, option)
      }
    })
    options[source.name] = Array.from(unique.values()).sort((a, b) => a.label.localeCompare(b.label))
  })

  return options
}

export const normalizeCountBySelections = (
  selections: Record<string, CountBySelection>,
  options: Record<string, AncestorOption[]>
): Record<string, CountBySelection> => {
  let changed = false
  const next: Record<string, CountBySelection> = {}

  Object.entries(selections).forEach(([table, selection]) => {
    if (!selection) return
    const tableOptions = options[table]
    if (!tableOptions || tableOptions.length === 0) {
      changed = true
      return
    }
    const match = tableOptions.find(opt => opt.targetTable === selection.targetTable)
    if (!match) {
      changed = true
      return
    }
    next[table] = selection
  })

  return changed ? next : selections
}

// ── Histogram binning ───────────────────────────────────────────────

export const getNiceBinWidth = (range: number, desiredBins: number): number => {
  if (!Number.isFinite(range) || range <= 0) {
    return 1
  }

  const target = range / Math.max(desiredBins, 1)
  if (!Number.isFinite(target) || target <= 0) {
    return range
  }

  const exponent = Math.floor(Math.log10(target))
  const scaled = target / Math.pow(10, exponent)

  let niceScaled: number
  if (scaled <= 1) {
    niceScaled = 1
  } else if (scaled <= 2) {
    niceScaled = 2
  } else if (scaled <= 5) {
    niceScaled = 5
  } else {
    niceScaled = 10
  }

  return niceScaled * Math.pow(10, exponent)
}

export const getDisplayHistogram = (
  histogram: HistogramBin[] | undefined,
  stats: NumericStats | undefined
): HistogramBin[] => {
  if (!histogram || histogram.length === 0) return []
  if (!stats || stats.min === null || stats.max === null) return histogram

  const min = stats.min
  const max = stats.max
  if (!Number.isFinite(min) || !Number.isFinite(max) || max <= min) return histogram

  const originalTotal = histogram.reduce((sum, bin) => sum + bin.count, 0)
  if (!Number.isFinite(originalTotal) || originalTotal === 0) return histogram

  const range = max - min
  const desiredBins = Math.min(Math.max(histogram.length, 1), 60)
  let width = getNiceBinWidth(range, desiredBins)
  if (!Number.isFinite(width) || width <= 0) {
    width = range || 1
  }

  let guard = 0
  while (range / width > 60 && guard < 10) {
    const nextApprox = Math.ceil(range / width / 2)
    width = getNiceBinWidth(range, Math.max(nextApprox, 1))
    if (!Number.isFinite(width) || width <= 0) {
      width = range || 1
      break
    }
    guard += 1
  }

  const start = Math.floor(min / width) * width
  const bucketCount = Math.max(1, Math.ceil((max - start) / width) + 1)
  const buckets: HistogramBin[] = []
  for (let i = 0; i < bucketCount; i++) {
    buckets.push({
      bin_start: start + i * width,
      bin_end: start + (i + 1) * width,
      count: 0,
      percentage: 0
    })
  }

  histogram.forEach(bin => {
    const center = (bin.bin_start + bin.bin_end) / 2
    let index = Math.floor((center - start) / width)
    if (index < 0) index = 0
    if (index >= buckets.length) index = buckets.length - 1
    buckets[index].count += bin.count
  })

  const rebinnedTotal = buckets.reduce((sum, bucket) => sum + bucket.count, 0)
  const denominator = rebinnedTotal > 0 ? rebinnedTotal : originalTotal
  buckets.forEach(bucket => {
    bucket.percentage = denominator > 0 ? (bucket.count / denominator) * 100 : 0
  })

  const filtered = buckets.filter(bucket => bucket.count > 0)
  return filtered.length > 0 ? filtered : histogram
}
