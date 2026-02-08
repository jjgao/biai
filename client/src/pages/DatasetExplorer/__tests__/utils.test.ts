import { describe, test, expect } from 'vitest'
import {
  chartKey,
  targetFromCacheKey,
  parseSelectionFromCacheKey,
  normalizeFilterValue,
  formatRangeValue,
  buildFiltersKey,
  metricsMatch,
  serializeFilters,
  deserializeFilters,
  serializeCountBySelections,
  deserializeCountBySelections,
  buildAncestorOptions,
  normalizeCountBySelections,
  getNiceBinWidth,
  getDisplayHistogram,
} from '../utils'
import type { ColumnAggregation, Table } from '../types'

describe('utils', () => {
  describe('chartKey', () => {
    test('combines table and column names', () => {
      expect(chartKey('patients', 'age')).toBe('patients.age')
    })
  })

  describe('targetFromCacheKey', () => {
    test('returns null for undefined', () => {
      expect(targetFromCacheKey(undefined)).toBeNull()
    })

    test('returns null for non-parent key', () => {
      expect(targetFromCacheKey('rows')).toBeNull()
    })

    test('extracts target from parent key', () => {
      expect(targetFromCacheKey('parent:patients')).toBe('patients')
    })
  })

  describe('parseSelectionFromCacheKey', () => {
    test('returns null for undefined', () => {
      expect(parseSelectionFromCacheKey(undefined)).toBeNull()
    })

    test('returns null for non-parent key', () => {
      expect(parseSelectionFromCacheKey('rows')).toBeNull()
    })

    test('parses parent key', () => {
      expect(parseSelectionFromCacheKey('parent:patients')).toEqual({
        mode: 'parent',
        targetTable: 'patients',
      })
    })
  })

  describe('normalizeFilterValue', () => {
    test('returns empty string for null', () => {
      expect(normalizeFilterValue(null)).toBe('')
    })

    test('returns empty string for undefined', () => {
      expect(normalizeFilterValue(undefined)).toBe('')
    })

    test('converts number to string', () => {
      expect(normalizeFilterValue(42)).toBe('42')
    })

    test('passes through strings', () => {
      expect(normalizeFilterValue('hello')).toBe('hello')
    })
  })

  describe('formatRangeValue', () => {
    test('formats integers without decimals', () => {
      expect(formatRangeValue(42)).toBe('42')
    })

    test('formats floats to 2 decimal places', () => {
      expect(formatRangeValue(3.14159)).toBe('3.14')
    })

    test('returns dash for non-finite values', () => {
      expect(formatRangeValue(Infinity)).toBe('–')
      expect(formatRangeValue(NaN)).toBe('–')
    })
  })

  describe('buildFiltersKey', () => {
    test('returns empty array JSON for undefined', () => {
      expect(buildFiltersKey()).toBe('[]')
    })

    test('serializes filters to JSON', () => {
      const filters = [{ column: 'age', operator: 'eq', value: '30' }]
      expect(buildFiltersKey(filters as any)).toBe(JSON.stringify(filters))
    })
  })

  describe('metricsMatch', () => {
    test('returns false if either is undefined', () => {
      expect(metricsMatch(undefined, {} as any)).toBe(false)
      expect(metricsMatch({} as any, undefined)).toBe(false)
    })

    test('matches two row-type aggregations', () => {
      const a = { metric_type: 'rows' } as ColumnAggregation
      const b = {} as ColumnAggregation // defaults to 'rows'
      expect(metricsMatch(a, b)).toBe(true)
    })

    test('does not match different metric types', () => {
      const a = { metric_type: 'rows' } as ColumnAggregation
      const b = { metric_type: 'parent', metric_parent_table: 'x' } as ColumnAggregation
      expect(metricsMatch(a, b)).toBe(false)
    })

    test('matches parent aggregations with same table and path', () => {
      const path = [{ from_table: 'visits', via_column: 'patient_id', to_table: 'patients' }]
      const a = { metric_type: 'parent', metric_parent_table: 'patients', metric_path: path } as ColumnAggregation
      const b = { metric_type: 'parent', metric_parent_table: 'patients', metric_path: path } as ColumnAggregation
      expect(metricsMatch(a, b)).toBe(true)
    })

    test('does not match parent aggregations with different parent tables', () => {
      const a = { metric_type: 'parent', metric_parent_table: 'patients' } as ColumnAggregation
      const b = { metric_type: 'parent', metric_parent_table: 'visits' } as ColumnAggregation
      expect(metricsMatch(a, b)).toBe(false)
    })
  })

  describe('serializeFilters / deserializeFilters', () => {
    test('round-trips filters', () => {
      const filters = [{ column: 'age', operator: 'eq', value: '30' }]
      const encoded = serializeFilters(filters as any)
      const decoded = deserializeFilters(encoded)
      expect(decoded).toEqual(filters)
    })
  })

  describe('serializeCountBySelections / deserializeCountBySelections', () => {
    test('round-trips selections', () => {
      const selections = { patients: { mode: 'parent' as const, targetTable: 'visits' } }
      const encoded = serializeCountBySelections(selections)
      const decoded = deserializeCountBySelections(encoded)
      expect(decoded).toEqual(selections)
    })
  })

  describe('buildAncestorOptions', () => {
    test('returns empty for tables with no relationships', () => {
      const tables: Table[] = [
        { id: '1', name: 'patients', rowCount: 100, relationships: [] }
      ] as any
      const result = buildAncestorOptions(tables)
      expect(result.patients).toEqual([])
    })

    test('finds direct parent relationships', () => {
      const tables: Table[] = [
        {
          id: '1', name: 'visits', rowCount: 200,
          relationships: [{ foreign_key: 'patient_id', referenced_table: 'patients' }]
        },
        {
          id: '2', name: 'patients', rowCount: 100,
          relationships: []
        }
      ] as any
      const result = buildAncestorOptions(tables)
      expect(result.visits).toHaveLength(1)
      expect(result.visits[0].targetTable).toBe('patients')
      expect(result.visits[0].key).toBe('parent:patients')
    })

    test('returns empty for the root table with no parents', () => {
      const tables: Table[] = [
        {
          id: '1', name: 'visits', rowCount: 200,
          relationships: [{ foreign_key: 'patient_id', referenced_table: 'patients' }]
        },
        {
          id: '2', name: 'patients', rowCount: 100,
          relationships: []
        }
      ] as any
      const result = buildAncestorOptions(tables)
      expect(result.patients).toEqual([])
    })
  })

  describe('normalizeCountBySelections', () => {
    test('keeps valid selections', () => {
      const selections = { visits: { mode: 'parent' as const, targetTable: 'patients' } }
      const options = { visits: [{ targetTable: 'patients', label: 'Patients', key: 'parent:patients', path: [] }] }
      expect(normalizeCountBySelections(selections, options)).toBe(selections)
    })

    test('removes invalid selections', () => {
      const selections = { visits: { mode: 'parent' as const, targetTable: 'nonexistent' } }
      const options = { visits: [{ targetTable: 'patients', label: 'Patients', key: 'parent:patients', path: [] }] }
      const result = normalizeCountBySelections(selections, options)
      expect(result).toEqual({})
    })

    test('removes selections for tables with no options', () => {
      const selections = { visits: { mode: 'parent' as const, targetTable: 'patients' } }
      const options = { visits: [] }
      const result = normalizeCountBySelections(selections, options as any)
      expect(result).toEqual({})
    })
  })

  describe('getNiceBinWidth', () => {
    test('returns 1 for zero range', () => {
      expect(getNiceBinWidth(0, 10)).toBe(1)
    })

    test('returns 1 for negative range', () => {
      expect(getNiceBinWidth(-5, 10)).toBe(1)
    })

    test('returns nice bin width for typical range', () => {
      const width = getNiceBinWidth(100, 10)
      expect([1, 2, 5, 10, 20, 50, 100]).toContain(width)
    })

    test('returns nice bin width for large range', () => {
      const width = getNiceBinWidth(10000, 20)
      expect(width).toBeGreaterThan(0)
      expect(Number.isFinite(width)).toBe(true)
    })
  })

  describe('getDisplayHistogram', () => {
    test('returns empty array for empty histogram', () => {
      expect(getDisplayHistogram([], undefined)).toEqual([])
      expect(getDisplayHistogram(undefined, undefined)).toEqual([])
    })

    test('returns original histogram when stats are missing', () => {
      const histogram = [{ bin_start: 0, bin_end: 10, count: 5, percentage: 100 }]
      expect(getDisplayHistogram(histogram, undefined)).toBe(histogram)
    })

    test('returns original histogram when range is zero', () => {
      const histogram = [{ bin_start: 5, bin_end: 5, count: 10, percentage: 100 }]
      const stats = { min: 5, max: 5, mean: 5, stddev: 0, median: 5, q25: 5, q75: 5 }
      expect(getDisplayHistogram(histogram, stats)).toBe(histogram)
    })

    test('rebins histogram with nice bin widths', () => {
      const histogram = [
        { bin_start: 0, bin_end: 3, count: 10, percentage: 33.3 },
        { bin_start: 3, bin_end: 7, count: 15, percentage: 50 },
        { bin_start: 7, bin_end: 10, count: 5, percentage: 16.7 },
      ]
      const stats = { min: 0, max: 10, mean: 5, stddev: 3, median: 5, q25: 2, q75: 8 }
      const result = getDisplayHistogram(histogram, stats)
      expect(result.length).toBeGreaterThan(0)
      // All bins should have count > 0 (filtered)
      result.forEach(bin => expect(bin.count).toBeGreaterThan(0))
      // Percentages should sum to approximately 100
      const totalPercentage = result.reduce((sum, bin) => sum + bin.percentage, 0)
      expect(totalPercentage).toBeCloseTo(100, 0)
    })
  })
})
