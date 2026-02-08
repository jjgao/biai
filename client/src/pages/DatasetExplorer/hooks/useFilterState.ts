import { useState, useEffect, useRef } from 'react'
import {
  type Filter,
  ROW_COUNT_KEY,
  unwrapNot,
  rangeKey,
  rangesEqual,
  getFilterCountKey,
  filterContainsColumn,
  migrateFiltersToCurrentSchema,
} from '../../../utils/filterHelpers'
import {
  normalizeFilterValue,
  buildFiltersKey,
  deserializeFilters,
} from '../utils'

interface UseFilterStateArgs {
  identifier: string | undefined
}

export function useFilterState({ identifier }: UseFilterStateArgs) {
  const [filters, setFilters] = useState<Filter[]>([])
  const [activeFilterMenu, setActiveFilterMenu] = useState<{ tableName: string; columnName: string; countKey?: string } | null>(null)
  const [customRangeInputs, setCustomRangeInputs] = useState<Record<string, { min: string; max: string }>>({})
  const [rangeSelections, setRangeSelections] = useState<Record<string, Array<{ start: number; end: number }>>>({})

  const filtersInitialized = useRef(false)

  // Restore filters from URL hash / localStorage on mount
  useEffect(() => {
    if (filtersInitialized.current) return

    const hash = window.location.hash
    const match = hash.match(/filters=([^&]+)/)
    const encodedFilters = match ? match[1] : null

    if (encodedFilters) {
      const restored = deserializeFilters(encodedFilters)
      if (restored && restored.length > 0) {
        setFilters(migrateFiltersToCurrentSchema(restored))
        filtersInitialized.current = true
        return
      }
    }

    // Fallback to localStorage
    try {
      const stored = localStorage.getItem(`filters_${identifier}`)
      if (stored) {
        const parsed: Filter[] = JSON.parse(stored)
        if (parsed.length > 0) {
          setFilters(migrateFiltersToCurrentSchema(parsed))
        }
      }
    } catch (error) {
      console.error('Failed to load filters from localStorage:', error)
    }

    filtersInitialized.current = true
  }, [identifier])

  // ── Filter persistence helpers ────────────────────────────────────

  const saveFiltersToLocalStorage = (filterList: Filter[]) => {
    try {
      localStorage.setItem(`filters_${identifier}`, JSON.stringify(filterList))
    } catch (error) {
      console.error('Failed to save filters to localStorage:', error)
    }
  }

  // ── Filter manipulation functions ─────────────────────────────────

  const getFilterTableNameForCacheKey = (filter: Filter): string | undefined => filter.tableName

  const hasColumnFilter = (column: string, countKey?: string): boolean => {
    const resolvedKey = countKey ?? ROW_COUNT_KEY
    return filters.some(f => {
      const actual = unwrapNot(f)
      if (!actual || !filterContainsColumn(actual, column)) return false
      return getFilterCountKey(f) === resolvedKey
    })
  }

  const removeColumnFilters = (prev: Filter[], column: string, countKey?: string): Filter[] => {
    const resolvedKey = countKey ?? ROW_COUNT_KEY
    return prev.filter(filter => {
      const actualFilter = unwrapNot(filter)
      if (!actualFilter || !filterContainsColumn(actualFilter, column)) return true
      return getFilterCountKey(filter) !== resolvedKey
    })
  }

  const clearColumnFilter = (tableName: string, columnName: string, countKey?: string) => {
    setFilters(prev => removeColumnFilters(prev, columnName, countKey))
    const key = rangeKey(tableName, columnName, countKey)
    setCustomRangeInputs(prev => {
      if (!(key in prev)) return prev
      const { [key]: _removed, ...rest } = prev
      return rest
    })
    setRangeSelections(prev => {
      if (!(key in prev)) return prev
      const { [key]: _removed, ...rest } = prev
      return rest
    })
  }

  const updateColumnRanges = (
    tableName: string,
    columnName: string,
    updater: (ranges: Array<{ start: number; end: number }>) => Array<{ start: number; end: number }>,
    countKey?: string
  ) => {
    const key = rangeKey(tableName, columnName, countKey)
    let nextRanges: Array<{ start: number; end: number }> = []
    setRangeSelections(prev => {
      const prevRanges = prev[key] ?? []
      nextRanges = updater(prevRanges)
      nextRanges = nextRanges
        .slice()
        .sort((a, b) => (a.start - b.start) || (a.end - b.end))
      const unchanged = prevRanges.length === nextRanges.length && prevRanges.every((range, idx) => rangesEqual(range, nextRanges[idx]))
      if (unchanged) {
        nextRanges = prevRanges
        return prev
      }
      const updated = { ...prev }
      if (nextRanges.length === 0) {
        delete updated[key]
      } else {
        updated[key] = nextRanges
      }
      return updated
    })

    setFilters(prev => {
      const without = removeColumnFilters(prev, columnName, countKey)
      if (nextRanges.length === 0) return without
      if (nextRanges.length === 1) {
        const range = nextRanges[0]
        return [
          ...without,
          {
            column: columnName,
            operator: 'between',
            value: [range.start, range.end],
            tableName,
            countByKey: countKey
          }
        ]
      }
      const orFilters = nextRanges.map(range => ({ column: columnName, operator: 'between' as const, value: [range.start, range.end] }))
      return [
        ...without,
        { column: columnName, or: orFilters, tableName, countByKey: countKey }
      ]
    })
  }

  const toggleFilter = (column: string, value: string | number, tableName?: string, countByKey?: string) => {
    const filterValue = normalizeFilterValue(value)
    const resolvedCountKey = countByKey ?? ROW_COUNT_KEY

    setFilters(prevFilters => {
      const nextFilters = [...prevFilters]

      const existingIndex = nextFilters.findIndex(f => {
        const actualFilter = unwrapNot(f)
        if (!actualFilter || actualFilter.column !== column) return false
        return getFilterCountKey(f) === resolvedCountKey
      })

      const applyMetadata = (target: Filter, source?: Filter) => {
        if (tableName) {
          target.tableName = tableName
        } else if (source?.tableName) {
          target.tableName = source.tableName
        }
        target.countByKey = resolvedCountKey
      }

      if (existingIndex === -1) {
        const newFilter: Filter = { column, operator: 'eq', value: filterValue }
        applyMetadata(newFilter)
        nextFilters.push(newFilter)
        return nextFilters
      }

      const existingWrapped = nextFilters[existingIndex]
      const isNot = !!existingWrapped.not
      const existing = isNot && existingWrapped.not ? existingWrapped.not : existingWrapped

      if (existing.operator === 'eq') {
        const existingValue = normalizeFilterValue(existing.value as string | number)
        if (existingValue === filterValue) {
          nextFilters.splice(existingIndex, 1)
          return nextFilters
        }

        const updatedFilter: Filter = {
          column,
          operator: 'in',
          value: [existingValue, filterValue]
        }
        applyMetadata(updatedFilter, existing)
        nextFilters[existingIndex] = isNot ? { not: updatedFilter } : updatedFilter
        return nextFilters
      }

      if (existing.operator === 'in') {
        const values = Array.isArray(existing.value)
          ? existing.value.map(v => normalizeFilterValue(v as string | number))
          : []
        const matchIndex = values.findIndex(v => v === filterValue)

        if (matchIndex >= 0) {
          values.splice(matchIndex, 1)
        } else {
          values.push(filterValue)
        }

        if (values.length === 0) {
          nextFilters.splice(existingIndex, 1)
        } else if (values.length === 1) {
          const updatedFilter: Filter = { column, operator: 'eq', value: values[0] }
          applyMetadata(updatedFilter, existing)
          nextFilters[existingIndex] = isNot ? { not: updatedFilter } : updatedFilter
        } else {
          const updatedFilter: Filter = { column, operator: 'in', value: values }
          applyMetadata(updatedFilter, existing)
          nextFilters[existingIndex] = isNot ? { not: updatedFilter } : updatedFilter
        }

        return nextFilters
      }

      const updatedFilter: Filter = { column, operator: 'eq', value: filterValue }
      applyMetadata(updatedFilter, existing)
      nextFilters[existingIndex] = isNot ? { not: updatedFilter } : updatedFilter
      return nextFilters
    })
  }

  const clearFilters = () => {
    setFilters([])
    setCustomRangeInputs({})
    setRangeSelections({})
  }

  const isValueFiltered = (column: string, value: string | number, countByKey?: string): boolean => {
    const compareValue = normalizeFilterValue(value)
    const resolvedKey = countByKey ?? ROW_COUNT_KEY
    return filters.some(f => {
      const actualFilter = unwrapNot(f)
      if (!actualFilter || actualFilter.column !== column) return false
      if (getFilterCountKey(f) !== resolvedKey) return false
      if (actualFilter.operator === 'eq') {
        return normalizeFilterValue(actualFilter.value as string | number) === compareValue
      }
      if (actualFilter.operator === 'in' && Array.isArray(actualFilter.value)) {
        return actualFilter.value
          .map(v => normalizeFilterValue(v as string | number))
          .includes(compareValue)
      }
      return false
    })
  }

  const toggleRangeFilter = (tableName: string, column: string, binStart: number, binEnd: number, countKey?: string) => {
    const range = { start: binStart, end: binEnd }
    updateColumnRanges(tableName, column, prevRanges => {
      const existingIndex = prevRanges.findIndex(r => rangesEqual(r, range))
      if (existingIndex >= 0) {
        return [...prevRanges.slice(0, existingIndex), ...prevRanges.slice(existingIndex + 1)]
      }
      return [...prevRanges, range]
    }, countKey)
  }

  const isRangeFiltered = (tableName: string, column: string, binStart: number, binEnd: number, countKey?: string): boolean => {
    const key = rangeKey(tableName, column, countKey)
    const ranges = rangeSelections[key] ?? []
    return ranges.some(range => rangesEqual(range, { start: binStart, end: binEnd }))
  }

  const handleCustomRangeChange = (
    key: string,
    field: 'min' | 'max',
    value: string
  ) => {
    setCustomRangeInputs(prev => ({
      ...prev,
      [key]: {
        min: field === 'min' ? value : prev[key]?.min ?? '',
        max: field === 'max' ? value : prev[key]?.max ?? ''
      }
    }))
  }

  const applyCustomRange = (tableName: string, columnName: string, countKey?: string) => {
    const key = rangeKey(tableName, columnName, countKey)
    const range = customRangeInputs[key]
    if (!range) return

    const min = range.min.trim()
    const max = range.max.trim()
    if (min === '' || max === '') return

    const minValue = Number(min)
    const maxValue = Number(max)
    if (!Number.isFinite(minValue) || !Number.isFinite(maxValue) || minValue > maxValue) {
      return
    }

    setCustomRangeInputs(prev => ({
      ...prev,
      [key]: { min: String(minValue), max: String(maxValue) }
    }))

    updateColumnRanges(tableName, columnName, prevRanges => {
      const nextRange = { start: minValue, end: maxValue }
      const existingIndex = prevRanges.findIndex(r => rangesEqual(r, nextRange))
      if (existingIndex >= 0) return prevRanges
      return [...prevRanges, nextRange]
    }, countKey)
  }

  return {
    filters,
    setFilters,
    filtersInitialized,
    currentFiltersKey: buildFiltersKey(filters),
    activeFilterMenu,
    setActiveFilterMenu,
    customRangeInputs,
    setCustomRangeInputs,
    rangeSelections,
    setRangeSelections,
    toggleFilter,
    clearFilters,
    isValueFiltered,
    hasColumnFilter,
    removeColumnFilters,
    clearColumnFilter,
    updateColumnRanges,
    toggleRangeFilter,
    isRangeFiltered,
    handleCustomRangeChange,
    applyCustomRange,
    getFilterTableNameForCacheKey,
    saveFiltersToLocalStorage,
  }
}
