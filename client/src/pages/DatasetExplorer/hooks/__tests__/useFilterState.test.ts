import { describe, test, expect, beforeEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useFilterState } from '../useFilterState'

describe('useFilterState', () => {
  beforeEach(() => {
    localStorage.clear()
    window.location.hash = ''
  })

  test('initializes with empty filters', () => {
    const { result } = renderHook(() => useFilterState({ identifier: 'test-dataset' }))
    expect(result.current.filters).toEqual([])
    expect(result.current.activeFilterMenu).toBeNull()
    expect(result.current.customRangeInputs).toEqual({})
    expect(result.current.rangeSelections).toEqual({})
  })

  test('restores filters from localStorage', () => {
    const storedFilters = [{ column: 'age', operator: 'eq', value: '30', tableName: 'patients' }]
    localStorage.setItem('filters_test-dataset', JSON.stringify(storedFilters))

    const { result } = renderHook(() => useFilterState({ identifier: 'test-dataset' }))
    expect(result.current.filters).toHaveLength(1)
    expect(result.current.filters[0].column).toBe('age')
    expect(result.current.filters[0].operator).toBe('eq')
    expect(result.current.filters[0].value).toBe('30')
    expect(result.current.filters[0].tableName).toBe('patients')
  })

  describe('toggleFilter', () => {
    test('adds a new filter', () => {
      const { result } = renderHook(() => useFilterState({ identifier: 'test-dataset' }))

      act(() => {
        result.current.toggleFilter('age', '30', 'patients')
      })

      expect(result.current.filters).toHaveLength(1)
      expect(result.current.filters[0].column).toBe('age')
      expect(result.current.filters[0].operator).toBe('eq')
      expect(result.current.filters[0].value).toBe('30')
    })

    test('removes existing eq filter when toggled again', () => {
      const { result } = renderHook(() => useFilterState({ identifier: 'test-dataset' }))

      act(() => {
        result.current.toggleFilter('age', '30', 'patients')
      })
      expect(result.current.filters).toHaveLength(1)

      act(() => {
        result.current.toggleFilter('age', '30', 'patients')
      })
      expect(result.current.filters).toHaveLength(0)
    })

    test('upgrades eq to in when adding second value', () => {
      const { result } = renderHook(() => useFilterState({ identifier: 'test-dataset' }))

      act(() => {
        result.current.toggleFilter('age', '30', 'patients')
      })
      act(() => {
        result.current.toggleFilter('age', '40', 'patients')
      })

      expect(result.current.filters).toHaveLength(1)
      expect(result.current.filters[0].operator).toBe('in')
      expect(result.current.filters[0].value).toEqual(['30', '40'])
    })

    test('removes value from in filter', () => {
      const { result } = renderHook(() => useFilterState({ identifier: 'test-dataset' }))

      act(() => {
        result.current.toggleFilter('age', '30', 'patients')
      })
      act(() => {
        result.current.toggleFilter('age', '40', 'patients')
      })
      act(() => {
        result.current.toggleFilter('age', '30', 'patients')
      })

      expect(result.current.filters).toHaveLength(1)
      expect(result.current.filters[0].operator).toBe('eq')
      expect(result.current.filters[0].value).toBe('40')
    })
  })

  describe('clearFilters', () => {
    test('clears all filters and range state', () => {
      const { result } = renderHook(() => useFilterState({ identifier: 'test-dataset' }))

      act(() => {
        result.current.toggleFilter('age', '30', 'patients')
      })
      expect(result.current.filters).toHaveLength(1)

      act(() => {
        result.current.clearFilters()
      })

      expect(result.current.filters).toEqual([])
      expect(result.current.customRangeInputs).toEqual({})
      expect(result.current.rangeSelections).toEqual({})
    })
  })

  describe('isValueFiltered', () => {
    test('returns true for filtered value', () => {
      const { result } = renderHook(() => useFilterState({ identifier: 'test-dataset' }))

      act(() => {
        result.current.toggleFilter('age', '30', 'patients')
      })

      expect(result.current.isValueFiltered('age', '30')).toBe(true)
      expect(result.current.isValueFiltered('age', '40')).toBe(false)
    })

    test('returns true for value in in-filter', () => {
      const { result } = renderHook(() => useFilterState({ identifier: 'test-dataset' }))

      act(() => {
        result.current.toggleFilter('age', '30', 'patients')
      })
      act(() => {
        result.current.toggleFilter('age', '40', 'patients')
      })

      expect(result.current.isValueFiltered('age', '30')).toBe(true)
      expect(result.current.isValueFiltered('age', '40')).toBe(true)
      expect(result.current.isValueFiltered('age', '50')).toBe(false)
    })
  })

  describe('hasColumnFilter', () => {
    test('returns true when column has a filter', () => {
      const { result } = renderHook(() => useFilterState({ identifier: 'test-dataset' }))

      act(() => {
        result.current.toggleFilter('age', '30', 'patients')
      })

      expect(result.current.hasColumnFilter('age')).toBe(true)
      expect(result.current.hasColumnFilter('name')).toBe(false)
    })
  })

  describe('clearColumnFilter', () => {
    test('removes all filters for a specific column', () => {
      const { result } = renderHook(() => useFilterState({ identifier: 'test-dataset' }))

      act(() => {
        result.current.toggleFilter('age', '30', 'patients')
      })
      act(() => {
        result.current.toggleFilter('name', 'John', 'patients')
      })
      expect(result.current.filters).toHaveLength(2)

      act(() => {
        result.current.clearColumnFilter('patients', 'age')
      })

      expect(result.current.filters).toHaveLength(1)
      expect(result.current.filters[0].column).toBe('name')
    })
  })

  describe('handleCustomRangeChange', () => {
    test('updates min value', () => {
      const { result } = renderHook(() => useFilterState({ identifier: 'test-dataset' }))

      act(() => {
        result.current.handleCustomRangeChange('patients|age|rows', 'min', '10')
      })

      expect(result.current.customRangeInputs['patients|age|rows']).toEqual({
        min: '10',
        max: ''
      })
    })

    test('updates max value', () => {
      const { result } = renderHook(() => useFilterState({ identifier: 'test-dataset' }))

      act(() => {
        result.current.handleCustomRangeChange('patients|age|rows', 'max', '90')
      })

      expect(result.current.customRangeInputs['patients|age|rows']).toEqual({
        min: '',
        max: '90'
      })
    })
  })

  describe('saveFiltersToLocalStorage', () => {
    test('persists filters to localStorage', () => {
      const { result } = renderHook(() => useFilterState({ identifier: 'test-dataset' }))
      const filters = [{ column: 'age', operator: 'eq', value: '30' }]

      act(() => {
        result.current.saveFiltersToLocalStorage(filters as any)
      })

      expect(localStorage.getItem('filters_test-dataset')).toBe(JSON.stringify(filters))
    })
  })

  describe('currentFiltersKey', () => {
    test('changes when filters change', () => {
      const { result } = renderHook(() => useFilterState({ identifier: 'test-dataset' }))
      const initialKey = result.current.currentFiltersKey

      act(() => {
        result.current.toggleFilter('age', '30', 'patients')
      })

      expect(result.current.currentFiltersKey).not.toBe(initialKey)
    })
  })
})
