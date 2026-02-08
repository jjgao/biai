import { describe, test, expect, beforeEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useCountBy } from '../useCountBy'

describe('useCountBy', () => {
  beforeEach(() => {
    localStorage.clear()
    window.location.hash = ''
  })

  test('initializes with empty state', () => {
    const { result } = renderHook(() => useCountBy({ identifier: 'test-dataset' }))
    expect(result.current.countBySelections).toEqual({})
    expect(result.current.chartCountOverrides).toEqual({})
    expect(result.current.activeCountMenuKey).toBeNull()
    expect(result.current.countByReady).toBe(true) // becomes true after init
  })

  describe('getCountByCacheKey', () => {
    test('returns override when provided', () => {
      const { result } = renderHook(() => useCountBy({ identifier: 'test-dataset' }))
      expect(result.current.getCountByCacheKey('patients', 'parent:visits')).toBe('parent:visits')
    })

    test('returns rows key when no selection', () => {
      const { result } = renderHook(() => useCountBy({ identifier: 'test-dataset' }))
      expect(result.current.getCountByCacheKey('patients')).toBe('rows')
    })

    test('returns parent key when selection exists', () => {
      const { result } = renderHook(() => useCountBy({ identifier: 'test-dataset' }))

      act(() => {
        result.current.handleCountByChange('patients', 'parent:visits')
      })

      expect(result.current.getCountByCacheKey('patients')).toBe('parent:visits')
    })
  })

  describe('handleCountByChange', () => {
    test('sets parent selection', () => {
      const { result } = renderHook(() => useCountBy({ identifier: 'test-dataset' }))

      act(() => {
        result.current.handleCountByChange('patients', 'parent:visits')
      })

      expect(result.current.countBySelections.patients).toEqual({
        mode: 'parent',
        targetTable: 'visits'
      })
    })

    test('clears selection on rows key', () => {
      const { result } = renderHook(() => useCountBy({ identifier: 'test-dataset' }))

      act(() => {
        result.current.handleCountByChange('patients', 'parent:visits')
      })
      expect(result.current.countBySelections.patients).toBeDefined()

      act(() => {
        result.current.handleCountByChange('patients', 'rows')
      })
      expect(result.current.countBySelections.patients).toBeUndefined()
    })
  })

  describe('getEffectiveCacheKeyForChart', () => {
    test('returns table default when no chart override', () => {
      const { result } = renderHook(() => useCountBy({ identifier: 'test-dataset' }))
      expect(result.current.getEffectiveCacheKeyForChart('patients', 'age')).toBe('rows')
    })

    test('returns chart override when set', () => {
      const { result } = renderHook(() => useCountBy({ identifier: 'test-dataset' }))

      act(() => {
        result.current.setChartOverrideForChart('patients', 'age', 'parent:visits')
      })

      expect(result.current.getEffectiveCacheKeyForChart('patients', 'age')).toBe('parent:visits')
    })
  })

  describe('setChartOverrideForChart', () => {
    test('sets override', () => {
      const { result } = renderHook(() => useCountBy({ identifier: 'test-dataset' }))

      act(() => {
        result.current.setChartOverrideForChart('patients', 'age', 'parent:visits')
      })

      expect(result.current.chartCountOverrides['patients.age']).toBe('parent:visits')
    })

    test('clears override when undefined', () => {
      const { result } = renderHook(() => useCountBy({ identifier: 'test-dataset' }))

      act(() => {
        result.current.setChartOverrideForChart('patients', 'age', 'parent:visits')
      })
      expect(result.current.chartCountOverrides['patients.age']).toBe('parent:visits')

      act(() => {
        result.current.setChartOverrideForChart('patients', 'age')
      })
      expect(result.current.chartCountOverrides['patients.age']).toBeUndefined()
    })

    test('clears override when value matches default', () => {
      const { result } = renderHook(() => useCountBy({ identifier: 'test-dataset' }))

      act(() => {
        result.current.setChartOverrideForChart('patients', 'age', 'parent:visits')
      })

      act(() => {
        result.current.setChartOverrideForChart('patients', 'age', 'rows')
      })
      expect(result.current.chartCountOverrides['patients.age']).toBeUndefined()
    })
  })

  describe('getCountByValueForTable', () => {
    test('returns same as getCountByCacheKey', () => {
      const { result } = renderHook(() => useCountBy({ identifier: 'test-dataset' }))
      expect(result.current.getCountByValueForTable('patients')).toBe(
        result.current.getCountByCacheKey('patients')
      )
    })
  })

  describe('chart overrides persistence', () => {
    test('loads overrides from localStorage on init', () => {
      localStorage.setItem('chartOverrides_test-dataset', JSON.stringify({ 'patients.age': 'parent:visits' }))

      const { result } = renderHook(() => useCountBy({ identifier: 'test-dataset' }))
      expect(result.current.chartCountOverrides['patients.age']).toBe('parent:visits')
    })
  })

  describe('identifier change', () => {
    test('resets state when identifier changes', () => {
      const { result, rerender } = renderHook(
        ({ identifier }) => useCountBy({ identifier }),
        { initialProps: { identifier: 'dataset-1' } }
      )

      act(() => {
        result.current.handleCountByChange('patients', 'parent:visits')
      })
      expect(result.current.countBySelections.patients).toBeDefined()

      rerender({ identifier: 'dataset-2' })

      expect(result.current.countBySelections).toEqual({})
    })
  })
})
