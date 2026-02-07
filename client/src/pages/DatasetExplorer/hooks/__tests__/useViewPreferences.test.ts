import { describe, test, expect, beforeEach, vi } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useViewPreferences } from '../useViewPreferences'

describe('useViewPreferences', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorage.clear()
  })

  test('loads view preferences from localStorage', () => {
    localStorage.setItem('viewPrefs_test-ds', JSON.stringify({ 'geo.state': 'table' }))

    const { result } = renderHook(() => useViewPreferences({ identifier: 'test-ds' }))

    expect(result.current.getViewPreference('geo', 'state', 5)).toBe('table')
  })

  test('defaults to chart for <= 8 categories, table for > 8', () => {
    const { result } = renderHook(() => useViewPreferences({ identifier: 'test-ds' }))

    expect(result.current.getViewPreference('t', 'col', 5)).toBe('chart')
    expect(result.current.getViewPreference('t', 'col2', 12)).toBe('table')
  })

  test('toggleViewPreference switches and persists', () => {
    const { result } = renderHook(() => useViewPreferences({ identifier: 'test-ds' }))

    // Default for 5 categories is 'chart', so toggle should switch to 'table'
    act(() => {
      result.current.toggleViewPreference('geo', 'state')
    })

    // After toggle from undefined, should be 'table'
    expect(result.current.getViewPreference('geo', 'state', 5)).toBe('table')

    const stored = JSON.parse(localStorage.getItem('viewPrefs_test-ds') || '{}')
    expect(stored['geo.state']).toBe('table')
  })

  test('loads percentage label preference from localStorage', () => {
    localStorage.setItem('chartLabels_test-ds', 'percent')

    const { result } = renderHook(() => useViewPreferences({ identifier: 'test-ds' }))

    expect(result.current.showPercentageLabels).toBe(true)
  })

  test('defaults to count labels', () => {
    const { result } = renderHook(() => useViewPreferences({ identifier: 'test-ds' }))

    expect(result.current.showPercentageLabels).toBe(false)
  })

  test('survival view preference defaults to histogram', () => {
    const { result } = renderHook(() => useViewPreferences({ identifier: 'test-ds' }))

    expect(result.current.getSurvivalViewPreference('t', 'col')).toBe('histogram')
  })

  test('toggleSurvivalViewPreference switches and persists', () => {
    const { result } = renderHook(() => useViewPreferences({ identifier: 'test-ds' }))

    act(() => {
      result.current.toggleSurvivalViewPreference('t', 'col')
    })

    expect(result.current.getSurvivalViewPreference('t', 'col')).toBe('km')

    const stored = JSON.parse(localStorage.getItem('survivalPrefs_test-ds') || '{}')
    expect(stored['t.col']).toBe('km')
  })

  test('settings menu starts closed', () => {
    const { result } = renderHook(() => useViewPreferences({ identifier: 'test-ds' }))

    expect(result.current.showSettingsMenu).toBe(false)
  })

  test('settings menu closes on identifier change', () => {
    const { result, rerender } = renderHook(
      ({ identifier }) => useViewPreferences({ identifier }),
      { initialProps: { identifier: 'ds-1' } }
    )

    act(() => {
      result.current.setShowSettingsMenu(true)
    })
    expect(result.current.showSettingsMenu).toBe(true)

    rerender({ identifier: 'ds-2' })
    expect(result.current.showSettingsMenu).toBe(false)
  })
})
