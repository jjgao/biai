import { describe, test, expect, beforeEach, vi } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useDashboard } from '../useDashboard'

// Mock the API module
vi.mock('../../../../services/api', () => ({
  default: {
    get: vi.fn(),
    post: vi.fn(),
    delete: vi.fn(),
  },
}))

import api from '../../../../services/api'

const mockedGet = vi.mocked(api.get)
const mockedPost = vi.mocked(api.post as any)
const mockedDelete = vi.mocked(api.delete as any)

describe('useDashboard', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorage.clear()
    // Default: API returns empty dashboards
    mockedGet.mockImplementation(() => Promise.resolve({ data: { dashboards: [] } }) as any)
    mockedPost.mockImplementation(() => Promise.resolve({ data: {} }))
    mockedDelete.mockImplementation(() => Promise.resolve({ data: {} }))
  })

  test('initializes with empty dashboard charts', () => {
    const { result } = renderHook(() => useDashboard({ identifier: 'test-ds' }))

    expect(result.current.dashboardCharts).toEqual([])
  })

  test('setDashboardCharts updates chart list', () => {
    const { result } = renderHook(() => useDashboard({ identifier: 'test-ds' }))

    const chart = { tableName: 'geo', columnName: 'state', countByTarget: null, addedAt: new Date().toISOString() }
    act(() => {
      result.current.setDashboardCharts([chart])
    })

    expect(result.current.dashboardCharts).toHaveLength(1)
    expect(result.current.dashboardCharts[0].tableName).toBe('geo')
  })

  test('getDashboardChartKey returns correct key format', () => {
    const { result } = renderHook(() => useDashboard({ identifier: 'test-ds' }))

    const key = result.current.getDashboardChartKey({
      tableName: 'geo',
      columnName: 'state',
      countByTarget: null,
    })
    expect(key).toBe('geo:state:rows')

    const keyWithTarget = result.current.getDashboardChartKey({
      tableName: 'geo',
      columnName: 'state',
      countByTarget: 'patients',
    })
    expect(keyWithTarget).toBe('geo:state:patients')
  })

  test('saveDashboard calls API and updates state', async () => {
    const { result } = renderHook(() => useDashboard({ identifier: 'test-ds' }))

    // Add a chart first
    act(() => {
      result.current.setDashboardCharts([{
        tableName: 'geo',
        columnName: 'state',
        countByTarget: null,
        addedAt: new Date().toISOString(),
      }])
    })

    await act(async () => {
      await result.current.saveDashboard('My Dashboard')
    })

    expect(mockedPost).toHaveBeenCalledWith(
      '/datasets/test-ds/dashboards',
      expect.objectContaining({
        dashboard_name: 'My Dashboard',
        is_most_recent: false,
      })
    )
    expect(result.current.savedDashboards).toHaveLength(1)
    expect(result.current.savedDashboards[0].name).toBe('My Dashboard')
    expect(result.current.showSaveDashboardDialog).toBe(false)
  })

  test('deleteDashboard calls API', async () => {
    const { result } = renderHook(() => useDashboard({ identifier: 'test-ds' }))

    // Wait for initial effects
    await act(async () => {
      await new Promise(resolve => setTimeout(resolve, 100))
    })

    await act(async () => {
      await result.current.deleteDashboard('d1')
    })

    expect(mockedDelete).toHaveBeenCalledWith('/datasets/test-ds/dashboards/d1')
  })

  test('dialog states initialize as closed', () => {
    const { result } = renderHook(() => useDashboard({ identifier: 'test-ds' }))

    expect(result.current.showSaveDashboardDialog).toBe(false)
    expect(result.current.showLoadDashboardDialog).toBe(false)
    expect(result.current.showManageDashboardsDialog).toBe(false)
  })

  test('dialog state setters work', () => {
    const { result } = renderHook(() => useDashboard({ identifier: 'test-ds' }))

    act(() => {
      result.current.setShowSaveDashboardDialog(true)
    })
    expect(result.current.showSaveDashboardDialog).toBe(true)

    act(() => {
      result.current.setShowLoadDashboardDialog(true)
    })
    expect(result.current.showLoadDashboardDialog).toBe(true)
  })

  test('loads most recent dashboard from API', async () => {
    mockedGet.mockImplementation(() =>
      Promise.resolve({
        data: {
          dashboards: [{
            is_most_recent: true,
            charts: [{ tableName: 'geo', columnName: 'age', addedAt: '2024-01-01' }]
          }]
        }
      }) as any
    )

    const { result } = renderHook(() => useDashboard({ identifier: 'test-ds' }))

    // Wait for async effect
    await act(async () => {
      await new Promise(resolve => setTimeout(resolve, 100))
    })

    expect(result.current.dashboardCharts).toHaveLength(1)
    expect(result.current.dashboardCharts[0].columnName).toBe('age')
  })

  test('does nothing when identifier is undefined', () => {
    const { result } = renderHook(() => useDashboard({ identifier: undefined }))

    expect(result.current.dashboardCharts).toEqual([])
    expect(mockedGet).not.toHaveBeenCalled()
  })
})
