import { describe, test, expect, beforeEach, vi } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useFilterPresets } from '../useFilterPresets'

// Mock presetHelpers
vi.mock('../../../../utils/presetHelpers', () => ({
  savePresetsToLocalStorage: vi.fn(),
  loadPresetsFromLocalStorage: vi.fn(() => []),
  createNewPreset: vi.fn((name, filters, countBy) => ({
    id: `preset_${Date.now()}`,
    name,
    filters,
    countBySelections: countBy,
    createdAt: new Date().toISOString(),
  })),
  normalizeImportedPresets: vi.fn((presets) => presets),
}))

import {
  savePresetsToLocalStorage,
  loadPresetsFromLocalStorage,
} from '../../../../utils/presetHelpers'

const mockedLoad = vi.mocked(loadPresetsFromLocalStorage)
const mockedSave = vi.mocked(savePresetsToLocalStorage)

describe('useFilterPresets', () => {
  const baseArgs = {
    identifier: 'test-dataset',
    filters: [{ column: 'age', operator: '>', value: 30 }] as any[],
    countBySelections: {} as Record<string, any>,
    setFilters: vi.fn(),
    setCountBySelections: vi.fn(),
  }

  beforeEach(() => {
    vi.clearAllMocks()
    localStorage.clear()
    // Reset mock to return empty presets by default
    mockedLoad.mockReturnValue([])
  })

  test('loads presets from localStorage on mount', () => {
    const existing = [{ id: '1', name: 'My Preset', filters: [], countBySelections: {}, createdAt: '' }]
    mockedLoad.mockReturnValue(existing as any)

    const { result } = renderHook(() => useFilterPresets(baseArgs))

    expect(mockedLoad).toHaveBeenCalledWith(localStorage, 'test-dataset')
    expect(result.current.presets).toEqual(existing)
  })

  test('savePreset adds new preset and persists', () => {
    const { result } = renderHook(() => useFilterPresets(baseArgs))

    // Set a name first
    act(() => {
      result.current.setPresetNameInput('Test Filter')
    })

    act(() => {
      result.current.savePreset()
    })

    expect(result.current.presets).toHaveLength(1)
    expect(result.current.presets[0].name).toBe('Test Filter')
    expect(mockedSave).toHaveBeenCalledWith(localStorage, 'test-dataset', expect.any(Array))
    expect(result.current.presetNameInput).toBe('')
    expect(result.current.showSavePresetDialog).toBe(false)
  })

  test('savePreset does nothing when name is empty', () => {
    const { result } = renderHook(() => useFilterPresets(baseArgs))

    act(() => {
      result.current.savePreset()
    })

    expect(result.current.presets).toHaveLength(0)
    expect(mockedSave).not.toHaveBeenCalled()
  })

  test('deletePreset removes preset and persists', () => {
    const existing = [
      { id: '1', name: 'Preset A', filters: [], countBySelections: {}, createdAt: '' },
      { id: '2', name: 'Preset B', filters: [], countBySelections: {}, createdAt: '' },
    ]
    mockedLoad.mockReturnValue(existing as any)

    const { result } = renderHook(() => useFilterPresets(baseArgs))

    act(() => {
      result.current.deletePreset('1')
    })

    expect(result.current.presets).toHaveLength(1)
    expect(result.current.presets[0].id).toBe('2')
    expect(mockedSave).toHaveBeenCalled()
  })

  test('renamePreset updates name and persists', () => {
    const existing = [{ id: '1', name: 'Old Name', filters: [], countBySelections: {}, createdAt: '' }]
    mockedLoad.mockReturnValue(existing as any)

    const { result } = renderHook(() => useFilterPresets(baseArgs))

    act(() => {
      result.current.renamePreset('1', 'New Name')
    })

    expect(result.current.presets[0].name).toBe('New Name')
    expect(result.current.editingPresetId).toBeNull()
    expect(mockedSave).toHaveBeenCalled()
  })

  test('applyPreset calls setFilters and setCountBySelections', () => {
    const preset = {
      id: '1',
      name: 'Test',
      filters: [{ column: 'state', values: ['CA'] }],
      countBySelections: { patients: { mode: 'parent', targetTable: 'patients' } },
      createdAt: '',
    }

    const { result } = renderHook(() => useFilterPresets(baseArgs))

    act(() => {
      result.current.applyPreset(preset as any)
    })

    expect(baseArgs.setFilters).toHaveBeenCalledWith(preset.filters)
    expect(baseArgs.setCountBySelections).toHaveBeenCalledWith(
      expect.objectContaining({ patients: expect.any(Object) })
    )
    expect(result.current.showPresetsDropdown).toBe(false)
  })

  test('does not load presets when identifier is undefined', () => {
    renderHook(() => useFilterPresets({ ...baseArgs, identifier: undefined }))

    expect(mockedLoad).not.toHaveBeenCalled()
  })
})
