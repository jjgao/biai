import { describe, expect, test, vi, beforeEach } from 'vitest'
import {
    savePresetsToLocalStorage,
    loadPresetsFromLocalStorage,
    createNewPreset,
    normalizeImportedPresets,
    type FilterPreset
} from '../presetHelpers'
import { ROW_COUNT_KEY } from '../filterHelpers'

describe('presetHelpers', () => {
    const mockStorage = {
        getItem: vi.fn(),
        setItem: vi.fn(),
        removeItem: vi.fn(),
        clear: vi.fn(),
        key: vi.fn(),
        length: 0
    }

    beforeEach(() => {
        vi.clearAllMocks()
    })

    describe('savePresetsToLocalStorage', () => {
        test('saves presets to storage with correct key', () => {
            const presets: FilterPreset[] = [{ id: '1', name: 'Test', filters: [], createdAt: 'now' }]
            savePresetsToLocalStorage(mockStorage, 'test-id', presets)
            expect(mockStorage.setItem).toHaveBeenCalledWith(
                'presets_test-id',
                JSON.stringify(presets)
            )
        })

        test('handles storage errors gracefully', () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => { })
            mockStorage.setItem.mockImplementationOnce(() => { throw new Error('Quota exceeded') })

            const presets: FilterPreset[] = []
            expect(() => savePresetsToLocalStorage(mockStorage, 'id', presets)).not.toThrow()
            expect(consoleSpy).toHaveBeenCalled()

            consoleSpy.mockRestore()
        })
    })

    describe('loadPresetsFromLocalStorage', () => {
        test('loads and parses presets', () => {
            const presets = [{ id: '1', name: 'Test', filters: [], createdAt: 'now' }]
            mockStorage.getItem.mockReturnValue(JSON.stringify(presets))

            const loaded = loadPresetsFromLocalStorage(mockStorage, 'test-id')
            expect(loaded).toHaveLength(1)
            expect(loaded[0].name).toBe('Test')
        })

        test('returns empty array if storage is empty', () => {
            mockStorage.getItem.mockReturnValue(null)
            expect(loadPresetsFromLocalStorage(mockStorage, 'id')).toEqual([])
        })

        test('migrates legacy filters on load', () => {
            const legacyPreset = {
                id: '1',
                name: 'Legacy',
                filters: [{ column: 'age', operator: 'eq', value: 10, tableName: 't1' }], // missing countByKey
                createdAt: 'old'
            }
            mockStorage.getItem.mockReturnValue(JSON.stringify([legacyPreset]))

            const loaded = loadPresetsFromLocalStorage(mockStorage, 'id')
            expect(loaded[0].filters[0].countByKey).toBe(ROW_COUNT_KEY)
        })

        test('handles parse errors gracefully', () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => { })
            mockStorage.getItem.mockReturnValue('invalid-json')

            expect(loadPresetsFromLocalStorage(mockStorage, 'id')).toEqual([])
            expect(consoleSpy).toHaveBeenCalled()

            consoleSpy.mockRestore()
        })
    })

    describe('createNewPreset', () => {
        test('creates preset with deep cloned data', () => {
            const filters = [{ column: 'a', value: 1 }]
            const countBy = { t1: { mode: 'parent' as const, targetTable: 't2' } }

            const preset = createNewPreset(' My Preset ', filters, countBy)

            expect(preset.name).toBe('My Preset')
            expect(preset.id).toBeDefined()
            expect(preset.createdAt).toBeDefined()
            expect(preset.filters).toEqual(filters)
            expect(preset.countBySelections).toEqual(countBy)

            // Verify independence
            filters[0].value = 2
            expect(preset.filters[0].value).toBe(1)
        })
    })

    describe('normalizeImportedPresets', () => {
        test('normalizes legacy imported data', () => {
            const imported = [
                {
                    id: '1',
                    name: 'Imported',
                    filters: [{ column: 'age', tableName: 't1' }]
                }
            ]

            const normalized = normalizeImportedPresets(imported)
            expect(normalized[0].filters[0].countByKey).toBe(ROW_COUNT_KEY)
            expect(normalized[0].countBySelections).toEqual({})
        })

        test('throws for non-array input', () => {
            expect(() => normalizeImportedPresets({} as any)).toThrow()
        })
    })
})
