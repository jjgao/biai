import { type Filter, migrateFiltersToCurrentSchema } from './filterHelpers'

export interface CountBySelection {
    mode: 'parent'
    targetTable: string
}

export interface FilterPreset {
    id: string
    name: string
    filters: Filter[]
    countBySelections?: Record<string, CountBySelection>
    createdAt: string
}

const getStorageKey = (identifier: string) => `presets_${identifier}`

export const savePresetsToLocalStorage = (
    storage: Storage,
    identifier: string,
    presets: FilterPreset[]
): void => {
    try {
        storage.setItem(getStorageKey(identifier), JSON.stringify(presets))
    } catch (error) {
        console.error('Failed to save presets to localStorage:', error)
    }
}

export const loadPresetsFromLocalStorage = (
    storage: Storage,
    identifier: string
): FilterPreset[] => {
    try {
        const stored = storage.getItem(getStorageKey(identifier))
        if (!stored) return []
        const parsed: FilterPreset[] = JSON.parse(stored)
        return parsed.map(preset => ({
            ...preset,
            filters: migrateFiltersToCurrentSchema(preset.filters || []),
            countBySelections: preset.countBySelections || {}
        }))
    } catch (error) {
        console.error('Failed to load presets from localStorage:', error)
        return []
    }
}

export const createNewPreset = (
    name: string,
    filters: Filter[],
    countBySelections: Record<string, CountBySelection>
): FilterPreset => ({
    id: Date.now().toString(),
    name: name.trim(),
    // Deep clone to prevent mutation issues
    filters: JSON.parse(JSON.stringify(filters)),
    countBySelections: JSON.parse(JSON.stringify(countBySelections)),
    createdAt: new Date().toISOString()
})

/**
 * Normalizes imported presets ensuring schema compatibility.
 */
export const normalizeImportedPresets = (imported: any[]): FilterPreset[] => {
    if (!Array.isArray(imported)) {
        throw new Error('Imported data is not an array')
    }
    return imported.map(preset => {
        // Basic validation could go here
        if (!preset.name || !preset.filters) {
            // In a real app we might throw or skip, for now we let schema migration handle structure
        }
        return {
            ...preset,
            filters: migrateFiltersToCurrentSchema(preset.filters || []),
            countBySelections: preset.countBySelections || {}
        }
    })
}
