import React, { useState, useEffect } from 'react'
import {
  savePresetsToLocalStorage,
  loadPresetsFromLocalStorage,
  createNewPreset,
  normalizeImportedPresets,
  type FilterPreset,
  type CountBySelection,
} from '../../../utils/presetHelpers'
import type { Filter } from '../../../utils/filterHelpers'

interface UseFilterPresetsArgs {
  identifier: string | undefined
  filters: Filter[]
  countBySelections: Record<string, CountBySelection>
  setFilters: (filters: Filter[]) => void
  setCountBySelections: (selections: Record<string, CountBySelection>) => void
}

export function useFilterPresets({
  identifier,
  filters,
  countBySelections,
  setFilters,
  setCountBySelections,
}: UseFilterPresetsArgs) {
  const [presets, setPresets] = useState<FilterPreset[]>([])
  const [showSavePresetDialog, setShowSavePresetDialog] = useState(false)
  const [showManagePresetsDialog, setShowManagePresetsDialog] = useState(false)
  const [showPresetsDropdown, setShowPresetsDropdown] = useState(false)
  const [presetNameInput, setPresetNameInput] = useState('')
  const [editingPresetId, setEditingPresetId] = useState<string | null>(null)

  // Load presets from localStorage on mount
  useEffect(() => {
    if (identifier) {
      setPresets(loadPresetsFromLocalStorage(localStorage, identifier))
    }
  }, [identifier])

  const savePreset = () => {
    if (!identifier || !presetNameInput.trim() || filters.length === 0) return

    const newPreset = createNewPreset(
      presetNameInput,
      filters,
      countBySelections
    )

    const updated = [...presets, newPreset]
    setPresets(updated)
    savePresetsToLocalStorage(localStorage, identifier, updated)
    setPresetNameInput('')
    setShowSavePresetDialog(false)
  }

  const applyPreset = (preset: FilterPreset) => {
    setFilters(preset.filters || [])
    setCountBySelections(JSON.parse(JSON.stringify(preset.countBySelections || {})))
    setShowPresetsDropdown(false)
  }

  const deletePreset = (presetId: string) => {
    if (!identifier) return
    const updated = presets.filter(p => p.id !== presetId)
    setPresets(updated)
    savePresetsToLocalStorage(localStorage, identifier, updated)
  }

  const renamePreset = (presetId: string, newName: string) => {
    if (!identifier || !newName.trim()) return
    const updated = presets.map(p =>
      p.id === presetId ? { ...p, name: newName.trim() } : p
    )
    setPresets(updated)
    savePresetsToLocalStorage(localStorage, identifier, updated)
    setEditingPresetId(null)
  }

  const exportPresets = () => {
    const json = JSON.stringify(presets, null, 2)
    const blob = new Blob([json], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `saved-filters-${identifier}-${Date.now()}.json`
    a.click()
    URL.revokeObjectURL(url)
  }

  const importPresets = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file) return

    const reader = new FileReader()
    reader.onload = (e) => {
      try {
        const imported = JSON.parse(e.target?.result as string) as FilterPreset[]
        if (Array.isArray(imported)) {
          if (!identifier) return
          const updated = [...presets, ...normalizeImportedPresets(imported)]
          setPresets(updated)
          savePresetsToLocalStorage(localStorage, identifier, updated)
        }
      } catch (error) {
        console.error('Failed to import filters:', error)
        alert('Failed to import filters. Invalid file format.')
      }
    }
    reader.readAsText(file)
    // Reset input so same file can be imported again
    event.target.value = ''
  }

  return {
    presets,
    showSavePresetDialog,
    setShowSavePresetDialog,
    showManagePresetsDialog,
    setShowManagePresetsDialog,
    showPresetsDropdown,
    setShowPresetsDropdown,
    presetNameInput,
    setPresetNameInput,
    editingPresetId,
    setEditingPresetId,
    savePreset,
    applyPreset,
    deletePreset,
    renamePreset,
    exportPresets,
    importPresets,
  }
}
