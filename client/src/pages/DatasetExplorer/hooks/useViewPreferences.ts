import { useState, useEffect, useRef } from 'react'
import { CHART_LABEL_STORAGE_PREFIX } from '../types'

interface UseViewPreferencesArgs {
  identifier: string | undefined
}

export function useViewPreferences({ identifier }: UseViewPreferencesArgs) {
  // Chart vs table view per column
  const [viewPreferences, setViewPreferences] = useState<Record<string, 'chart' | 'table'>>({})

  // Survival chart view preferences (histogram vs KM)
  const [survivalViewPreferences, setSurvivalViewPreferences] = useState<Record<string, 'histogram' | 'km'>>(() => {
    try {
      if (!identifier) return {}
      const stored = localStorage.getItem(`survivalPrefs_${identifier}`)
      return stored ? JSON.parse(stored) : {}
    } catch {
      return {}
    }
  })

  // Percentage labels on pie charts
  const [showPercentageLabels, setShowPercentageLabels] = useState(() => {
    if (!identifier) return false
    const storageKey = `${CHART_LABEL_STORAGE_PREFIX}${identifier}`
    return (localStorage.getItem(storageKey) ?? localStorage.getItem(`pieLabels_${identifier}`)) === 'percent'
  })

  // Settings menu state
  const [showSettingsMenu, setShowSettingsMenu] = useState(false)
  const settingsButtonRef = useRef<HTMLButtonElement | null>(null)
  const settingsMenuRef = useRef<HTMLDivElement | null>(null)

  // Load view preferences from localStorage on mount
  useEffect(() => {
    try {
      const stored = localStorage.getItem(`viewPrefs_${identifier}`)
      if (stored) {
        setViewPreferences(JSON.parse(stored))
      }
    } catch (error) {
      console.error('Failed to load view preferences:', error)
    }
  }, [identifier])

  // Close settings menu on identifier change
  useEffect(() => {
    setShowSettingsMenu(false)
  }, [identifier])

  // Click-outside handler for settings menu
  useEffect(() => {
    if (!showSettingsMenu) return

    const handleClick = (event: MouseEvent) => {
      const target = event.target as Node
      if (
        settingsMenuRef.current &&
        !settingsMenuRef.current.contains(target) &&
        !settingsButtonRef.current?.contains(target)
      ) {
        setShowSettingsMenu(false)
      }
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setShowSettingsMenu(false)
      }
    }

    document.addEventListener('mousedown', handleClick)
    document.addEventListener('keydown', handleKeyDown)

    return () => {
      document.removeEventListener('mousedown', handleClick)
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [showSettingsMenu])

  // Re-load chart label preference when identifier changes
  useEffect(() => {
    const storageKey = `${CHART_LABEL_STORAGE_PREFIX}${identifier}`
    const stored = localStorage.getItem(storageKey) ?? localStorage.getItem(`pieLabels_${identifier}`)
    setShowPercentageLabels(stored === 'percent')
  }, [identifier])

  // Re-load survival view preferences when identifier changes
  useEffect(() => {
    try {
      const stored = localStorage.getItem(`survivalPrefs_${identifier}`)
      if (stored) {
        setSurvivalViewPreferences(JSON.parse(stored))
      } else {
        setSurvivalViewPreferences({})
      }
    } catch {
      setSurvivalViewPreferences({})
    }
  }, [identifier])

  const getViewPreference = (tableName: string, columnName: string, categoryCount: number): 'chart' | 'table' => {
    const key = `${tableName}.${columnName}`
    if (viewPreferences[key]) {
      return viewPreferences[key]
    }
    return categoryCount > 8 ? 'table' : 'chart'
  }

  const toggleViewPreference = (tableName: string, columnName: string) => {
    const key = `${tableName}.${columnName}`
    setViewPreferences(prev => {
      const current = prev[key]
      const newValue: 'table' | 'chart' = current === 'table' ? 'chart' : 'table'
      const updated: Record<string, 'table' | 'chart'> = { ...prev, [key]: newValue }
      try {
        localStorage.setItem(`viewPrefs_${identifier}`, JSON.stringify(updated))
      } catch (error) {
        console.error('Failed to save view preferences:', error)
      }
      return updated
    })
  }

  const getSurvivalViewPreference = (tableName: string, columnName: string): 'histogram' | 'km' => {
    const key = `${tableName}.${columnName}`
    return survivalViewPreferences[key] || 'histogram'
  }

  const toggleSurvivalViewPreference = (tableName: string, columnName: string) => {
    const key = `${tableName}.${columnName}`
    setSurvivalViewPreferences(prev => {
      const current = prev[key] || 'histogram'
      const next: 'histogram' | 'km' = current === 'histogram' ? 'km' : 'histogram'
      const updated: Record<string, 'histogram' | 'km'> = { ...prev, [key]: next }
      try {
        localStorage.setItem(`survivalPrefs_${identifier}`, JSON.stringify(updated))
      } catch (error) {
        console.error('Failed to save survival view preferences:', error)
      }
      return updated
    })
  }

  return {
    showPercentageLabels,
    setShowPercentageLabels,
    showSettingsMenu,
    setShowSettingsMenu,
    settingsButtonRef,
    settingsMenuRef,
    getViewPreference,
    toggleViewPreference,
    getSurvivalViewPreference,
    toggleSurvivalViewPreference,
  }
}
