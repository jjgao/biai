import { useState, useEffect, useRef } from 'react'
import { ROW_COUNT_KEY } from '../../../utils/filterHelpers'
import type { CountBySelection } from '../../../utils/presetHelpers'
import { chartKey, deserializeCountBySelections } from '../utils'
import { persistChartOverrides, loadChartOverrides } from '../types'

interface UseCountByArgs {
  identifier: string | undefined
}

export function useCountBy({ identifier }: UseCountByArgs) {
  const [countBySelections, setCountBySelections] = useState<Record<string, CountBySelection>>({})
  const [countByReady, setCountByReady] = useState(false)
  const [chartCountOverrides, setChartCountOverrides] = useState<Record<string, string>>({})
  const [activeCountMenuKey, setActiveCountMenuKey] = useState<string | null>(null)

  const countByInitialized = useRef(false)
  const previousCountByRef = useRef<Record<string, CountBySelection>>({})
  const chartOverridesInitialized = useRef(false)

  // Reset on identifier change
  useEffect(() => {
    countByInitialized.current = false
    setCountBySelections({})
    setCountByReady(false)
    chartOverridesInitialized.current = false
    setChartCountOverrides({})
    setActiveCountMenuKey(null)
  }, [identifier])

  // Load chart overrides from localStorage
  useEffect(() => {
    if (!identifier) return
    try {
      const stored = loadChartOverrides(localStorage, identifier)
      setChartCountOverrides(stored || {})
    } catch (error) {
      console.error('Failed to load chart overrides:', error)
    }
    chartOverridesInitialized.current = true
  }, [identifier])

  // Persist chart overrides
  useEffect(() => {
    if (!chartOverridesInitialized.current || !identifier) return
    try {
      persistChartOverrides(localStorage, identifier, chartCountOverrides)
    } catch (error) {
      console.error('Failed to persist chart overrides:', error)
    }
  }, [chartCountOverrides, identifier])

  // Close count menu on outside click
  useEffect(() => {
    if (!activeCountMenuKey) return
    const handleClick = () => setActiveCountMenuKey(null)
    document.addEventListener('click', handleClick)
    return () => document.removeEventListener('click', handleClick)
  }, [activeCountMenuKey])

  // Init countBy from URL hash / localStorage
  useEffect(() => {
    if (countByInitialized.current) return

    const hash = window.location.hash
    const match = hash.match(/countBy=([^&]+)/)
    const encodedCountBy = match ? match[1] : null

    if (encodedCountBy) {
      const restored = deserializeCountBySelections(encodedCountBy)
      if (restored) {
        setCountBySelections(restored)
        countByInitialized.current = true
        setCountByReady(true)
        return
      }
    }

    try {
      const stored = localStorage.getItem(`countBy_${identifier}`)
      if (stored) {
        setCountBySelections(JSON.parse(stored))
      } else {
        setCountBySelections({})
      }
    } catch (error) {
      console.error('Failed to load countBy from localStorage:', error)
      setCountBySelections({})
    }
    countByInitialized.current = true
    setCountByReady(true)
  }, [identifier])

  // ── CountBy functions ─────────────────────────────────────────────

  const getCountByCacheKey = (tableName: string, override?: string): string => {
    if (override) return override
    const selection = countBySelections[tableName]
    return selection ? `parent:${selection.targetTable}` : ROW_COUNT_KEY
  }

  const getChartOverrideKey = (tableName: string, columnName: string): string | undefined =>
    chartCountOverrides[chartKey(tableName, columnName)]

  const getEffectiveCacheKeyForChart = (tableName: string, columnName: string): string => {
    const override = getChartOverrideKey(tableName, columnName)
    return override ?? getCountByCacheKey(tableName)
  }

  const setChartOverrideForChart = (tableName: string, columnName: string, override?: string) => {
    setChartCountOverrides(prev => {
      const key = chartKey(tableName, columnName)
      const defaultKey = getCountByCacheKey(tableName)
      if (!override || override === defaultKey) {
        if (!(key in prev)) return prev
        const { [key]: _removed, ...rest } = prev
        return rest
      }
      if (prev[key] === override) return prev
      return { ...prev, [key]: override }
    })
  }

  const handleCountByChange = (tableName: string, value: string) => {
    if (value === ROW_COUNT_KEY) {
      setCountBySelections(prev => {
        if (!prev[tableName]) return prev
        const next = { ...prev }
        delete next[tableName]
        return next
      })
      return
    }

    const targetTable = value.startsWith('parent:') ? value.slice('parent:'.length) : value
    if (!targetTable) return

    setCountBySelections(prev => ({
      ...prev,
      [tableName]: { mode: 'parent', targetTable }
    }))
  }

  const getCountByValueForTable = (tableName: string) => getCountByCacheKey(tableName)

  return {
    countBySelections,
    setCountBySelections,
    countByReady,
    chartCountOverrides,
    activeCountMenuKey,
    setActiveCountMenuKey,
    countByInitialized,
    previousCountByRef,
    getCountByCacheKey,
    getEffectiveCacheKeyForChart,
    setChartOverrideForChart,
    handleCountByChange,
    getCountByValueForTable,
  }
}
