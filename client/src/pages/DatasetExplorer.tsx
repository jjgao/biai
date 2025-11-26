import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react'
import { useParams, useNavigate, useLocation } from 'react-router-dom'
import Plot from 'react-plotly.js'
import type { PlotMouseEvent, PlotSelectionEvent } from 'plotly.js'
import SafeHtml from '../components/SafeHtml'
import api from '../services/api'
import type { MetricPathSegment } from '../types'
import { findRelationshipPath, type Filter } from '../utils/filterHelpers'
import { getStateCode, normalizeStateName } from '../data/us-states'
// Small categorical sets render better as pie charts; beyond this use bars.
const MAX_PIE_CATEGORIES = 8
const ROW_COUNT_KEY = 'rows'
const CHART_LABEL_STORAGE_PREFIX = 'chartLabels_'
const CHART_OVERRIDE_STORAGE_PREFIX = 'chartOverrides_'
const TABLE_SCOPE_KEY = 'table'
const DASHBOARD_SCOPE_KEY = 'dashboard'
const CACHE_TTL_MS = 5 * 60 * 1000 // 5 minutes
const CACHE_MAX_ENTRIES_PER_TABLE = 5
const MAX_ANCESTOR_DEPTH = 4

export const unwrapNot = (filter?: Filter | null): Filter | undefined => {
  if (!filter) return undefined
  return filter.not ?? filter
}

const cloneFilterNode = (filter: Filter): Filter => {
  const cloned: Filter = {
    ...filter,
    and: filter.and ? filter.and.map(cloneFilterNode) : undefined,
    or: filter.or ? filter.or.map(cloneFilterNode) : undefined,
    not: filter.not ? cloneFilterNode(filter.not) : undefined
  }

  if (cloned.countByKey === undefined && cloned.tableName) {
    cloned.countByKey = ROW_COUNT_KEY
  }

  return cloned
}

export const migrateFiltersToCurrentSchema = (filters: Filter[]): Filter[] =>
  filters.map(cloneFilterNode)

const chartOverrideStorageKey = (identifier: string) => `${CHART_OVERRIDE_STORAGE_PREFIX}${identifier}`

export const persistChartOverrides = (
  storage: Storage,
  identifier: string,
  overrides: Record<string, string>
): void => {
  const key = chartOverrideStorageKey(identifier)
  if (Object.keys(overrides).length === 0) {
    storage.removeItem(key)
  } else {
    storage.setItem(key, JSON.stringify(overrides))
  }
}

export const loadChartOverrides = (
  storage: Storage,
  identifier: string
): Record<string, string> | null => {
  const key = chartOverrideStorageKey(identifier)
  const stored = storage.getItem(key)
  if (!stored) return null
  try {
    return JSON.parse(stored)
  } catch {
    return null
  }
}

interface Column {
  name: string
  type: string
  nullable: boolean
}

interface ColumnMetadata {
  column_name: string
  column_type: string
  column_index: number
  is_nullable: boolean
  display_name: string
  description: string
  user_data_type: string
  user_priority: number | null
  display_type: string
  unique_value_count: number
  null_count: number
  min_value: string | null
  max_value: string | null
  suggested_chart: string
  display_priority: number
  is_hidden: boolean
  // Temporal filtering fields
  temporal_role?: 'none' | 'start_date' | 'stop_date' | 'duration'
  temporal_paired_column?: string
  temporal_unit?: 'days' | 'months' | 'years'
}

interface CategoryCount {
  value: string
  display_value: string
  count: number
  percentage: number
}

interface NumericStats {
  min: number
  max: number
  mean: number
  median: number
  stddev: number
  q25: number
  q75: number
}

interface HistogramBin {
  bin_start: number
  bin_end: number
  count: number
  percentage: number
}

interface SurvivalCurvePoint {
  time: number
  atRisk: number
  events: number
  censored: number
  survival: number
}

interface ColumnAggregation {
  column_name: string
  display_type: string
  normalized_display_type?: string
  total_rows: number
  null_count: number
  unique_count: number
  categories?: CategoryCount[]
  numeric_stats?: NumericStats
  histogram?: HistogramBin[]
  metric_type?: 'rows' | 'parent'
  metric_parent_table?: string
  metric_parent_column?: string
  metric_path?: MetricPathSegment[]
}

interface FilterPreset {
  id: string
  name: string
  filters: Filter[]
  countBySelections: Record<string, CountBySelection>
  createdAt: string
}

interface SavedDashboard {
  id: string
  name: string
  charts: Array<{ tableName: string; columnName: string; addedAt: string }>
  createdAt: string
  updatedAt: string
}

interface TableRelationship {
  foreign_key: string
  referenced_table: string
  referenced_column: string
  type?: string
}

interface Table {
  id: string
  name: string
  displayName: string
  rowCount: number
  columns: Column[]
  primaryKey?: string
  relationships?: TableRelationship[]
}

interface Dataset {
  id: string
  name: string
  database_name?: string
  database_type?: 'created' | 'connected'
  description: string
  tags?: string[]
  tables: Table[]
}

type CountBySelection = {
  mode: 'parent'
  targetTable: string
}

type AncestorOption = {
  targetTable: string
  label: string
  key: string
  path: MetricPathSegment[]
}

type AggregationCacheEntry = {
  data: ColumnAggregation[]
  filtersKey: string
  timestamp: number
}

type SurvivalCacheEntry = {
  data: SurvivalCurvePoint[]
  filtersKey: string
  countByKey: string
  statusColumn: string
  timestamp: number
}

function DatasetExplorer() {
  const { id, database } = useParams()
  const navigate = useNavigate()
  const location = useLocation()

  // Determine if we're in database mode or dataset mode
  const isDatabaseMode = !!database
  const identifier = database || id
  const [dataset, setDataset] = useState<Dataset | null>(null)

  // Helper to determine if we should use database API
  // Use database API if:
  // 1. We're in database mode (viewing from /databases/:database), OR
  // 2. The dataset is a "connected" type (registered existing database)
  const usesDatabaseAPI = isDatabaseMode ? true : dataset?.database_type === 'connected'
  const databaseIdentifier = isDatabaseMode ? identifier : dataset?.database_name
  const datasetIdentifier = dataset?.id
  const [loading, setLoading] = useState(true)
  const [columnMetadata, setColumnMetadata] = useState<Record<string, ColumnMetadata[]>>({})
  const [aggregations, setAggregations] = useState<Record<string, Record<string, AggregationCacheEntry>>>({})
  const [survivalCurves, setSurvivalCurves] = useState<Record<string, Record<string, SurvivalCacheEntry>>>({})
  const [baselineAggregations, setBaselineAggregations] = useState<Record<string, ColumnAggregation[]>>({})
  const [filters, setFilters] = useState<Filter[]>([])
  const [activeFilterMenu, setActiveFilterMenu] = useState<{ tableName: string; columnName: string; countKey?: string } | null>(null)
  const [customRangeInputs, setCustomRangeInputs] = useState<Record<string, { min: string; max: string }>>({})
  const [rangeSelections, setRangeSelections] = useState<Record<string, Array<{ start: number; end: number }>>>({})
  const [countBySelections, setCountBySelections] = useState<Record<string, CountBySelection>>({})
  const [countByReady, setCountByReady] = useState(false)

  // Filter preset state
  const [presets, setPresets] = useState<FilterPreset[]>([])
  const [showSavePresetDialog, setShowSavePresetDialog] = useState(false)
  const [showManagePresetsDialog, setShowManagePresetsDialog] = useState(false)
  const [showPresetsDropdown, setShowPresetsDropdown] = useState(false)
  const [presetNameInput, setPresetNameInput] = useState('')
  const [editingPresetId, setEditingPresetId] = useState<string | null>(null)

  // View preferences: track whether each column should show chart or table
  // Key format: "tableName.columnName", Value: "chart" | "table"
  const [viewPreferences, setViewPreferences] = useState<Record<string, 'chart' | 'table'>>({})

  // Tab navigation state: track which table tab is currently active
  const [activeTab, setActiveTab] = useState<string | null>(null)

  // Dashboard state: track which charts are pinned to dashboard
  const [dashboardCharts, setDashboardCharts] = useState<Array<{ tableName: string; columnName: string; countByTarget: string | null; addedAt: string }>>([])
  const [chartCountOverrides, setChartCountOverrides] = useState<Record<string, string>>({})
  const [activeCountMenuKey, setActiveCountMenuKey] = useState<string | null>(null)
  const [ancestorOptions, setAncestorOptions] = useState<Record<string, AncestorOption[]>>({})
  const [visibleDashboardKeys, setVisibleDashboardKeys] = useState<Record<string, boolean>>({})
  const [survivalViewPreferences, setSurvivalViewPreferences] = useState<Record<string, 'histogram' | 'km'>>({})
  const survivalRequests = useRef<Set<string>>(new Set())
  const dashboardObserverRef = useRef<IntersectionObserver | null>(null)
  const dashboardCardRefs = useRef<Record<string, HTMLDivElement | null>>({})
  const dashboardElementKeyMap = useRef<Map<Element, string>>(new Map())
  const intersectionObserverAvailable = typeof window !== 'undefined' && 'IntersectionObserver' in window

  // Saved dashboards state
  const [savedDashboards, setSavedDashboards] = useState<SavedDashboard[]>([])
  const [activeDashboardId, setActiveDashboardId] = useState<string | null>(null) // null = "Most Recent"
  const [showSaveDashboardDialog, setShowSaveDashboardDialog] = useState(false)
  const [showLoadDashboardDialog, setShowLoadDashboardDialog] = useState(false)
  const [showManageDashboardsDialog, setShowManageDashboardsDialog] = useState(false)
  const [newDashboardName, setNewDashboardName] = useState('')
  const [editingDashboardId, setEditingDashboardId] = useState<string | null>(null)
  const [editingDashboardName, setEditingDashboardName] = useState('')

  // Track if filters have been initialized from URL to prevent overwriting
  const filtersInitialized = useRef(false)
  const isUpdatingURL = useRef(false)
  const dashboardInitialized = useRef(false)
  const savedDashboardsInitialized = useRef(false)
  const countByInitialized = useRef(false)
  const previousCountByRef = useRef<Record<string, CountBySelection>>({})
  const chartOverridesInitialized = useRef(false)
  const [showPercentageLabels, setShowPercentageLabels] = useState(false)
  const [showSettingsMenu, setShowSettingsMenu] = useState(false)
  const settingsButtonRef = useRef<HTMLButtonElement | null>(null)
  const settingsMenuRef = useRef<HTMLDivElement | null>(null)

  // Helper functions for URL persistence
  const serializeFilters = (filters: Filter[]): string => {
    try {
      const json = JSON.stringify(filters)
      return btoa(encodeURIComponent(json))
    } catch (error) {
      console.error('Failed to serialize filters:', error)
      return ''
    }
  }

  const deserializeFilters = (encoded: string): Filter[] | null => {
    try {
      const json = decodeURIComponent(atob(encoded))
      return JSON.parse(json)
    } catch (error) {
      console.error('Failed to deserialize filters:', error)
      return null
    }
  }

  const saveFiltersToLocalStorage = (filters: Filter[]) => {
    try {
      localStorage.setItem(`filters_${identifier}`, JSON.stringify(filters))
    } catch (error) {
      console.error('Failed to save filters to localStorage:', error)
    }
  }

  const loadFiltersFromLocalStorage = (): Filter[] | null => {
    try {
      const stored = localStorage.getItem(`filters_${identifier}`)
      if (!stored) return null
      const parsed: Filter[] = JSON.parse(stored)
      if (parsed.length === 0) return []
      return migrateFiltersToCurrentSchema(parsed)
    } catch (error) {
      console.error('Failed to load filters from localStorage:', error)
      return null
    }
  }

  const buildFiltersKey = (list?: Filter[]): string => JSON.stringify(list ?? [])
  const currentFiltersKey = useMemo(() => buildFiltersKey(filters), [filters])

  const serializeCountBySelections = (selections: Record<string, CountBySelection>): string => {
    try {
      return btoa(encodeURIComponent(JSON.stringify(selections)))
    } catch (error) {
      console.error('Failed to serialize countBy selections:', error)
      return ''
    }
  }

  const deserializeCountBySelections = (encoded: string): Record<string, CountBySelection> | null => {
    try {
      const json = decodeURIComponent(atob(encoded))
      return JSON.parse(json)
    } catch (error) {
      console.error('Failed to deserialize countBy selections:', error)
      return null
    }
  }

  const saveCountByToLocalStorage = (selections: Record<string, CountBySelection>) => {
    try {
      if (Object.keys(selections).length === 0) {
        localStorage.removeItem(`countBy_${identifier}`)
      } else {
        localStorage.setItem(`countBy_${identifier}`, JSON.stringify(selections))
      }
    } catch (error) {
      console.error('Failed to persist countBy selections:', error)
    }
  }

  const loadCountByFromLocalStorage = (): Record<string, CountBySelection> | null => {
    try {
      const stored = localStorage.getItem(`countBy_${identifier}`)
      return stored ? JSON.parse(stored) : null
    } catch (error) {
      console.error('Failed to load countBy selections from localStorage:', error)
      return null
    }
  }

  const saveChartOverridesToLocalStorage = (overrides: Record<string, string>) => {
    try {
      persistChartOverrides(localStorage, identifier, overrides)
    } catch (error) {
      console.error('Failed to persist chart overrides:', error)
    }
  }

  const loadChartOverridesFromLocalStorage = (): Record<string, string> | null => {
    try {
      return loadChartOverrides(localStorage, identifier)
    } catch (error) {
      console.error('Failed to load chart overrides from localStorage:', error)
      return null
    }
  }

  // Helper functions for preset management
  const savePresetsToLocalStorage = (presets: FilterPreset[]) => {
    try {
      localStorage.setItem(`presets_${identifier}`, JSON.stringify(presets))
    } catch (error) {
      console.error('Failed to save presets to localStorage:', error)
    }
  }

  const loadPresetsFromLocalStorage = (): FilterPreset[] => {
    try {
      const stored = localStorage.getItem(`presets_${identifier}`)
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

  const savePreset = () => {
    if (!presetNameInput.trim() || filters.length === 0) return

    const newPreset: FilterPreset = {
      id: Date.now().toString(),
      name: presetNameInput.trim(),
      filters: JSON.parse(JSON.stringify(filters)), // Deep clone
      countBySelections: JSON.parse(JSON.stringify(countBySelections)),
      createdAt: new Date().toISOString()
    }

    const updated = [...presets, newPreset]
    setPresets(updated)
    savePresetsToLocalStorage(updated)
    setPresetNameInput('')
    setShowSavePresetDialog(false)
  }

  const applyPreset = (preset: FilterPreset) => {
    setFilters(migrateFiltersToCurrentSchema(preset.filters || []))
    setCountBySelections(JSON.parse(JSON.stringify(preset.countBySelections || {})))
    setShowPresetsDropdown(false)
  }

  const deletePreset = (presetId: string) => {
    const updated = presets.filter(p => p.id !== presetId)
    setPresets(updated)
    savePresetsToLocalStorage(updated)
  }

  const renamePreset = (presetId: string, newName: string) => {
    if (!newName.trim()) return
    const updated = presets.map(p =>
      p.id === presetId ? { ...p, name: newName.trim() } : p
    )
    setPresets(updated)
    savePresetsToLocalStorage(updated)
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
          const normalized = imported.map(preset => ({
            ...preset,
            countBySelections: preset.countBySelections || {}
          }))
          const updated = [...presets, ...normalized]
          setPresets(updated)
          savePresetsToLocalStorage(updated)
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

  // Helper functions for view preferences
  const getViewPreference = (tableName: string, columnName: string, categoryCount: number): 'chart' | 'table' => {
    const key = `${tableName}.${columnName}`
    // Check if user has set a preference
    if (viewPreferences[key]) {
      return viewPreferences[key]
    }
    // Default: table for >8 categories, chart for ≤8
    return categoryCount > 8 ? 'table' : 'chart'
  }

  const toggleViewPreference = (tableName: string, columnName: string) => {
    const key = `${tableName}.${columnName}`
    setViewPreferences(prev => {
      const current = prev[key]
      const newValue = current === 'table' ? 'chart' : 'table'
      const updated = { ...prev, [key]: newValue }
      // Save to localStorage
      try {
        localStorage.setItem(`viewPrefs_${identifier}`, JSON.stringify(updated))
      } catch (error) {
        console.error('Failed to save view preferences:', error)
      }
      return updated
    })
  }

  // Survival view preferences (histogram vs KM)
  const getSurvivalViewPreference = (tableName: string, columnName: string): 'histogram' | 'km' => {
    const key = `${tableName}.${columnName}`
    return survivalViewPreferences[key] || 'histogram'
  }

  const toggleSurvivalViewPreference = (tableName: string, columnName: string) => {
    const key = `${tableName}.${columnName}`
    setSurvivalViewPreferences(prev => {
      const current = prev[key] || 'histogram'
      const next = current === 'histogram' ? 'km' : 'histogram'
      return { ...prev, [key]: next }
    })
  }

  // Dashboard chart management
  const isOnDashboard = (tableName: string, columnName: string): boolean => {
    const cacheKey = getEffectiveCacheKeyForChart(tableName, columnName)
    const target = targetFromCacheKey(cacheKey)
    return dashboardCharts.some(chart =>
      chart.tableName === tableName &&
      chart.columnName === columnName &&
      chart.countByTarget === target
    )
  }

  const toggleDashboard = (tableName: string, columnName: string) => {
    const cacheKey = getEffectiveCacheKeyForChart(tableName, columnName)
    const target = targetFromCacheKey(cacheKey)
    if (isOnDashboard(tableName, columnName)) {
      // Remove from dashboard
      setDashboardCharts(prev =>
        prev.filter(chart =>
          !(chart.tableName === tableName && chart.columnName === columnName && chart.countByTarget === target)
        ))
    } else {
      // Add to dashboard
      setDashboardCharts(prev => [...prev, { tableName, columnName, countByTarget: target, addedAt: new Date().toISOString() }])
      ensureAggregationForCacheKey(tableName, cacheKey)
    }
  }

  const addAllChartsToTable = (tableName: string) => {
    const tableAggregations = getAggregationsForTable(tableName)
    const tableMetadata = columnMetadata[tableName]
    if (!tableAggregations || tableAggregations.length === 0) return
    if (!tableMetadata || !Array.isArray(tableMetadata)) return

    // Get all visible aggregations for this table
    const visibleAggregations = tableAggregations.filter(agg => {
      const metadata = tableMetadata.find(m => m.column_name === agg.column_name)
      return !metadata?.is_hidden
    })

    // Add all charts that aren't already on dashboard
    const newCharts = visibleAggregations
      .map(agg => {
        const cacheKey = getEffectiveCacheKeyForChart(tableName, agg.column_name)
        const target = targetFromCacheKey(cacheKey)
        return {
          tableName,
          columnName: agg.column_name,
          countByTarget: target,
          addedAt: new Date().toISOString()
        }
      })
      .filter(newChart =>
        !dashboardCharts.some(chart =>
          chart.tableName === newChart.tableName &&
          chart.columnName === newChart.columnName &&
          chart.countByTarget === newChart.countByTarget
        )
      )

    if (newCharts.length > 0) {
      setDashboardCharts(prev => [...prev, ...newCharts])
      newCharts.forEach(chart => {
        const cacheKey = chart.countByTarget ? `parent:${chart.countByTarget}` : ROW_COUNT_KEY
        ensureAggregationForCacheKey(chart.tableName, cacheKey)
      })
    }
  }

  const getTableChartCount = (tableName: string): number => {
    const tableAggregations = baselineAggregations[tableName] || []
    const tableMetadata = columnMetadata[tableName]
    if (!tableMetadata || !Array.isArray(tableMetadata)) return 0

    return tableAggregations.filter(agg => {
      const metadata = tableMetadata.find(m => m.column_name === agg.column_name)
      return !metadata?.is_hidden
    }).length
  }

  const getDashboardChartKey = (chart: { tableName: string; columnName: string; countByTarget: string | null }) =>
    `${chart.tableName}:${chart.columnName}:${chart.countByTarget ?? 'rows'}`

  const registerDashboardCard = useCallback(
    (key: string) => (node: HTMLDivElement | null) => {
      const observer = dashboardObserverRef.current
      const prevNode = dashboardCardRefs.current[key]
      if (prevNode) {
        if (observer) {
          observer.unobserve(prevNode)
        }
        dashboardElementKeyMap.current.delete(prevNode)
      }
      if (!node) {
        dashboardCardRefs.current[key] = null
        return
      }
      dashboardCardRefs.current[key] = node
      if (observer) {
        dashboardElementKeyMap.current.set(node, key)
        observer.observe(node)
      }
    },
    []
  )

  // Saved dashboard management
  const saveDashboard = async (name: string) => {
    const newDashboard: SavedDashboard = {
      id: `dashboard_${Date.now()}`,
      name,
      charts: [...dashboardCharts],
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    }

    try {
      // Save to database
      await api.post(`/datasets/${identifier}/dashboards`, {
        dashboard_id: newDashboard.id,
        dashboard_name: newDashboard.name,
        charts: newDashboard.charts,
        is_most_recent: false
      })

      // Update local state
      setSavedDashboards(prev => [...prev, newDashboard])
      setShowSaveDashboardDialog(false)
      setNewDashboardName('')
    } catch (error) {
      console.error('Failed to save dashboard:', error)
      alert('Failed to save dashboard. Please try again.')
    }
  }

  const loadDashboard = (dashboardId: string) => {
    const dashboard = savedDashboards.find(d => d.id === dashboardId)
    if (dashboard) {
      setDashboardCharts(normalizeDashboardCharts(dashboard.charts))
      setActiveDashboardId(dashboardId)
    }
  }

  const loadMostRecent = () => {
    // Most Recent is the current dashboardCharts state (already loaded from database)
    setActiveDashboardId(null)
  }

  const deleteDashboard = async (dashboardId: string) => {
    try {
      // Delete from database
      await api.delete(`/datasets/${identifier}/dashboards/${dashboardId}`)

      // Update local state
      setSavedDashboards(prev => prev.filter(d => d.id !== dashboardId))
      if (activeDashboardId === dashboardId) {
        setActiveDashboardId(null)
      }
    } catch (error) {
      console.error('Failed to delete dashboard:', error)
      alert('Failed to delete dashboard. Please try again.')
    }
  }

  const renameDashboard = async (dashboardId: string, newName: string) => {
    const dashboard = savedDashboards.find(d => d.id === dashboardId)
    if (!dashboard) return

    try {
      // Update in database
      await api.post(`/datasets/${identifier}/dashboards`, {
        dashboard_id: dashboardId,
        dashboard_name: newName,
        charts: dashboard.charts,
        is_most_recent: false
      })

      // Update local state
      setSavedDashboards(prev => prev.map(d =>
        d.id === dashboardId
          ? { ...d, name: newName, updatedAt: new Date().toISOString() }
          : d
      ))
      setEditingDashboardId(null)
      setEditingDashboardName('')
    } catch (error) {
      console.error('Failed to rename dashboard:', error)
      alert('Failed to rename dashboard. Please try again.')
    }
  }

  const getCurrentDashboardName = (): string => {
    if (!activeDashboardId) return 'Most Recent'
    const dashboard = savedDashboards.find(d => d.id === activeDashboardId)
    return dashboard?.name || 'Most Recent'
  }

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

  // Load "Most Recent" dashboard from database on mount (only once)
  useEffect(() => {
    if (!identifier || dashboardInitialized.current) return

    const loadMostRecentDashboard = async () => {
      try {
        // Try to load from API first
        const response = await api.get(`/datasets/${identifier}/dashboards`)
        const dashboards = response.data.dashboards || []
        const mostRecent = dashboards.find((d: any) => d.is_most_recent)

        if (mostRecent) {
          setDashboardCharts(normalizeDashboardCharts(mostRecent.charts))
        } else {
          // Migration: check localStorage for legacy data
          const key = `dashboard_${identifier}`
          const stored = localStorage.getItem(key)
          if (stored) {
            const charts = JSON.parse(stored)
            setDashboardCharts(normalizeDashboardCharts(charts))
            // Migrate to database
            if (charts.length > 0) {
              await api.post(`/datasets/${identifier}/dashboards`, {
                dashboard_id: 'most_recent',
                dashboard_name: 'Most Recent',
                charts,
                is_most_recent: true
              })
            }
            // Clear localStorage after migration
            localStorage.removeItem(key)
          }
        }
      } catch (error) {
        console.error('Failed to load most recent dashboard:', error)
        // Fallback to localStorage if API fails
        try {
          const key = `dashboard_${identifier}`
          const stored = localStorage.getItem(key)
          if (stored) {
            setDashboardCharts(normalizeDashboardCharts(JSON.parse(stored)))
          }
        } catch (e) {
          console.error('Failed to load from localStorage:', e)
        }
      } finally {
        setTimeout(() => {
          dashboardInitialized.current = true
        }, 50)
      }
    }

    loadMostRecentDashboard()
  }, [identifier])

  // Save "Most Recent" dashboard to database when changed (only after initial load)
  useEffect(() => {
    if (!dashboardInitialized.current || !identifier) return

    const saveMostRecentDashboard = async () => {
      try {
        await api.post(`/datasets/${identifier}/dashboards`, {
          dashboard_id: 'most_recent',
          dashboard_name: 'Most Recent',
          charts: dashboardCharts,
          is_most_recent: true
        })
      } catch (error) {
        console.error('Failed to save most recent dashboard:', error)
        // Fallback to localStorage if API fails
        try {
          const key = `dashboard_${identifier}`
          localStorage.setItem(key, JSON.stringify(dashboardCharts))
        } catch (e) {
          console.error('Failed to save to localStorage:', e)
        }
      }
    }

    saveMostRecentDashboard()
  }, [dashboardCharts, identifier])

  // Load saved dashboards from database on mount (only once)
  useEffect(() => {
    if (!identifier || savedDashboardsInitialized.current) return

    const loadSavedDashboards = async () => {
      try {
        // Try to load from API first
        const response = await api.get(`/datasets/${identifier}/dashboards`)
        const dashboards = response.data.dashboards || []

        // Filter out "Most Recent" (is_most_recent = true)
        const savedOnly = dashboards
          .filter((d: any) => !d.is_most_recent)
          .map((d: any) => ({
            id: d.dashboard_id,
            name: d.dashboard_name,
            charts: d.charts,
            createdAt: d.created_at,
            updatedAt: d.updated_at
          }))

        setSavedDashboards(savedOnly)

        // Migration: check localStorage for legacy data
        const key = `savedDashboards_${identifier}`
        const stored = localStorage.getItem(key)
        if (stored) {
          const localDashboards = JSON.parse(stored)

          // Migrate each dashboard to database
          for (const dashboard of localDashboards) {
            try {
              await api.post(`/datasets/${identifier}/dashboards`, {
                dashboard_id: dashboard.id,
                dashboard_name: dashboard.name,
                charts: dashboard.charts,
                is_most_recent: false
              })
            } catch (err) {
              console.error(`Failed to migrate dashboard ${dashboard.id}:`, err)
            }
          }

          // Clear localStorage after migration
          localStorage.removeItem(key)

          // Reload dashboards after migration
          const updatedResponse = await api.get(`/datasets/${identifier}/dashboards`)
          const updatedDashboards = updatedResponse.data.dashboards || []
          const updatedSavedOnly = updatedDashboards
            .filter((d: any) => !d.is_most_recent)
            .map((d: any) => ({
              id: d.dashboard_id,
              name: d.dashboard_name,
              charts: d.charts,
              createdAt: d.created_at,
              updatedAt: d.updated_at
            }))
          setSavedDashboards(updatedSavedOnly)
        }
      } catch (error) {
        console.error('Failed to load saved dashboards from database:', error)
        // Fallback to localStorage if API fails
        try {
          const key = `savedDashboards_${identifier}`
          const stored = localStorage.getItem(key)
          if (stored) {
            setSavedDashboards(JSON.parse(stored))
          }
        } catch (e) {
          console.error('Failed to load from localStorage:', e)
        }
      } finally {
        setTimeout(() => {
          savedDashboardsInitialized.current = true
        }, 50)
      }
    }

    loadSavedDashboards()
  }, [identifier])

  // Note: Saved dashboards are now persisted individually through save/update/delete functions
  // No need for a bulk save effect since each operation updates the database directly

  // Restore filters from URL hash on mount
  useEffect(() => {
    if (filtersInitialized.current) return

    // Parse hash fragment for filters
    const hash = location.hash
    const match = hash.match(/filters=([^&]+)/)
    const encodedFilters = match ? match[1] : null

    if (encodedFilters) {
      const restored = deserializeFilters(encodedFilters)
      if (restored && restored.length > 0) {
        setFilters(migrateFiltersToCurrentSchema(restored))
        filtersInitialized.current = true
        return
      }
    }

    // Fallback to localStorage if hash doesn't have filters
    const localFilters = loadFiltersFromLocalStorage()
    if (localFilters && localFilters.length > 0) {
      setFilters(localFilters)
    }

    filtersInitialized.current = true
  }, [location.hash, identifier])

  // Update URL hash when filters change
  useEffect(() => {
    if (!filtersInitialized.current || !countByInitialized.current || isUpdatingURL.current) return

    const hashParts: string[] = []

    if (filters.length === 0) {
      try {
        localStorage.removeItem(`filters_${identifier}`)
      } catch (error) {
        console.error('Failed to clear filters from localStorage:', error)
      }
    } else {
      const encodedFilters = serializeFilters(filters)
      hashParts.push(`filters=${encodedFilters}`)
      saveFiltersToLocalStorage(filters)
    }

    if (Object.keys(countBySelections).length === 0) {
      try {
        localStorage.removeItem(`countBy_${identifier}`)
      } catch (error) {
        console.error('Failed to clear countBy selections:', error)
      }
    } else {
      const encodedCountBy = serializeCountBySelections(countBySelections)
      if (encodedCountBy) {
        hashParts.push(`countBy=${encodedCountBy}`)
      }
      saveCountByToLocalStorage(countBySelections)
    }

    const newHash = hashParts.length > 0 ? `#${hashParts.join('&')}` : ''
    const newURL = `${location.pathname}${location.search}${newHash}`

    if (newHash !== location.hash) {
      isUpdatingURL.current = true
      navigate(newURL, { replace: true })
      setTimeout(() => {
        isUpdatingURL.current = false
      }, 0)
    }
  }, [filters, countBySelections, location.pathname, location.search, location.hash, navigate, identifier])

  // Load presets from localStorage on mount
  useEffect(() => {
    const stored = loadPresetsFromLocalStorage()
    setPresets(stored)
  }, [identifier])

  useEffect(() => {
    if (!countByReady) return
    loadDataset()
  }, [id, database, countByReady])

  useEffect(() => {
    if (!intersectionObserverAvailable) return
    const observer = new IntersectionObserver(entries => {
      entries.forEach(entry => {
        const key = dashboardElementKeyMap.current.get(entry.target)
        if (!key || !entry.isIntersecting) return
        setVisibleDashboardKeys(prev => {
          if (prev[key]) return prev
          return { ...prev, [key]: true }
        })
        observer.unobserve(entry.target)
        dashboardElementKeyMap.current.delete(entry.target)
      })
    }, { threshold: 0.1 })
    dashboardObserverRef.current = observer
    Object.entries(dashboardCardRefs.current).forEach(([key, node]) => {
      if (node) {
        dashboardElementKeyMap.current.set(node, key)
        observer.observe(node)
      }
    })
    return () => observer.disconnect()
  }, [intersectionObserverAvailable])

  useEffect(() => {
    if (intersectionObserverAvailable) return
    setVisibleDashboardKeys(prev => {
      let changed = false
      const next = { ...prev }
      dashboardCharts.forEach(chart => {
        const key = getDashboardChartKey(chart)
        if (!next[key]) {
          next[key] = true
          changed = true
        }
      })
      return changed ? next : prev
    })
  }, [dashboardCharts, intersectionObserverAvailable])

  useEffect(() => {
    setVisibleDashboardKeys(prev => {
      const allowed = new Set(dashboardCharts.map(chart => getDashboardChartKey(chart)))
      let changed = false
      const next: Record<string, boolean> = {}
      Object.entries(prev).forEach(([key, value]) => {
        if (allowed.has(key)) {
          next[key] = value
        } else {
          changed = true
        }
      })
      return changed ? next : prev
    })
  }, [dashboardCharts])

  useEffect(() => {
    if (!dataset?.tables) {
      setAncestorOptions({})
      return
    }
    let cancelled = false
    const tablesSnapshot = dataset.tables
    Promise.resolve().then(() => {
      if (cancelled) return
      const options = buildAncestorOptions(tablesSnapshot)
      if (cancelled) return
      setAncestorOptions(options)
      setCountBySelections(prev => normalizeCountBySelections(prev, options))
    })
    return () => {
      cancelled = true
    }
  }, [dataset])

  useEffect(() => {
    // Reload aggregations when filters change
    if (dataset && countByReady) {
      reloadAggregations()
    }
  }, [filters, dataset, countByReady])

  useEffect(() => {
    if (!dataset || !countByInitialized.current || !countByReady) return

    const shouldUseDatabaseAPI = isDatabaseMode || dataset.database_type === 'connected'
    const dbIdentifier = isDatabaseMode ? identifier : dataset.database_name

    dataset.tables.forEach(table => {
      const previousKey = previousCountByRef.current[table.name]
        ? `parent:${previousCountByRef.current[table.name].targetTable}`
        : 'rows'
      const currentSelection = countBySelections[table.name]
      const currentKey = currentSelection ? `parent:${currentSelection.targetTable}` : 'rows'

      if (previousKey !== currentKey) {
        const cachedEntry = aggregations[table.name]?.[currentKey]
        if (isCacheEntryFresh(cachedEntry, currentFiltersKey)) {
          return
        }
        loadTableAggregations(table.id, table.name, {
          useDbAPI: shouldUseDatabaseAPI,
          dbName: dbIdentifier,
          datasetId: dataset.id,
          cacheKey: currentKey
        })
      }
    })

    previousCountByRef.current = countBySelections
  }, [countBySelections, dataset, isDatabaseMode, identifier, aggregations, currentFiltersKey])

  const reloadAggregations = async () => {
    if (!dataset || !countByReady) return
    // Determine if we should use database API based on current dataset
    const shouldUseDatabaseAPI = isDatabaseMode || dataset.database_type === 'connected'
    const dbIdentifier = isDatabaseMode ? identifier : dataset.database_name

    // Send ALL filters to ALL tables and let the backend figure out cross-table filtering
    // The backend will detect which filters are for each table using the tableName property
    for (const table of dataset.tables) {
      const selection = countBySelections[table.name] ?? null
      const cacheKey = getCountByCacheKey(table.name)
      await loadTableAggregations(table.id, table.name, {
        useDbAPI: shouldUseDatabaseAPI,
        dbName: dbIdentifier,
        datasetId: dataset.id,
        tableFilters: filters,
        cacheKey,
        selectionOverride: selection
      })
    }
  }

  useEffect(() => {
    if (!dataset || !countByReady) return
    Object.entries(chartCountOverrides).forEach(([key, cacheKey]) => {
      const [tableName, columnName] = key.split('.')
      if (tableName && columnName) {
        ensureAggregationForCacheKey(tableName, cacheKey)
      }
    })
  }, [chartCountOverrides, dataset, countByReady])

  useEffect(() => {
    if (!dataset || !countByReady) return
    const shouldUseDatabaseAPI = isDatabaseMode || dataset.database_type === 'connected'
    const dbIdentifier = isDatabaseMode ? identifier : dataset.database_name

    dashboardCharts.forEach(chart => {
      const key = getDashboardChartKey(chart)
      if (!visibleDashboardKeys[key]) return
      const table = dataset.tables.find(t => t.name === chart.tableName)
      if (!table) return
      const cacheKey = chart.countByTarget ? `parent:${chart.countByTarget}` : ROW_COUNT_KEY
      const cachedEntry = aggregations[chart.tableName]?.[cacheKey]
      if (isCacheEntryFresh(cachedEntry, currentFiltersKey)) return
      loadTableAggregations(table.id, table.name, {
        useDbAPI: shouldUseDatabaseAPI,
        dbName: dbIdentifier,
        datasetId: dataset.id,
        cacheKey,
        selectionOverride: chart.countByTarget ? { mode: 'parent', targetTable: chart.countByTarget } : null
      })
    })
  }, [dashboardCharts, dataset, countByReady, aggregations, isDatabaseMode, identifier, currentFiltersKey, visibleDashboardKeys])

  const loadDataset = async () => {
    try {
      setLoading(true)
      setAggregations({})
      setBaselineAggregations({})

      // Use different API endpoint based on mode
      const apiPath = isDatabaseMode ? `/databases/${identifier}` : `/datasets/${identifier}`
      const response = await api.get(apiPath)

      const loadedDataset = response.data.dataset
      setDataset(loadedDataset)
      setBaselineAggregations({})
      setCustomRangeInputs({})
      setRangeSelections({})
      setActiveFilterMenu(null)

      // Initialize active tab to dashboard
      setActiveTab('dashboard')

      // Determine if this dataset uses database API
      const shouldUseDatabaseAPI = isDatabaseMode || loadedDataset.database_type === 'connected'
      const dbIdentifier = isDatabaseMode ? identifier : loadedDataset.database_name

      // Load aggregations and column metadata for all tables
      for (const table of loadedDataset.tables) {
        await loadTableAggregations(table.id, table.name, {
          storeBaseline: true,
          useDbAPI: shouldUseDatabaseAPI,
          dbName: dbIdentifier,
          datasetId: loadedDataset.id,
          cacheKey: ROW_COUNT_KEY
        })
        await loadColumnMetadata(table.id, table.name, {
          useDbAPI: shouldUseDatabaseAPI,
          dbName: dbIdentifier,
          datasetId: loadedDataset.id
        })
      }
    } catch (error) {
      console.error('Failed to load dataset:', error)
    } finally {
      setLoading(false)
    }
  }

  const loadTableAggregations = async (
    tableId: string,
    tableName: string,
    options?: {
      storeBaseline?: boolean
      useDbAPI?: boolean
      dbName?: string
      datasetId?: string
      tableFilters?: Filter[]
      cacheKey?: string
      selectionOverride?: CountBySelection | null
    }
  ) => {
    try {
      // Use table-specific filters if provided, otherwise fall back to global filters
      const activeFilters = options?.tableFilters !== undefined ? options.tableFilters : filters
      const requestFiltersKey = buildFiltersKey(activeFilters)
      const params: Record<string, any> = activeFilters.length > 0 ? { filters: JSON.stringify(activeFilters) } : {}
      // Use provided values or fall back to computed values
      const shouldUseDbAPI = options?.useDbAPI !== undefined ? options.useDbAPI : usesDatabaseAPI
      const dbIdentifier = options?.dbName || databaseIdentifier
      const datasetParam = options?.datasetId || datasetIdentifier

      const apiPath = shouldUseDbAPI
        ? `/databases/${dbIdentifier}/tables/${tableId}/aggregations`
        : `/datasets/${identifier}/tables/${tableId}/aggregations`
      if (shouldUseDbAPI && datasetParam) {
        params.datasetId = datasetParam
      }
      const selection = options?.selectionOverride ?? countBySelections[tableName]
      const cacheKey = options?.cacheKey ?? getCountByCacheKey(tableName)
      if (selection?.mode === 'parent') {
        params.countBy = `parent:${selection.targetTable}`
      }

      const response = await api.get(apiPath, { params })
      setAggregations(prev => {
        const previousTableCache = prev[tableName] || {}
        const nextEntry: AggregationCacheEntry = {
          data: response.data.aggregations,
          filtersKey: requestFiltersKey,
          timestamp: Date.now()
        }
        const nextTableCache: Record<string, AggregationCacheEntry> = {
          ...previousTableCache,
          [cacheKey]: nextEntry
        }

        const tableKeys = Object.keys(nextTableCache)
        if (tableKeys.length > CACHE_MAX_ENTRIES_PER_TABLE) {
          const sortedByAge = tableKeys
            .slice()
            .sort((a, b) => nextTableCache[a].timestamp - nextTableCache[b].timestamp)
          while (sortedByAge.length > CACHE_MAX_ENTRIES_PER_TABLE) {
            const oldestKey = sortedByAge.shift()
            if (oldestKey) {
              delete nextTableCache[oldestKey]
            }
          }
        }

        return {
          ...prev,
          [tableName]: nextTableCache
        }
      })
      if (options?.storeBaseline && cacheKey === ROW_COUNT_KEY) {
        setBaselineAggregations(prev => ({ ...prev, [tableName]: response.data.aggregations }))
      }
    } catch (error) {
      console.error('Failed to load table aggregations:', error)
    }
  }

  const loadColumnMetadata = async (
    tableId: string,
    tableName: string,
    options?: { useDbAPI?: boolean; dbName?: string; datasetId?: string }
  ) => {
    try {
      // Use provided values or fall back to computed values
      const shouldUseDbAPI = options?.useDbAPI !== undefined ? options.useDbAPI : usesDatabaseAPI
      const dbIdentifier = options?.dbName || databaseIdentifier
      const datasetParam = options?.datasetId || datasetIdentifier

      const apiPath = shouldUseDbAPI
        ? `/databases/${dbIdentifier}/tables/${tableId}/columns`
        : `/datasets/${identifier}/tables/${tableId}/columns`
      const response = await api.get(apiPath, {
        params: shouldUseDbAPI && datasetParam ? { datasetId: datasetParam } : undefined
      })
      setColumnMetadata(prev => ({ ...prev, [tableName]: response.data.columns }))
    } catch (error) {
      console.error('Failed to load column metadata:', error)
    }
  }

  const getBaselineAggregation = (tableName: string, columnName: string): ColumnAggregation | undefined => {
    const tableAggregations = baselineAggregations[tableName]
    if (!tableAggregations) return undefined
    return tableAggregations.find(agg => agg.column_name === columnName)
  }

  const getAggregation = (tableName: string, columnName: string, overrideKey?: string): ColumnAggregation | undefined => {
    const tableAggregations = getAggregationsForTable(tableName, overrideKey)
    if (!tableAggregations) return undefined
    return tableAggregations.find(agg => agg.column_name === columnName)
  }

  const getColumnMetadata = (tableName: string, columnName: string): ColumnMetadata | undefined => {
    const metadata = columnMetadata[tableName]
    if (!metadata) return undefined
    return metadata.find(col => col.column_name === columnName)
  }

  const getDisplayTitle = (tableName: string, columnName: string): string => {
    const metadata = getColumnMetadata(tableName, columnName)
    return metadata?.display_name || columnName.replace(/_/g, ' ')
  }

  const getCountByCacheKey = (tableName: string, override?: string): string => {
    if (override) return override
    const selection = countBySelections[tableName]
    return selection ? `parent:${selection.targetTable}` : ROW_COUNT_KEY
  }

  const isCacheEntryFresh = (entry?: { timestamp: number; filtersKey?: string }, filtersKey?: string) => {
    if (!entry) return false
    if (filtersKey && entry.filtersKey !== filtersKey) return false
    return Date.now() - entry.timestamp < CACHE_TTL_MS
  }

  const getAggregationsForTable = (tableName: string, override?: string): ColumnAggregation[] | undefined => {
    const cacheKey = getCountByCacheKey(tableName, override)
    const entry = aggregations[tableName]?.[cacheKey]
    if (!isCacheEntryFresh(entry, currentFiltersKey)) return undefined
    return entry?.data
  }

  const getSurvivalEntryKey = (timeColumn: string, statusColumn: string, cacheKey: string) =>
    `${timeColumn}::${statusColumn}::${cacheKey}`

  const getSurvivalCurve = (
    tableName: string,
    timeColumn: string,
    statusColumn: string,
    cacheKey?: string
  ): SurvivalCurvePoint[] | undefined => {
    const key = cacheKey ?? getCountByCacheKey(tableName)
    const entryKey = getSurvivalEntryKey(timeColumn, statusColumn, key)
    const entry = survivalCurves[tableName]?.[entryKey]
    if (!isCacheEntryFresh(entry, currentFiltersKey)) return undefined
    return entry?.data
  }

  const loadSurvivalCurve = async (
    table: Table,
    timeColumn: string,
    statusColumn: string,
    cacheKey?: string
  ) => {
    const key = cacheKey ?? getCountByCacheKey(table.name)
    const entryKey = getSurvivalEntryKey(timeColumn, statusColumn, key)
    const cached = survivalCurves[table.name]?.[entryKey]
    if (isCacheEntryFresh(cached, currentFiltersKey)) return

    const requestKey = `${table.name}|${entryKey}|${currentFiltersKey}`
    if (survivalRequests.current.has(requestKey)) return
    survivalRequests.current.add(requestKey)

    try {
      const params: Record<string, any> = {
        timeColumn,
        statusColumn
      }
      if (filters.length > 0) {
        params.filters = JSON.stringify(filters)
      }
      const selection = countBySelections[table.name]
      if (selection?.mode === 'parent') {
        params.countBy = `parent:${selection.targetTable}`
      }

      const response = await api.get(`/datasets/${identifier}/tables/${table.id}/survival`, { params })
      setSurvivalCurves(prev => {
        const tableCache = prev[table.name] || {}
        const nextEntry: SurvivalCacheEntry = {
          data: response.data.curve || [],
          filtersKey: currentFiltersKey,
          countByKey: key,
          statusColumn,
          timestamp: Date.now()
        }
        return {
          ...prev,
          [table.name]: {
            ...tableCache,
            [entryKey]: nextEntry
          }
        }
      })
    } catch (error) {
      console.error('Failed to load survival curve:', error)
    } finally {
      survivalRequests.current.delete(requestKey)
    }
  }

  const ensureSurvivalCurve = (
    table: Table,
    timeColumn: string,
    statusColumn: string,
    cacheKey?: string
  ) => {
    const key = cacheKey ?? getCountByCacheKey(table.name)
    const entryKey = getSurvivalEntryKey(timeColumn, statusColumn, key)
    const cached = survivalCurves[table.name]?.[entryKey]
    if (isCacheEntryFresh(cached, currentFiltersKey)) return
    loadSurvivalCurve(table, timeColumn, statusColumn, key)
  }

  const findSurvivalStatusColumn = (tableName: string, timeColumn: string): string | null => {
    const metadata = columnMetadata[tableName]
    if (!metadata) return null
    const statusColumns = metadata.filter(col => col.display_type === 'survival_status')
    if (statusColumns.length === 0) return null
    const base = timeColumn.replace(/_(months|days|time)$/i, '')
    const matched = statusColumns.find(col => col.column_name.startsWith(base))
    return (matched || statusColumns[0]).column_name
  }

  const getCountByLabelFromTarget = (tableName: string, target: string | null): string => {
    if (!target) {
      const display = getTableDisplayNameByName(tableName) || tableName
      return `Rows (${display})`
    }
    const option = ancestorOptions[tableName]?.find(opt => opt.targetTable === target)
    if (option) return option.label
    const targetDisplay = getTableDisplayNameByName(target) || target
    return `Unique ${targetDisplay}`
  }

  const getCountByLabelForTable = (tableName: string): string => {
    const selection = countBySelections[tableName]
    return getCountByLabelFromTarget(tableName, selection?.targetTable ?? null)
  }

  const buildAncestorOptions = (tables: Table[]): Record<string, AncestorOption[]> => {
    const tableMap = new Map(tables.map(t => [t.name, t]))
    const options: Record<string, AncestorOption[]> = {}

    tables.forEach(source => {
      const result: AncestorOption[] = []
      const queue: Array<{ tableName: string; path: MetricPathSegment[] }> = [{ tableName: source.name, path: [] }]
      const visited = new Set<string>([source.name])

      while (queue.length > 0) {
        const { tableName, path } = queue.shift()!
        const tableMeta = tableMap.get(tableName)
        if (!tableMeta) continue

        for (const rel of tableMeta.relationships || []) {
          const nextTable = rel.referenced_table
          const segment: MetricPathSegment = { from_table: tableName, via_column: rel.foreign_key, to_table: nextTable }
          const nextPath = [...path, segment]

          if (nextPath.length > MAX_ANCESTOR_DEPTH) {
            continue
          }

          if (!visited.has(nextTable)) {
            queue.push({ tableName: nextTable, path: nextPath })
            visited.add(nextTable)
          }

          if (nextPath.length > 0) {
            const targetMeta = tableMap.get(nextTable)
            const labelParts = nextPath.map(seg => `${seg.from_table}.${seg.via_column}`)
            const label = `${targetMeta?.displayName || targetMeta?.name || nextTable} via ${labelParts.join(' → ')}`
            const key = `parent:${nextTable}`
            result.push({ targetTable: nextTable, label, key, path: nextPath })
          }
        }
      }

      const unique = new Map<string, AncestorOption>()
      result.forEach(option => {
        if (!unique.has(option.targetTable)) {
          unique.set(option.targetTable, option)
        }
      })
      options[source.name] = Array.from(unique.values()).sort((a, b) => a.label.localeCompare(b.label))
    })

    return options
  }

  const normalizeCountBySelections = (
    selections: Record<string, CountBySelection>,
    options: Record<string, AncestorOption[]>
  ): Record<string, CountBySelection> => {
    let changed = false
    const next: Record<string, CountBySelection> = {}

    Object.entries(selections).forEach(([table, selection]) => {
      if (!selection) return
      const tableOptions = options[table]
      if (!tableOptions || tableOptions.length === 0) {
        changed = true
        return
      }
      const match = tableOptions.find(opt => opt.targetTable === selection.targetTable)
      if (!match) {
        changed = true
        return
      }
      next[table] = selection
    })

    return changed ? next : selections
  }

  const getTableDisplayNameByName = (tableName?: string): string | undefined => {
    if (!tableName || !dataset?.tables) return tableName
    const match = dataset.tables.find(t => t.name === tableName)
    return match?.displayName || tableName
  }

  const getMetricLabels = (aggregation?: ColumnAggregation) => {
    if (!aggregation || aggregation.metric_type !== 'parent' || !aggregation.metric_parent_table) {
      return { short: 'rows', long: 'Rows' }
    }
    const parentName = getTableDisplayNameByName(aggregation.metric_parent_table) || aggregation.metric_parent_table
    return {
      short: `unique ${parentName.toLowerCase()}`,
      long: `Unique ${parentName}`
    }
  }

  const formatMetricPath = (aggregation?: ColumnAggregation): string | null => {
    if (!aggregation || aggregation.metric_type !== 'parent' || !aggregation.metric_path || aggregation.metric_path.length === 0) {
      return null
    }
    const target = aggregation.metric_parent_table
    const targetLabel = target ? getTableDisplayNameByName(target) || target : ''
    const chain = aggregation.metric_path
      .map(segment => `${segment.from_table}.${segment.via_column}`)
      .join(' → ')
    return targetLabel ? `${targetLabel} via ${chain}` : chain
  }

  const metricsMatch = (a?: ColumnAggregation, b?: ColumnAggregation) => {
    if (!a || !b) return false
    const typeA = a.metric_type || 'rows'
    const typeB = b.metric_type || 'rows'
    if (typeA !== typeB) return false
    if (typeA === 'parent') {
      const pathA = JSON.stringify(a.metric_path || [])
      const pathB = JSON.stringify(b.metric_path || [])
      return a.metric_parent_table === b.metric_parent_table && pathA === pathB
    }
    return true
  }

  const normalizeFilterValue = (value: string | number | null | undefined): string => {
    if (value === null || value === undefined) return ''
    return String(value)
  }

  const formatRangeValue = (value: number): string => {
    if (!Number.isFinite(value)) return '–'
    if (Number.isInteger(value)) return value.toString()
    return value.toFixed(2)
  }

  const chartKey = (tableName: string, columnName: string) => `${tableName}.${columnName}`

  const getChartOverrideKey = (tableName: string, columnName: string): string | undefined =>
    chartCountOverrides[chartKey(tableName, columnName)]

  const getEffectiveCacheKeyForChart = (tableName: string, columnName: string): string => {
    const override = getChartOverrideKey(tableName, columnName)
    return override ?? getCountByCacheKey(tableName)
  }

  const parseSelectionFromCacheKey = (key?: string): CountBySelection | null => {
    if (!key) return null
    if (key.startsWith('parent:')) {
      return { mode: 'parent', targetTable: key.slice('parent:'.length) }
    }
    return null
  }

  const targetFromCacheKey = (key?: string): string | null => {
    if (!key) return null
    return key.startsWith('parent:') ? key.slice('parent:'.length) : null
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

  const ensureAggregationForCacheKey = (tableName: string, cacheKey: string) => {
    if (!dataset || !countByReady) return
    const table = dataset.tables.find(t => t.name === tableName)
    if (!table) return
    const cachedEntry = aggregations[tableName]?.[cacheKey]
    if (isCacheEntryFresh(cachedEntry, currentFiltersKey)) return

    const shouldUseDatabaseAPI = isDatabaseMode || dataset.database_type === 'connected'
    const dbIdentifier = isDatabaseMode ? identifier : dataset.database_name
    const selectionOverride = parseSelectionFromCacheKey(cacheKey)

    loadTableAggregations(table.id, table.name, {
      useDbAPI: shouldUseDatabaseAPI,
      dbName: dbIdentifier,
      datasetId: dataset.id,
      cacheKey,
      selectionOverride
    })
  }

  const getCountByLabelFromCacheKey = (tableName: string, cacheKey: string): string => {
    if (cacheKey.startsWith('parent:')) {
      const target = cacheKey.slice('parent:'.length)
      return getTableDisplayNameByName(target) || target
    }
    return 'Rows'
  }

  const getCountByOptions = (tableName: string) => [
    {
      value: ROW_COUNT_KEY,
      label: getTableDisplayNameByName(tableName) || tableName
    },
    ...(ancestorOptions[tableName] || []).map(option => ({
      value: option.key,
      label: getTableDisplayNameByName(option.targetTable) || option.targetTable
    }))
  ]

  const getCountIndicatorColor = (tableName: string, cacheKey: string): string => {
    // Color bar represents the data source table
    return getTableColor(tableName)
  }

  const getCountByTableColor = (tableName: string, cacheKey: string): string | null => {
    // Border color represents the count-by table (when different from data source)
    const target = targetFromCacheKey(cacheKey)
    return target ? getTableColor(target) : null
  }

  const renderCountIndicator = ({
    menuKey,
    indicatorColor,
    borderColor,
    label,
    options,
    currentValue,
    onSelect,
    buttonLabel,
    size = 'default'
  }: {
    menuKey: string
    indicatorColor: string
    borderColor?: string | null
    label: string
    options: Array<{ value: string; label: string }>
    currentValue: string
    onSelect: (value: string) => void
    buttonLabel: string
    size?: 'default' | 'large'
  }) => {
    const isOpen = activeCountMenuKey === menuKey
    const hasBorder = borderColor && borderColor !== indicatorColor

    // Size variants
    const dimensions = size === 'large'
      ? { width: hasBorder ? '16px' : '12px', height: '40px' }
      : { width: hasBorder ? '14px' : '10px', height: '22px' }

    return (
      <div style={{ position: 'relative', display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
        <button
          type="button"
          aria-label={buttonLabel}
          title={`${label} (click to change)`}
          onClick={event => {
            event.stopPropagation()
            setActiveCountMenuKey(prev => (prev === menuKey ? null : menuKey))
          }}
          style={{
            width: dimensions.width,
            height: dimensions.height,
            borderRadius: '4px',
            border: hasBorder ? `2px solid ${borderColor}` : 'none',
            background: indicatorColor,
            cursor: 'pointer',
            padding: 0
          }}
        />
        {isOpen && (
          <div
            style={{
              position: 'absolute',
              top: '120%',
              left: 0,
              background: 'white',
              border: '1px solid #ddd',
              borderRadius: '6px',
              boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
              padding: '0.35rem 0.4rem',
              zIndex: 20,
              minWidth: '160px',
              display: 'flex',
              flexDirection: 'column',
              gap: '0.3rem'
            }}
            onClick={event => event.stopPropagation()}
          >
            {options.map(option => {
              const active = option.value === currentValue
              return (
                <button
                  key={option.value}
                  type="button"
                  onClick={() => {
                    onSelect(option.value)
                    setActiveCountMenuKey(null)
                  }}
                  style={{
                    textAlign: 'left',
                    border: active ? '1px solid #1976D2' : '1px solid transparent',
                    borderRadius: '4px',
                    background: active ? '#E3F2FD' : 'transparent',
                    color: '#333',
                    fontSize: '0.72rem',
                    padding: '0.15rem 0.35rem',
                    cursor: 'pointer'
                  }}
                >
                  {option.label}
                </button>
              )
            })}
          </div>
        )}
      </div>
    )
  }

  const renderTableCountIndicator = (tableName: string, columnName: string, cacheKey: string) => {
    const indicatorColor = getCountIndicatorColor(tableName, cacheKey)
    const borderColor = getCountByTableColor(tableName, cacheKey)
    const label = getCountByLabelFromCacheKey(tableName, cacheKey)
    const options = getCountByOptions(tableName)
    const currentValue = cacheKey
    return renderCountIndicator({
      menuKey: `${TABLE_SCOPE_KEY}:${tableName}.${columnName}`,
      indicatorColor,
      borderColor,
      label,
      options,
      currentValue,
      buttonLabel: `Change count-by for ${tableName}.${columnName}`,
      onSelect: value => handleChartCountOverrideChange(tableName, columnName, value)
    })
  }

  const renderDashboardCountIndicator = (
    chartIndex: number,
    tableName: string,
    columnName: string,
    cacheKey: string
  ) => {
    const indicatorColor = getCountIndicatorColor(tableName, cacheKey)
    const borderColor = getCountByTableColor(tableName, cacheKey)
    const label = getCountByLabelFromCacheKey(tableName, cacheKey)
    const options = getCountByOptions(tableName)
    return renderCountIndicator({
      menuKey: `${DASHBOARD_SCOPE_KEY}:${chartIndex}`,
      indicatorColor,
      borderColor,
      label,
      options,
      currentValue: cacheKey,
      buttonLabel: `Change count-by for dashboard chart ${tableName}.${columnName}`,
      onSelect: value => handleDashboardChartCountChange(chartIndex, tableName, value)
    })
  }

  const renderTabCountIndicator = (tableName: string, cacheKey: string) => {
    const indicatorColor = getCountIndicatorColor(tableName, cacheKey)
    const borderColor = getCountByTableColor(tableName, cacheKey)
    const label = getCountByLabelFromCacheKey(tableName, cacheKey)
    const options = getCountByOptions(tableName)
    return renderCountIndicator({
      menuKey: `tab:${tableName}`,
      indicatorColor,
      borderColor,
      label,
      options,
      currentValue: cacheKey,
      buttonLabel: `Change count-by for ${tableName}`,
      onSelect: value => handleCountByChange(tableName, value),
      size: 'large'
    })
  }

  const renderChartHeader = ({
    title,
    tooltip,
    countIndicator,
    actions
  }: {
    title: string
    tooltip?: string
    countIndicator: React.ReactNode
    actions?: React.ReactNode
  }) => (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: '0.35rem',
        marginBottom: '0.4rem'
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', minWidth: 0, flex: 1 }}>
        {countIndicator}
        <h4
          style={{
            margin: 0,
            fontSize: '0.75rem',
            fontWeight: 600,
            cursor: tooltip ? 'help' : 'default',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
            flex: 1
          }}
          title={tooltip}
        >
          {title}
        </h4>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.25rem', flexShrink: 0 }}>
        {actions}
      </div>
    </div>
  )

  const handleChartCountOverrideChange = (tableName: string, columnName: string, value: string) => {
    const defaultKey = getCountByCacheKey(tableName)
    if (value === defaultKey) {
      setChartOverrideForChart(tableName, columnName)
    } else {
      setChartOverrideForChart(tableName, columnName, value)
    }
    ensureAggregationForCacheKey(tableName, value)
  }

  const handleDashboardChartCountChange = (chartIndex: number, tableName: string, value: string) => {
    const nextTarget = value.startsWith('parent:') ? value.slice('parent:'.length) : null
    setDashboardCharts(prev =>
      prev.map((chart, idx) =>
        idx === chartIndex ? { ...chart, countByTarget: nextTarget } : chart
      )
    )
    ensureAggregationForCacheKey(tableName, value)
  }



const rangeKey = (tableName: string, columnName: string, countKey?: string) =>
  `${tableName}.${columnName}:${countKey ?? 'rows'}`

  const rangesEqual = (a: { start: number; end: number }, b: { start: number; end: number }) =>
    Math.abs(a.start - b.start) < 1e-9 && Math.abs(a.end - b.end) < 1e-9

  const getFilterColumn = (filter: Filter): string | undefined => {
    if (filter.column) return filter.column
    if (filter.or && Array.isArray(filter.or) && filter.or.length > 0) {
      const child = filter.or[0] as Filter
      return getFilterColumn(child)
    }
    if (filter.and && Array.isArray(filter.and) && filter.and.length > 0) {
      const child = filter.and[0] as Filter
      return getFilterColumn(child)
    }
    if (filter.not) {
      return getFilterColumn(filter.not)
    }
    return undefined
  }

  /**
   * Determine which table a filter applies to for cache lookups / backend requests.
   *
   * Note: {@link Filter.countByKey} is purely client-side metadata that scopes UI controls.
   * The backend only inspects {@link Filter.tableName} to build JOIN paths, so we never rewrite
   * tableName when users select different ancestor count targets.
   */
  const getFilterTableNameForCacheKey = (filter: Filter): string | undefined => filter.tableName

  const normalizeDashboardCharts = (charts: Array<{ tableName: string; columnName: string; countByTarget?: string | null; addedAt: string }>) => {
    return charts.map(chart => ({
      tableName: chart.tableName,
      columnName: chart.columnName,
      countByTarget: chart.countByTarget ?? null,
      addedAt: chart.addedAt || new Date().toISOString()
    }))
  }

  // Helper: Get all effective filters (direct + propagated) for all tables
  const getAllEffectiveFilters = (): Record<string, { direct: Filter[]; propagated: Filter[] }> => {
    if (!dataset) return {}

    const result: Record<string, { direct: Filter[]; propagated: Filter[] }> = {}

    // Initialize all tables
    for (const table of dataset.tables) {
      result[table.name] = { direct: [], propagated: [] }
    }

    // Group filters by their tableName property
    for (const filter of filters) {
      const filterTableName = getFilterTableNameForCacheKey(filter)
      if (!filterTableName) continue

      // This filter belongs to filterTableName
      // It's "direct" for that table, "propagated" for other tables with relationships
      for (const table of dataset.tables) {
        if (table.name === filterTableName) {
          // Direct filter
          result[table.name].direct.push(filter)
        } else {
          // Check if there's a relationship path between these tables (including transitive)
          const path = findRelationshipPath(table.name, filterTableName, dataset.tables)
          const hasRelationship = path !== null

          if (hasRelationship) {
            // This is a propagated filter for this table
            result[table.name].propagated.push(filter)
          }
        }
      }
    }

    return result
  }

const filterContainsColumn = (filter: Filter, column: string): boolean => {
  if (filter.column === column) return true
  if (filter.or && Array.isArray(filter.or)) {
    return filter.or.some(child => filterContainsColumn(child, column))
  }
  if (filter.and && Array.isArray(filter.and)) {
    return filter.and.some(child => filterContainsColumn(child, column))
  }
  if (filter.not) {
    return filterContainsColumn(filter.not, column)
  }
  return false
}

const getFilterCountKey = (filter: Filter): string => {
  const actual = unwrapNot(filter)
  return actual?.countByKey ?? filter.countByKey ?? ROW_COUNT_KEY
}

  const hasColumnFilter = (column: string, countKey?: string): boolean => {
    const resolvedKey = countKey ?? ROW_COUNT_KEY
    return filters.some(f => {
      const actual = unwrapNot(f)
      if (!actual || !filterContainsColumn(actual, column)) return false
      return getFilterCountKey(f) === resolvedKey
    })
  }

  const removeColumnFilters = (prev: Filter[], column: string, countKey?: string): Filter[] => {
    const resolvedKey = countKey ?? ROW_COUNT_KEY
    return prev.filter(filter => {
      const actualFilter = unwrapNot(filter)
      if (!actualFilter || !filterContainsColumn(actualFilter, column)) return true
      return getFilterCountKey(filter) !== resolvedKey
    })
  }

  const clearColumnFilter = (tableName: string, columnName: string, countKey?: string) => {
    setFilters(prev => removeColumnFilters(prev, columnName, countKey))
    const key = rangeKey(tableName, columnName, countKey)
    setCustomRangeInputs(prev => {
      if (!(key in prev)) return prev
      const { [key]: _removed, ...rest } = prev
      return rest
    })
    setRangeSelections(prev => {
      if (!(key in prev)) return prev
      const { [key]: _removed, ...rest } = prev
      return rest
    })
  }

  const updateColumnRanges = (
    tableName: string,
    columnName: string,
    updater: (ranges: Array<{ start: number; end: number }>) => Array<{ start: number; end: number }>,
    countKey?: string
  ) => {
    const key = rangeKey(tableName, columnName, countKey)
    let nextRanges: Array<{ start: number; end: number }> = []
    setRangeSelections(prev => {
      const prevRanges = prev[key] ?? []
      nextRanges = updater(prevRanges)
      nextRanges = nextRanges
        .slice()
        .sort((a, b) => (a.start - b.start) || (a.end - b.end))
      const unchanged = prevRanges.length === nextRanges.length && prevRanges.every((range, idx) => rangesEqual(range, nextRanges[idx]))
      if (unchanged) {
        nextRanges = prevRanges
        return prev
      }
      const updated = { ...prev }
      if (nextRanges.length === 0) {
        delete updated[key]
      } else {
        updated[key] = nextRanges
      }
      return updated
    })

    setFilters(prev => {
      const without = removeColumnFilters(prev, columnName, countKey)
      if (nextRanges.length === 0) return without
      if (nextRanges.length === 1) {
        const range = nextRanges[0]
        return [
          ...without,
          {
            column: columnName,
            operator: 'between',
            value: [range.start, range.end],
            tableName,
            countByKey: countKey
          }
        ]
      }
      const orFilters = nextRanges.map(range => ({ column: columnName, operator: 'between', value: [range.start, range.end] }))
      return [
        ...without,
        { column: columnName, or: orFilters, tableName, countByKey: countKey }
      ]
    })
  }

  const renderFilterMenu = (
    tableName: string,
    columnName: string,
    categories?: CategoryCount[],
    cacheKeyOverride?: string
  ) => {
    const cacheKey = cacheKeyOverride ?? getEffectiveCacheKeyForChart(tableName, columnName)
    const menuOpen =
      activeFilterMenu?.tableName === tableName &&
      activeFilterMenu.columnName === columnName &&
      activeFilterMenu.countKey === cacheKey
    if (!menuOpen) return null

    const aggregation = getAggregation(tableName, columnName, cacheKey)
    if (!aggregation || !categories || categories.length === 0) return null

    const metricLabels = getMetricLabels(aggregation)
    const pathLabel = formatMetricPath(aggregation)

    const columnHasFilter = hasColumnFilter(columnName, cacheKey)

    // Check if the current filter for this column has NOT wrapper
    const currentFilter = filters.find(f => {
      const actualF = unwrapNot(f)
      if (!actualF || getFilterColumn(actualF) !== columnName) return false
      return getFilterCountKey(f) === cacheKey
    })
    const isNot = !!currentFilter?.not

    // Toggle NOT for this column's filter
    const toggleColumnNot = () => {
      setFilters(prev => {
        const idx = prev.findIndex(f => {
          const actualF = unwrapNot(f)
          if (!actualF || getFilterColumn(actualF) !== columnName) return false
          return getFilterCountKey(f) === cacheKey
        })
        if (idx === -1) return prev

        const updated = [...prev]
        const filter = prev[idx]
        if (filter.not) {
          // Remove NOT wrapper
          updated[idx] = filter.not
        } else {
          // Add NOT wrapper
          updated[idx] = { not: filter }
        }
        return updated
      })
    }

    // Parent counting can double-book entities across wedges, so prefer bars
    if (aggregation.metric_type === 'parent') {
      return null
    }

    return (
      <div
        style={{
          position: 'absolute',
          top: '28px',
          right: 0,
          zIndex: 10,
          background: 'white',
          border: '1px solid #ddd',
          borderRadius: '6px',
          boxShadow: '0 4px 12px rgba(0,0,0,0.12)',
          padding: '0.5rem',
          maxHeight: '200px',
          overflowY: 'auto',
          minWidth: '140px',
          display: 'flex',
          flexDirection: 'column',
          gap: '0.25rem'
        }}
        onClick={(event) => event.stopPropagation()}
      >
        <div style={{ display: 'flex', gap: '0.25rem' }}>
          <button
            onClick={() => clearColumnFilter(tableName, columnName, cacheKey)}
            style={{
              border: 'none',
              background: columnHasFilter ? '#1976D2' : '#eee',
              color: columnHasFilter ? 'white' : '#555',
              borderRadius: '4px',
              padding: '0.25rem 0.5rem',
              fontSize: '0.7rem',
              cursor: columnHasFilter ? 'pointer' : 'default',
              opacity: columnHasFilter ? 1 : 0.6,
              flex: 1
            }}
            disabled={!columnHasFilter}
          >
            Reset
          </button>
          <button
            onClick={toggleColumnNot}
            style={{
              border: 'none',
              background: isNot ? '#333' : '#f0f0f0',
              color: isNot ? 'white' : '#555',
              borderRadius: '4px',
              padding: '0.25rem 0.5rem',
              fontSize: '0.7rem',
              cursor: columnHasFilter ? 'pointer' : 'default',
              opacity: columnHasFilter ? 1 : 0.6,
              fontWeight: isNot ? 'bold' : 'normal'
            }}
            disabled={!columnHasFilter}
            title={isNot ? 'Remove NOT' : 'Add NOT'}
          >
            ¬
          </button>
        </div>
        <div style={{ borderBottom: '1px solid #eee', margin: '0.25rem 0' }} />
        {categories.map(category => {
          const rawValue = normalizeFilterValue(category.value)
          const label = category.display_value ?? (category.value === '' ? '(Empty)' : String(category.value))
          const active = isValueFiltered(columnName, rawValue, cacheKey)

          return (
            <button
              key={`${tableName}-${columnName}-${label}`}
              onMouseDown={event => event.preventDefault()}
              onClick={() => toggleFilter(columnName, rawValue, tableName, cacheKey)}
              style={{
                border: active ? '1px solid #1976D2' : '1px solid #ccc',
                background: active ? '#E3F2FD' : '#fafafa',
                color: active ? '#0D47A1' : '#444',
                borderRadius: '999px',
                padding: '0.25rem 0.5rem',
                fontSize: '0.7rem',
                cursor: 'pointer',
                textAlign: 'left'
              }}
              title={`${label} (${category.count} ${metricLabels.short})`}
            >
              {label}
            </button>
          )
        })}
      </div>
    )
  }

  useEffect(() => {
    if (!activeFilterMenu) return
    const { tableName, columnName, countKey } = activeFilterMenu
    const key = rangeKey(tableName, columnName, countKey)
    const baselineAgg = getBaselineAggregation(tableName, columnName)
    if (!baselineAgg || baselineAgg.display_type !== 'numeric') return
    const stats = baselineAgg.numeric_stats
    if (!stats) return

    const defaultMin = stats.min !== null ? String(stats.min) : ''
    const defaultMax = stats.max !== null ? String(stats.max) : ''

    const selectedRanges = rangeSelections[key] ?? []
    const singleRange = selectedRanges.length === 1 ? selectedRanges[0] : null

    const nextMin = singleRange ? String(singleRange.start) : defaultMin
    const nextMax = singleRange ? String(singleRange.end) : defaultMax

    setCustomRangeInputs(prev => {
      const current = prev[key]
      if (current && current.min === nextMin && current.max === nextMax) {
        return prev
      }
      return { ...prev, [key]: { min: nextMin, max: nextMax } }
    })
  }, [activeFilterMenu, baselineAggregations, rangeSelections])

  useEffect(() => {
    countByInitialized.current = false
    setCountBySelections({})
    setCountByReady(false)
    chartOverridesInitialized.current = false
    setChartCountOverrides({})
    setActiveCountMenuKey(null)
  }, [identifier])

  useEffect(() => {
    const storedOverrides = loadChartOverridesFromLocalStorage()
    setChartCountOverrides(storedOverrides || {})
    chartOverridesInitialized.current = true
  }, [identifier])

  useEffect(() => {
    if (!chartOverridesInitialized.current) return
    saveChartOverridesToLocalStorage(chartCountOverrides)
  }, [chartCountOverrides, identifier])

  useEffect(() => {
    setShowSettingsMenu(false)
  }, [identifier])

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

  useEffect(() => {
    if (!activeCountMenuKey) return
    const handleClick = () => setActiveCountMenuKey(null)
    document.addEventListener('click', handleClick)
    return () => document.removeEventListener('click', handleClick)
  }, [activeCountMenuKey])

  useEffect(() => {
    const storageKey = `${CHART_LABEL_STORAGE_PREFIX}${identifier}`
    const stored = localStorage.getItem(storageKey) ?? localStorage.getItem(`pieLabels_${identifier}`)
    if (stored === 'percent') {
      setShowPercentageLabels(true)
    } else {
      setShowPercentageLabels(false)
    }
  }, [identifier])

  useEffect(() => {
    try {
      localStorage.setItem(`${CHART_LABEL_STORAGE_PREFIX}${identifier}`, showPercentageLabels ? 'percent' : 'count')
    } catch (error) {
      console.error('Failed to persist chart label mode:', error)
    }
  }, [showPercentageLabels, identifier])

  useEffect(() => {
    if (countByInitialized.current) return

    const hash = location.hash
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

    const local = loadCountByFromLocalStorage()
    if (local) {
      setCountBySelections(local)
    } else {
      setCountBySelections({})
    }
    countByInitialized.current = true
    setCountByReady(true)
  }, [identifier, location.hash])

  const handleCustomRangeChange = (
    key: string,
    field: 'min' | 'max',
    value: string
  ) => {
    setCustomRangeInputs(prev => ({
      ...prev,
      [key]: {
        min: field === 'min' ? value : prev[key]?.min ?? '',
        max: field === 'max' ? value : prev[key]?.max ?? ''
      }
    }))
  }

  const applyCustomRange = (tableName: string, columnName: string, countKey?: string) => {
    const key = rangeKey(tableName, columnName, countKey)
    const range = customRangeInputs[key]
    if (!range) return

    const min = range.min.trim()
    const max = range.max.trim()
    if (min === '' || max === '') return

    const minValue = Number(min)
    const maxValue = Number(max)
    if (!Number.isFinite(minValue) || !Number.isFinite(maxValue) || minValue > maxValue) {
      return
    }

    setCustomRangeInputs(prev => ({
      ...prev,
      [key]: { min: String(minValue), max: String(maxValue) }
    }))

    updateColumnRanges(tableName, columnName, prevRanges => {
      const nextRange = { start: minValue, end: maxValue }
      const existingIndex = prevRanges.findIndex(range => rangesEqual(range, nextRange))
      if (existingIndex >= 0) return prevRanges
      return [...prevRanges, nextRange]
    }, countKey)
  }

  const getNiceBinWidth = (range: number, desiredBins: number): number => {
    if (!Number.isFinite(range) || range <= 0) {
      return 1
    }

    const target = range / Math.max(desiredBins, 1)
    if (!Number.isFinite(target) || target <= 0) {
      return range
    }

    const exponent = Math.floor(Math.log10(target))
    const scaled = target / Math.pow(10, exponent)

    let niceScaled: number
    if (scaled <= 1) {
      niceScaled = 1
    } else if (scaled <= 2) {
      niceScaled = 2
    } else if (scaled <= 5) {
      niceScaled = 5
    } else {
      niceScaled = 10
    }

    return niceScaled * Math.pow(10, exponent)
  }

  const getDisplayHistogram = (
    histogram: HistogramBin[] | undefined,
    stats: NumericStats | undefined
  ): HistogramBin[] => {
    if (!histogram || histogram.length === 0) return []
    if (!stats || stats.min === null || stats.max === null) return histogram

    const min = stats.min
    const max = stats.max
    if (!Number.isFinite(min) || !Number.isFinite(max) || max <= min) return histogram

    const originalTotal = histogram.reduce((sum, bin) => sum + bin.count, 0)
    if (!Number.isFinite(originalTotal) || originalTotal === 0) return histogram

    const range = max - min
    const desiredBins = Math.min(Math.max(histogram.length, 1), 60)
    let width = getNiceBinWidth(range, desiredBins)
    if (!Number.isFinite(width) || width <= 0) {
      width = range || 1
    }

    let guard = 0
    while (range / width > 60 && guard < 10) {
      const nextApprox = Math.ceil(range / width / 2)
      width = getNiceBinWidth(range, Math.max(nextApprox, 1))
      if (!Number.isFinite(width) || width <= 0) {
        width = range || 1
        break
      }
      guard += 1
    }

    const start = Math.floor(min / width) * width
    const bucketCount = Math.max(1, Math.ceil((max - start) / width) + 1)
    const buckets: HistogramBin[] = []
    for (let i = 0; i < bucketCount; i++) {
      buckets.push({
        bin_start: start + i * width,
        bin_end: start + (i + 1) * width,
        count: 0,
        percentage: 0
      })
    }

    histogram.forEach(bin => {
      const center = (bin.bin_start + bin.bin_end) / 2
      let index = Math.floor((center - start) / width)
      if (index < 0) index = 0
      if (index >= buckets.length) index = buckets.length - 1
      buckets[index].count += bin.count
    })

    const rebinnedTotal = buckets.reduce((sum, bucket) => sum + bucket.count, 0)
    const denominator = rebinnedTotal > 0 ? rebinnedTotal : originalTotal
    buckets.forEach(bucket => {
      bucket.percentage = denominator > 0 ? (bucket.count / denominator) * 100 : 0
    })

    const filtered = buckets.filter(bucket => bucket.count > 0)
    return filtered.length > 0 ? filtered : histogram
  }

const renderNumericFilterMenu = (
    tableName: string,
    columnName: string,
    histogram?: HistogramBin[],
    stats?: NumericStats,
    cacheKeyOverride?: string
  ) => {
    const cacheKey = cacheKeyOverride ?? getEffectiveCacheKeyForChart(tableName, columnName)
    const menuOpen =
      activeFilterMenu?.tableName === tableName &&
      activeFilterMenu.columnName === columnName &&
      activeFilterMenu.countKey === cacheKey
    if (!menuOpen) return null

    const bins = histogram ?? []
    const aggregation = getAggregation(tableName, columnName, cacheKey)
    if (!aggregation) return null

    const metricLabels = getMetricLabels(aggregation)
    const key = rangeKey(tableName, columnName, cacheKey)
    const range = customRangeInputs[key] || { min: stats && stats.min !== null ? String(stats.min) : '', max: stats && stats.max !== null ? String(stats.max) : '' }
    const columnHasFilter = hasColumnFilter(columnName, cacheKey)
    const selectedRanges = rangeSelections[key] ?? []
    const customRanges = selectedRanges.filter(range => !bins.some(bin => rangesEqual(range, { start: bin.bin_start, end: bin.bin_end })))
    const minDisplay = stats && stats.min !== null ? formatRangeValue(stats.min) : '–'
    const maxDisplay = stats && stats.max !== null ? formatRangeValue(stats.max) : '–'
    const medianDisplay = stats && stats.median !== null ? formatRangeValue(stats.median) : '–'
    const stdDisplay = stats && stats.stddev !== undefined && stats.stddev !== null ? stats.stddev.toFixed(2) : '–'

    const minValue = Number(range.min)
    const maxValue = Number(range.max)
    const hasValidRange =
      range.min.trim() !== '' &&
      range.max.trim() !== '' &&
      Number.isFinite(minValue) &&
      Number.isFinite(maxValue) &&
      minValue <= maxValue

    // Check if the current filter for this column has NOT wrapper
    const currentFilter = filters.find(f => {
      const actualF = unwrapNot(f)
      if (!actualF || getFilterColumn(actualF) !== columnName) return false
      return getFilterCountKey(f) === cacheKey
    })
    const isNot = !!currentFilter?.not

    // Toggle NOT for this column's filter
    const toggleColumnNot = () => {
      setFilters(prev => {
        const idx = prev.findIndex(f => {
          const actualF = unwrapNot(f)
          if (!actualF || getFilterColumn(actualF) !== columnName) return false
          return getFilterCountKey(f) === cacheKey
        })
        if (idx === -1) return prev

        const updated = [...prev]
        const filter = prev[idx]
        if (filter.not) {
          // Remove NOT wrapper
          updated[idx] = filter.not
        } else {
          // Add NOT wrapper
          updated[idx] = { not: filter }
        }
        return updated
      })
    }

    return (
      <div
        style={{
          position: 'absolute',
          top: '28px',
          right: 0,
          zIndex: 10,
          background: 'white',
          border: '1px solid #ddd',
          borderRadius: '6px',
          boxShadow: '0 4px 12px rgba(0,0,0,0.12)',
          padding: '0.5rem',
          maxHeight: '260px',
          overflowY: 'auto',
          minWidth: '180px',
          display: 'flex',
          flexDirection: 'column',
          gap: '0.35rem'
        }}
        onClick={(event) => event.stopPropagation()}
      >
        {stats && (
          <>
            <div style={{ fontSize: '0.7rem', color: '#555', display: 'flex', justifyContent: 'space-between', gap: '0.5rem' }}>
              <span>Min: {minDisplay}</span>
              <span>Max: {maxDisplay}</span>
            </div>
            <div style={{ fontSize: '0.7rem', color: '#555', display: 'flex', justifyContent: 'space-between', gap: '0.5rem' }}>
              <span>Median: {medianDisplay}</span>
              <span>Std: {stdDisplay}</span>
            </div>
          </>
        )}
        {(() => {
          const aggregation = getAggregation(tableName, columnName, cacheKey)
          const metricLabels = getMetricLabels(aggregation)
          const nullCount = aggregation?.null_count ?? 0
          if (nullCount === 0) return null

          const nullActive = isValueFiltered(columnName, '', cacheKey)
          return (
            <>
              <div style={{ borderBottom: '1px solid #eee', margin: '0.25rem 0' }} />
              <button
                onMouseDown={event => event.preventDefault()}
                onClick={() => toggleFilter(columnName, '', tableName, cacheKey)}
                style={{
                  border: nullActive ? '1px solid #1976D2' : '1px solid #ccc',
                  background: nullActive ? '#E3F2FD' : '#fafafa',
                  color: nullActive ? '#0D47A1' : '#444',
                  borderRadius: '999px',
                  padding: '0.25rem 0.5rem',
                  fontSize: '0.7rem',
                  cursor: 'pointer',
                  textAlign: 'left'
                }}
                title={`Null values (${nullCount} ${metricLabels.short})`}
              >
                (Null) — {nullCount} {metricLabels.short}
              </button>
            </>
          )
        })()}
        <div style={{ display: 'flex', gap: '0.25rem' }}>
          <button
            onClick={() => clearColumnFilter(tableName, columnName, cacheKey)}
            style={{
              border: 'none',
              background: columnHasFilter ? '#1976D2' : '#eee',
              color: columnHasFilter ? 'white' : '#555',
              borderRadius: '4px',
              padding: '0.25rem 0.5rem',
              fontSize: '0.7rem',
              cursor: columnHasFilter ? 'pointer' : 'default',
              opacity: columnHasFilter ? 1 : 0.6,
              flex: 1
            }}
            disabled={!columnHasFilter}
          >
            Reset
          </button>
          <button
            onClick={toggleColumnNot}
            style={{
              border: 'none',
              background: isNot ? '#333' : '#f0f0f0',
              color: isNot ? 'white' : '#555',
              borderRadius: '4px',
              padding: '0.25rem 0.5rem',
              fontSize: '0.7rem',
              cursor: columnHasFilter ? 'pointer' : 'default',
              opacity: columnHasFilter ? 1 : 0.6,
              fontWeight: isNot ? 'bold' : 'normal'
            }}
            disabled={!columnHasFilter}
            title={isNot ? 'Remove NOT' : 'Add NOT'}
          >
            ¬
          </button>
        </div>
        {bins.length > 0 && (
          <div style={{ borderTop: '1px solid #eee', paddingTop: '0.25rem', display: 'flex', flexWrap: 'wrap', gap: '0.25rem' }}>
            {bins.map((bin, index) => {
              const active = isRangeFiltered(tableName, columnName, bin.bin_start, bin.bin_end, cacheKey)
              const label = `${formatRangeValue(bin.bin_start)} – ${formatRangeValue(bin.bin_end)}`
              return (
                <button
                  key={`${tableName}-${columnName}-bin-${index}`}
                  onMouseDown={event => event.preventDefault()}
                  onClick={() => toggleRangeFilter(tableName, columnName, bin.bin_start, bin.bin_end, cacheKey)}
                  style={{
                    border: active ? '1px solid #1976D2' : '1px solid #ccc',
                    background: active ? '#E3F2FD' : '#fafafa',
                    color: active ? '#0D47A1' : '#444',
                    borderRadius: '999px',
                    padding: '0.25rem 0.5rem',
                    fontSize: '0.7rem',
                    cursor: 'pointer'
                  }}
                  title={`${label} (${bin.count} ${metricLabels.short})`}
                >
                  {label}
                </button>
              )
            })}
          </div>
        )}
        {customRanges.length > 0 && (
          <div style={{ borderTop: '1px solid #eee', paddingTop: '0.25rem', display: 'flex', flexWrap: 'wrap', gap: '0.25rem' }}>
            {customRanges.map((range, index) => {
              const label = `${formatRangeValue(range.start)} – ${formatRangeValue(range.end)}`
              return (
                <button
                  key={`${tableName}-${columnName}-custom-${index}`}
                  onMouseDown={event => event.preventDefault()}
                  onClick={() => updateColumnRanges(tableName, columnName, prev => prev.filter(r => !rangesEqual(r, range)), cacheKey)}
                  style={{
                    border: '1px solid #1976D2',
                    background: '#E3F2FD',
                    color: '#0D47A1',
                    borderRadius: '999px',
                    padding: '0.25rem 0.5rem',
                    fontSize: '0.7rem',
                    cursor: 'pointer'
                  }}
                  title={`Remove ${label}`}
                >
                  {label} ×
                </button>
              )
            })}
          </div>
        )}
        <div style={{ borderTop: '1px solid #eee', paddingTop: '0.35rem', display: 'flex', flexDirection: 'column', gap: '0.35rem' }}>
          <div style={{ display: 'flex', gap: '0.35rem' }}>
            <label style={{ fontSize: '0.7rem', color: '#555', flex: 1 }}>
              From
              <input
                type="number"
                value={range.min}
                onChange={(event) => handleCustomRangeChange(key, 'min', event.target.value)}
                placeholder={stats?.min !== null && stats?.min !== undefined ? String(stats.min) : ''}
                style={{ width: '100%', padding: '0.2rem 0.3rem', marginTop: '0.15rem' }}
              />
            </label>
            <label style={{ fontSize: '0.7rem', color: '#555', flex: 1 }}>
              To
              <input
                type="number"
                value={range.max}
                onChange={(event) => handleCustomRangeChange(key, 'max', event.target.value)}
                placeholder={stats?.max !== null && stats?.max !== undefined ? String(stats.max) : ''}
                style={{ width: '100%', padding: '0.2rem 0.3rem', marginTop: '0.15rem' }}
              />
            </label>
          </div>
          <button
            onClick={() => applyCustomRange(tableName, columnName, cacheKey)}
            style={{
              border: 'none',
              background: hasValidRange ? '#1976D2' : '#ccc',
              color: 'white',
              borderRadius: '4px',
              padding: '0.3rem 0.5rem',
              fontSize: '0.75rem',
              cursor: hasValidRange ? 'pointer' : 'default'
            }}
            disabled={!hasValidRange}
          >
            Apply
          </button>
        </div>
      </div>
    )
  }

  const toggleFilter = (column: string, value: string | number, tableName?: string, countByKey?: string) => {
    const filterValue = normalizeFilterValue(value)
    const resolvedCountKey = countByKey ?? ROW_COUNT_KEY

    setFilters(prevFilters => {
      const nextFilters = [...prevFilters]

      const existingIndex = nextFilters.findIndex(f => {
        const actualFilter = unwrapNot(f)
        if (!actualFilter || actualFilter.column !== column) return false
        return getFilterCountKey(f) === resolvedCountKey
      })

      const applyMetadata = (target: Filter, source?: Filter) => {
        if (tableName) {
          target.tableName = tableName
        } else if (source?.tableName) {
          target.tableName = source.tableName
        }
        target.countByKey = resolvedCountKey
      }

      if (existingIndex === -1) {
        const newFilter: Filter = { column, operator: 'eq', value: filterValue }
        applyMetadata(newFilter)
        nextFilters.push(newFilter)
        return nextFilters
      }

      const existingWrapped = nextFilters[existingIndex]
      const isNot = !!existingWrapped.not
      const existing = isNot && existingWrapped.not ? existingWrapped.not : existingWrapped

      if (existing.operator === 'eq') {
        const existingValue = normalizeFilterValue(existing.value as string | number)
        if (existingValue === filterValue) {
          nextFilters.splice(existingIndex, 1)
          return nextFilters
        }

        const updatedFilter: Filter = {
          column,
          operator: 'in',
          value: [existingValue, filterValue]
        }
        applyMetadata(updatedFilter, existing)
        nextFilters[existingIndex] = isNot ? { not: updatedFilter } : updatedFilter
        return nextFilters
      }

      if (existing.operator === 'in') {
        const values = Array.isArray(existing.value)
          ? existing.value.map(v => normalizeFilterValue(v as string | number))
          : []
        const matchIndex = values.findIndex(v => v === filterValue)

        if (matchIndex >= 0) {
          values.splice(matchIndex, 1)
        } else {
          values.push(filterValue)
        }

        if (values.length === 0) {
          nextFilters.splice(existingIndex, 1)
        } else if (values.length === 1) {
          const updatedFilter: Filter = { column, operator: 'eq', value: values[0] }
          applyMetadata(updatedFilter, existing)
          nextFilters[existingIndex] = isNot ? { not: updatedFilter } : updatedFilter
        } else {
          const updatedFilter: Filter = { column, operator: 'in', value: values }
          applyMetadata(updatedFilter, existing)
          nextFilters[existingIndex] = isNot ? { not: updatedFilter } : updatedFilter
        }

        return nextFilters
      }

      const updatedFilter: Filter = { column, operator: 'eq', value: filterValue }
      applyMetadata(updatedFilter, existing)
      nextFilters[existingIndex] = isNot ? { not: updatedFilter } : updatedFilter
      return nextFilters
    })
  }

  const clearFilters = () => {
    setFilters([])
    setCustomRangeInputs({})
    setRangeSelections({})
  }

  const isValueFiltered = (column: string, value: string | number, countByKey?: string): boolean => {
    const compareValue = normalizeFilterValue(value)
    const resolvedKey = countByKey ?? ROW_COUNT_KEY
    return filters.some(f => {
      const actualFilter = unwrapNot(f)
      if (!actualFilter || actualFilter.column !== column) return false
      if (getFilterCountKey(f) !== resolvedKey) return false
      if (actualFilter.operator === 'eq') {
        return normalizeFilterValue(actualFilter.value as string | number) === compareValue
      }
      if (actualFilter.operator === 'in' && Array.isArray(actualFilter.value)) {
        return actualFilter.value
          .map(v => normalizeFilterValue(v as string | number))
          .includes(compareValue)
      }
      return false
    })
  }

  const toggleRangeFilter = (tableName: string, column: string, binStart: number, binEnd: number, countKey?: string) => {
    const range = { start: binStart, end: binEnd }
    updateColumnRanges(tableName, column, prevRanges => {
      const existingIndex = prevRanges.findIndex(r => rangesEqual(r, range))
      if (existingIndex >= 0) {
        return [...prevRanges.slice(0, existingIndex), ...prevRanges.slice(existingIndex + 1)]
      }
      return [...prevRanges, range]
    }, countKey)
  }

  const isRangeFiltered = (tableName: string, column: string, binStart: number, binEnd: number, countKey?: string): boolean => {
    const key = rangeKey(tableName, column, countKey)
    const ranges = rangeSelections[key] ?? []
    return ranges.some(range => rangesEqual(range, { start: binStart, end: binEnd }))
  }

  const getTableColor = (tableName: string): string => {
    if (!dataset?.tables) return '#9E9E9E'

    // Assign colors based on table index for more consistent, predictable coloring
    const tableIndex = dataset.tables.findIndex(t => t.name === tableName)
    const colors = ['#2196F3', '#4CAF50', '#FF9800', '#9C27B0', '#F44336', '#00BCD4', '#FFC107', '#E91E63']

    return tableIndex >= 0 ? colors[tableIndex % colors.length] : '#9E9E9E'
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

  const renderTableView = (
    title: string,
    tableName: string,
    field: string,
    tableColor?: string,
    aggregationOverride?: ColumnAggregation,
    cacheKeyOverride?: string,
    countIndicatorOverride?: React.ReactNode
  ) => {
    const cacheKey = cacheKeyOverride ?? getEffectiveCacheKeyForChart(tableName, field)
    const aggregation =
      aggregationOverride && (!cacheKeyOverride || cacheKeyOverride === cacheKey)
        ? aggregationOverride
        : getAggregation(tableName, field, cacheKey)

    const metadata = getColumnMetadata(tableName, field)
    const tableDisplayName = getTableDisplayNameByName(tableName) || tableName
    const tooltipParts = [
      metadata?.display_name || title,
      `ID: ${field}`,
      metadata?.description || '',
      `Table: ${tableDisplayName}`
    ]
    if (aggregation) {
      const pathLabel = formatMetricPath(aggregation)
      if (pathLabel) {
        tooltipParts.push(pathLabel)
      }
    }
    const tooltipText = tooltipParts.filter(Boolean).join('\n')

    const menuOpen =
      activeFilterMenu?.tableName === tableName &&
      activeFilterMenu.columnName === field &&
      activeFilterMenu.countKey === cacheKey
    const columnActive = hasColumnFilter(field, cacheKey)

    const actionButtons = (
      <>
        <button
          type="button"
          onClick={event => {
            event.stopPropagation()
            toggleViewPreference(tableName, field)
          }}
          style={{
            border: 'none',
            background: '#f0f0f0',
            color: '#333',
            borderRadius: '50%',
            width: '20px',
            height: '20px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '0.7rem',
            cursor: 'pointer',
            lineHeight: 1
          }}
          title="Switch to chart view"
        >
          ◐
        </button>
        <button
          type="button"
          onClick={event => {
            event.stopPropagation()
            toggleDashboard(tableName, field)
          }}
          style={{
            border: 'none',
            background: isOnDashboard(tableName, field) ? '#4CAF50' : '#f0f0f0',
            color: isOnDashboard(tableName, field) ? 'white' : '#333',
            borderRadius: '50%',
            width: '20px',
            height: '20px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '0.7rem',
            cursor: 'pointer',
            lineHeight: 1
          }}
          title={isOnDashboard(tableName, field) ? 'Remove from dashboard' : 'Add to dashboard'}
        >
          {isOnDashboard(tableName, field) ? '✓' : '+'}
        </button>
        <button
          type="button"
          onClick={event => {
            event.stopPropagation()
            setActiveFilterMenu(prev =>
              prev && prev.tableName === tableName && prev.columnName === field && prev.countKey === cacheKey
                ? null
                : { tableName, columnName: field, countKey: cacheKey }
            )
          }}
          style={{
            border: 'none',
            background: menuOpen || columnActive ? '#1976D2' : '#f0f0f0',
            color: menuOpen || columnActive ? 'white' : '#333',
            borderRadius: '50%',
            width: '20px',
            height: '20px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '0.75rem',
            cursor: 'pointer',
            lineHeight: 1
          }}
          title="Filter values"
        >
          ⚲
        </button>
      </>
    )

    const countIndicator = countIndicatorOverride ?? renderTableCountIndicator(tableName, field, cacheKey)

    const containerStyle: React.CSSProperties = {
      position: 'relative',
      background: 'white',
      padding: '0.5rem',
      borderRadius: '8px',
      boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
      width: '358px',
      height: '358px',
      boxSizing: 'border-box',
      flexShrink: 0,
      display: 'flex',
      flexDirection: 'column',
      border: tableColor ? `2px solid ${tableColor}20` : undefined
    }

    if (!aggregation || !aggregation.categories || aggregation.categories.length === 0) {
      const message = aggregation ? 'No data for current filters' : 'Loading data…'
      return (
        <div style={containerStyle}>
          {renderChartHeader({
            title: metadata?.display_name || title,
            tooltip: tooltipText,
            countIndicator,
            actions: actionButtons
          })}
          <div
            style={{
              flex: 1,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              color: '#999',
              fontSize: '0.75rem',
              textAlign: 'center',
              padding: '0.5rem'
            }}
          >
            {message}
          </div>
        </div>
      )
    }

    const metricLabels = getMetricLabels(aggregation)
    const baselineAggregation = getBaselineAggregation(tableName, field)
    const categoriesForMenu =
      metricsMatch(baselineAggregation, aggregation) && baselineAggregation?.categories?.length
        ? baselineAggregation.categories
        : aggregation.categories

    const totalRows = aggregation.total_rows ?? aggregation.categories.reduce((sum, cat) => sum + cat.count, 0)

    const tableData = aggregation.categories.map(cat => ({
      category: cat.display_value ?? (cat.value === '' ? '(Empty)' : String(cat.value)),
      rawValue: cat.value,
      count: cat.count,
      percentage: totalRows > 0 ? (cat.count / totalRows) * 100 : 0
    }))

    const sortedData = [...tableData].sort((a, b) => b.count - a.count)
    const showLimit = 100
    const visibleData = sortedData.slice(0, showLimit)
    const hasMore = sortedData.length > showLimit

    return (
      <div style={containerStyle}>
        {renderChartHeader({
          title: metadata?.display_name || title,
          tooltip: tooltipText,
          countIndicator,
          actions: actionButtons
        })}
        <div
          style={{
            overflowY: 'auto',
            flex: 1,
            minHeight: 0,
            fontSize: '0.75rem'
          }}
        >
          <table
            style={{
              width: '100%',
              borderCollapse: 'collapse',
              fontSize: '0.75rem'
            }}
          >
            <thead
              style={{
                position: 'sticky',
                top: 0,
                background: '#f5f5f5',
                borderBottom: '2px solid #ddd'
              }}
            >
              <tr>
                <th
                  style={{
                    padding: '0.4rem 0.5rem',
                    textAlign: 'left',
                    fontWeight: 600
                  }}
                >
                  Category
                </th>
                <th
                  style={{
                    padding: '0.4rem 0.5rem',
                    textAlign: 'right',
                    fontWeight: 600
                  }}
                >
                  Count ↓
                </th>
                <th
                  style={{
                    padding: '0.4rem 0.5rem',
                    textAlign: 'right',
                    fontWeight: 600
                  }}
                >
                  %
                </th>
              </tr>
            </thead>
            <tbody>
              {visibleData.map((row, idx) => {
                const rawValue = normalizeFilterValue(row.rawValue)
                const isFiltered = isValueFiltered(field, rawValue, cacheKey)
                return (
                  <tr
                    key={idx}
                    onClick={() => toggleFilter(field, rawValue, tableName, cacheKey)}
                    style={{
                      cursor: 'pointer',
                      background: isFiltered ? '#E3F2FD' : idx % 2 === 0 ? 'white' : '#fafafa',
                      borderLeft: isFiltered ? '3px solid #1976D2' : '3px solid transparent'
                    }}
                    onMouseEnter={e => {
                      if (!isFiltered) e.currentTarget.style.background = '#f0f0f0'
                    }}
                    onMouseLeave={e => {
                      if (!isFiltered) e.currentTarget.style.background = idx % 2 === 0 ? 'white' : '#fafafa'
                    }}
                  >
                    <td
                      style={{
                        padding: '0.4rem 0.5rem',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                        maxWidth: '180px'
                      }}
                    >
                      {row.category}
                    </td>
                    <td style={{ padding: '0.4rem 0.5rem', textAlign: 'right' }}>
                      {row.count.toLocaleString()}
                    </td>
                    <td style={{ padding: '0.4rem 0.5rem', textAlign: 'right' }}>
                      {row.percentage.toFixed(1)}%
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
          {hasMore && (
            <div style={{ textAlign: 'center', padding: '0.5rem', fontSize: '0.7rem', color: '#666' }}>
              Showing first {showLimit} of {sortedData.length} categories
            </div>
          )}
        </div>
        {renderFilterMenu(tableName, field, categoriesForMenu, cacheKey)}
      </div>
    )
  }

  const renderPieChart = (
    title: string,
    tableName: string,
    field: string,
    tableColor?: string,
    aggregationOverride?: ColumnAggregation,
    cacheKeyOverride?: string,
    countIndicatorOverride?: React.ReactNode
  ) => {
    const cacheKey = cacheKeyOverride ?? getEffectiveCacheKeyForChart(tableName, field)
    const aggregation =
      aggregationOverride && (!cacheKeyOverride || cacheKeyOverride === cacheKey)
        ? aggregationOverride
        : getAggregation(tableName, field, cacheKey)

    const metadata = getColumnMetadata(tableName, field)
    const tableDisplayName = getTableDisplayNameByName(tableName) || tableName
    const tooltipParts = [
      metadata?.display_name || title,
      `ID: ${field}`,
      metadata?.description || '',
      `Table: ${tableDisplayName}`
    ]
    if (aggregation) {
      const pathLabel = formatMetricPath(aggregation)
      if (pathLabel) tooltipParts.push(pathLabel)
    }
    const tooltipText = tooltipParts.filter(Boolean).join('\n')

    const menuOpen =
      activeFilterMenu?.tableName === tableName &&
      activeFilterMenu.columnName === field &&
      activeFilterMenu.countKey === cacheKey
    const columnActive = hasColumnFilter(field, cacheKey)
    const actionButtons = (
      <>
        <button
          type="button"
          onClick={event => {
            event.stopPropagation()
            toggleViewPreference(tableName, field)
          }}
          style={{
            border: 'none',
            background: '#f0f0f0',
            color: '#333',
            borderRadius: '50%',
            width: '20px',
            height: '20px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '0.7rem',
            cursor: 'pointer',
            lineHeight: 1
          }}
          title="Switch to table view"
        >
          ⊞
        </button>
        <button
          type="button"
          onClick={event => {
            event.stopPropagation()
            toggleDashboard(tableName, field)
          }}
          style={{
            border: 'none',
            background: isOnDashboard(tableName, field) ? '#4CAF50' : '#f0f0f0',
            color: isOnDashboard(tableName, field) ? 'white' : '#333',
            borderRadius: '50%',
            width: '20px',
            height: '20px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '0.7rem',
            cursor: 'pointer',
            lineHeight: 1
          }}
          title={isOnDashboard(tableName, field) ? 'Remove from dashboard' : 'Add to dashboard'}
        >
          {isOnDashboard(tableName, field) ? '✓' : '+'}
        </button>
        <button
          type="button"
          onClick={event => {
            event.stopPropagation()
            setActiveFilterMenu(prev =>
              prev && prev.tableName === tableName && prev.columnName === field && prev.countKey === cacheKey
                ? null
                : { tableName, columnName: field, countKey: cacheKey }
            )
          }}
          style={{
            border: 'none',
            background: menuOpen || columnActive ? '#1976D2' : '#f0f0f0',
            color: menuOpen || columnActive ? 'white' : '#333',
            borderRadius: '50%',
            width: '20px',
            height: '20px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '0.75rem',
            cursor: 'pointer',
            lineHeight: 1
          }}
          title="Filter values"
        >
          ⚲
        </button>
      </>
    )

    const containerStyle: React.CSSProperties = {
      position: 'relative',
      background: 'white',
      padding: '0.5rem',
      borderRadius: '8px',
      boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
      width: '175px',
      minHeight: '175px',
      boxSizing: 'border-box',
      flexShrink: 0,
      display: 'flex',
      flexDirection: 'column',
      border: tableColor ? `2px solid ${tableColor}20` : undefined
    }

    const countIndicator = countIndicatorOverride ?? renderTableCountIndicator(tableName, field, cacheKey)

    if (!aggregation || !aggregation.categories || aggregation.categories.length === 0) {
      const message = aggregation ? 'No data for current filters' : 'Loading data…'
      return (
        <div style={containerStyle}>
          {renderChartHeader({
            title: metadata?.display_name || title,
            tooltip: tooltipText,
            countIndicator,
            actions: actionButtons
          })}
          <div
            style={{
              flex: 1,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              color: '#999',
              fontSize: '0.75rem',
              textAlign: 'center',
              padding: '0.5rem'
            }}
          >
            {message}
          </div>
        </div>
      )
    }

    const metricLabels = getMetricLabels(aggregation)
    const pathLabel = formatMetricPath(aggregation)
    const labels = aggregation.categories.map(c => c.display_value ?? (c.value === '' ? '(Empty)' : String(c.value)))
    const values = aggregation.categories.map(c => c.count)
    const filterValues = aggregation.categories.map(c => normalizeFilterValue(c.value))
    const shouldShowPiePercentages = showPercentageLabels

    const baselineAggregation = getBaselineAggregation(tableName, field)
    const categoriesForMenu =
      metricsMatch(baselineAggregation, aggregation) && baselineAggregation?.categories?.length
        ? baselineAggregation.categories
        : aggregation.categories

    const totalCount = aggregation.total_rows ?? values.reduce((sum, val) => sum + val, 0)
    const percentTexts = values.map(val =>
      totalCount > 0 ? `${((val / totalCount) * 100).toFixed(1)}%` : '0%'
    )
    const countTexts = values.map(val => val.toLocaleString())

    return (
      <div style={containerStyle}>
        {renderChartHeader({
          title: metadata?.display_name || title,
          tooltip: tooltipText,
          countIndicator,
          actions: actionButtons
        })}
        <Plot
          data={[{
            type: 'pie',
            labels,
            values,
            textposition: 'inside',
            insidetextorientation: 'radial',
            marker: {
              colors: filterValues.map(value =>
                isValueFiltered(field, value, cacheKey) ? '#1976D2' : undefined
              ),
              line: {
                color: filterValues.map(value =>
                  isValueFiltered(field, value, cacheKey) ? '#000' : undefined
                ),
                width: filterValues.map(value =>
                  isValueFiltered(field, value, cacheKey) ? 2 : 0
                )
              }
            },
            textfont: { size: 9 },
            hovertemplate: `${['%{label}', `Count (${metricLabels.short}): %{value}`, 'Percent of total: %{customdata}']
              .concat(pathLabel ? [pathLabel] : [])
              .join('<br>')}<extra></extra>`,
            customdata: percentTexts,
            textinfo: shouldShowPiePercentages ? 'label+text' : 'label+value',
            text: shouldShowPiePercentages ? percentTexts : countTexts
          }]}
          layout={{
            height: 135,
            margin: { t: 5, b: 5, l: 5, r: 5 },
            showlegend: false,
            paper_bgcolor: 'transparent',
            plot_bgcolor: 'transparent',
            dragmode: false
          }}
          config={{
            displayModeBar: false,
            responsive: true,
            staticPlot: false
          }}
          style={{ width: '165px', height: '135px', cursor: 'pointer' }}
          onClick={(event: PlotMouseEvent) => {
            const point = event.points?.[0]
            if (!point) return

            const index = point.pointNumber ?? point.pointIndex
            if (typeof index === 'number' && index >= 0 && index < filterValues.length) {
              const clickedValue = filterValues[index]
              toggleFilter(field, clickedValue, tableName, cacheKey)
            }
          }}
        />
        {renderFilterMenu(tableName, field, categoriesForMenu, cacheKey)}
      </div>
    )
  }

  const renderBarChart = (
    title: string,
    tableName: string,
    field: string,
    tableColor?: string,
    aggregationOverride?: ColumnAggregation,
    cacheKeyOverride?: string,
    countIndicatorOverride?: React.ReactNode
  ) => {
    const cacheKey = cacheKeyOverride ?? getEffectiveCacheKeyForChart(tableName, field)
    const aggregation =
      aggregationOverride && (!cacheKeyOverride || cacheKeyOverride === cacheKey)
        ? aggregationOverride
        : getAggregation(tableName, field, cacheKey)

    const metadata = getColumnMetadata(tableName, field)
    const tableDisplayName = getTableDisplayNameByName(tableName) || tableName
    const tooltipParts = [
      metadata?.display_name || title,
      `ID: ${field}`,
      metadata?.description || '',
      `Table: ${tableDisplayName}`
    ]
    if (aggregation) {
      const pathLabel = formatMetricPath(aggregation)
      if (pathLabel) tooltipParts.push(pathLabel)
    }
    const tooltipText = tooltipParts.filter(Boolean).join('\n')

    const menuOpen =
      activeFilterMenu?.tableName === tableName &&
      activeFilterMenu.columnName === field &&
      activeFilterMenu.countKey === cacheKey
    const columnActive = hasColumnFilter(field, cacheKey)

    const actionButtons = (
      <>
        <button
          type="button"
          onClick={event => {
            event.stopPropagation()
            toggleViewPreference(tableName, field)
          }}
          style={{
            border: 'none',
            background: '#f0f0f0',
            color: '#333',
            borderRadius: '50%',
            width: '20px',
            height: '20px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '0.7rem',
            cursor: 'pointer',
            lineHeight: 1
          }}
          title="Switch to table view"
        >
          ⊞
        </button>
        <button
          type="button"
          onClick={event => {
            event.stopPropagation()
            toggleDashboard(tableName, field)
          }}
          style={{
            border: 'none',
            background: isOnDashboard(tableName, field) ? '#4CAF50' : '#f0f0f0',
            color: isOnDashboard(tableName, field) ? 'white' : '#333',
            borderRadius: '50%',
            width: '20px',
            height: '20px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '0.7rem',
            cursor: 'pointer',
            lineHeight: 1
          }}
          title={isOnDashboard(tableName, field) ? 'Remove from dashboard' : 'Add to dashboard'}
        >
          {isOnDashboard(tableName, field) ? '✓' : '+'}
        </button>
        <button
          type="button"
          onClick={event => {
            event.stopPropagation()
            setActiveFilterMenu(prev =>
              prev && prev.tableName === tableName && prev.columnName === field && prev.countKey === cacheKey
                ? null
                : { tableName, columnName: field, countKey: cacheKey }
            )
          }}
          style={{
            border: 'none',
            background: menuOpen || columnActive ? '#1976D2' : '#f0f0f0',
            color: menuOpen || columnActive ? 'white' : '#333',
            borderRadius: '50%',
            width: '20px',
            height: '20px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '0.75rem',
            cursor: 'pointer',
            lineHeight: 1
          }}
          title="Filter values"
        >
          ⚲
        </button>
      </>
    )

    const containerStyle: React.CSSProperties = {
      position: 'relative',
      background: 'white',
      padding: '0.5rem',
      borderRadius: '8px',
      boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
      width: '358px',
      minHeight: '175px',
      boxSizing: 'border-box',
      flexShrink: 0,
      display: 'flex',
      flexDirection: 'column',
      border: tableColor ? `2px solid ${tableColor}20` : undefined
    }

    const countIndicator = countIndicatorOverride ?? renderTableCountIndicator(tableName, field, cacheKey)

    if (!aggregation || !aggregation.categories || aggregation.categories.length === 0) {
      const message = aggregation ? 'No data for current filters' : 'Loading data…'
      return (
        <div style={containerStyle}>
          {renderChartHeader({
            title: metadata?.display_name || title,
            tooltip: tooltipText,
            countIndicator,
            actions: actionButtons
          })}
          <div
            style={{
              flex: 1,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              color: '#999',
              fontSize: '0.75rem',
              textAlign: 'center',
              padding: '0.5rem'
            }}
          >
            {message}
          </div>
        </div>
      )
    }

    const metricLabels = getMetricLabels(aggregation)
    const pathLabel = formatMetricPath(aggregation)
    const labels = aggregation.categories.map(c => c.display_value ?? (c.value === '' ? '(Empty)' : String(c.value)))
    const values = aggregation.categories.map(c => c.count)
    const filterValues = aggregation.categories.map(c => normalizeFilterValue(c.value))
    const totalCount = aggregation.total_rows ?? values.reduce((sum, val) => sum + val, 0)
    const percentTexts = values.map(val =>
      totalCount > 0 ? `${((val / totalCount) * 100).toFixed(1)}%` : '0%'
    )

    const baselineAggregation = getBaselineAggregation(tableName, field)
    const categoriesForMenu =
      metricsMatch(baselineAggregation, aggregation) && baselineAggregation?.categories?.length
        ? baselineAggregation.categories
        : aggregation.categories

    return (
      <div style={containerStyle}>
        {renderChartHeader({
          title: metadata?.display_name || title,
          tooltip: tooltipText,
          countIndicator,
          actions: actionButtons
        })}
        <Plot
          data={[{
            type: 'bar',
            x: labels,
            y: values,
            marker: {
              color: filterValues.map(value =>
                isValueFiltered(field, value, cacheKey) ? '#1976D2' : '#2196F3'
              ),
              line: {
                color: filterValues.map(value =>
                  isValueFiltered(field, value, cacheKey) ? '#000' : undefined
                ),
                width: filterValues.map(value =>
                  isValueFiltered(field, value, cacheKey) ? 2 : 0
                )
              }
            },
            hovertemplate: `${['%{x}', `Count (${metricLabels.short}): %{y}`, 'Percent of total: %{text}']
              .concat(pathLabel ? [pathLabel] : [])
              .join('<br>')}<extra></extra>`,
            text: percentTexts,
            textposition: 'auto'
          }]}
          layout={{
            height: 135,
            margin: { t: 5, b: 40, l: 30, r: 5 },
            xaxis: { tickangle: -45, automargin: true, tickfont: { size: 9 } },
            yaxis: { title: metricLabels.long, automargin: true, tickfont: { size: 9 }, titlefont: { size: 10 } },
            paper_bgcolor: 'transparent',
            plot_bgcolor: 'transparent',
            dragmode: 'select',
            selectdirection: 'h'
          }}
          config={{
            displayModeBar: false,
            responsive: true,
            staticPlot: false,
            scrollZoom: false
          }}
          style={{ width: '348px', height: '135px', cursor: 'pointer' }}
          onClick={(event: PlotMouseEvent) => {
            const point = event.points?.[0]
            if (!point) return

            const pointIndex = point.pointIndex
            if (typeof pointIndex === 'number' && pointIndex >= 0 && pointIndex < filterValues.length) {
              const clickedValue = filterValues[pointIndex]
              toggleFilter(field, clickedValue, tableName, cacheKey)
            }
          }}
          onSelected={(event: PlotSelectionEvent) => {
            if (!event?.points || event.points.length === 0) return
            const selectedValues = event.points
              .map(p => p.pointIndex)
              .filter((idx): idx is number => typeof idx === 'number' && idx >= 0 && idx < filterValues.length)
              .map(idx => filterValues[idx])

            if (selectedValues.length > 0) {
              setFilters(prev => [
                ...removeColumnFilters(prev, field, cacheKey),
                { column: field, operator: 'in', value: selectedValues, tableName, countByKey: cacheKey }
              ])
            }
          }}
        />
        {renderFilterMenu(tableName, field, categoriesForMenu, cacheKey)}
      </div>
    )
  }

  const renderHistogram = (
    title: string,
    tableName: string,
    field: string,
    tableColor?: string,
    aggregationOverride?: ColumnAggregation,
    cacheKeyOverride?: string,
    countIndicatorOverride?: React.ReactNode,
    extraActions?: React.ReactNode
  ) => {
    const cacheKey = cacheKeyOverride ?? getEffectiveCacheKeyForChart(tableName, field)
    const aggregation =
      aggregationOverride && (!cacheKeyOverride || cacheKeyOverride === cacheKey)
        ? aggregationOverride
        : getAggregation(tableName, field, cacheKey)
    if (!aggregation?.numeric_stats) return null

    const rawHistogram = aggregation.histogram ?? []
    if (rawHistogram.length === 0) return null

    const metricLabels = getMetricLabels(aggregation)
    const pathLabel = formatMetricPath(aggregation)
    const metadata = getColumnMetadata(tableName, field)
    const tableDisplayName = getTableDisplayNameByName(tableName) || tableName

    const statsText = [
      `Mean: ${aggregation.numeric_stats.mean !== null ? aggregation.numeric_stats.mean.toFixed(2) : 'N/A'}`,
      `Median: ${aggregation.numeric_stats.median !== null ? aggregation.numeric_stats.median.toFixed(2) : 'N/A'}`,
      `Range: [${aggregation.numeric_stats.min !== null ? aggregation.numeric_stats.min.toFixed(2) : 'N/A'}, ${aggregation.numeric_stats.max !== null ? aggregation.numeric_stats.max.toFixed(2) : 'N/A'}]`
    ].join(' | ')

    const tooltipParts = [
      metadata?.display_name || title,
      `ID: ${field}`,
      metadata?.description || '',
      `Table: ${tableDisplayName}`
    ]
    if (pathLabel) tooltipParts.push(pathLabel)
    tooltipParts.push('', statsText)
    const tooltipText = tooltipParts.filter(Boolean).join('\n')

    const baselineAggregation = getBaselineAggregation(tableName, field)
    const histogramMatches = metricsMatch(baselineAggregation, aggregation)
    const menuHistogram = histogramMatches && baselineAggregation?.histogram?.length
      ? baselineAggregation.histogram
      : rawHistogram
    const menuStats = histogramMatches && baselineAggregation?.numeric_stats
      ? baselineAggregation.numeric_stats
      : aggregation.numeric_stats

    const menuOpen =
      activeFilterMenu?.tableName === tableName &&
      activeFilterMenu.columnName === field &&
      activeFilterMenu.countKey === cacheKey
    const columnActive = hasColumnFilter(field, cacheKey)

    const actionButtons = (
      <>
        {extraActions}
        <button
          type="button"
          onClick={event => {
            event.stopPropagation()
            toggleDashboard(tableName, field)
          }}
          style={{
            border: 'none',
            background: isOnDashboard(tableName, field) ? '#4CAF50' : '#f0f0f0',
            color: isOnDashboard(tableName, field) ? 'white' : '#333',
            borderRadius: '50%',
            width: '20px',
            height: '20px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '0.7rem',
            cursor: 'pointer',
            lineHeight: 1
          }}
          title={isOnDashboard(tableName, field) ? 'Remove from dashboard' : 'Add to dashboard'}
        >
          {isOnDashboard(tableName, field) ? '✓' : '+'}
        </button>
        <button
          type="button"
          onClick={event => {
            event.stopPropagation()
            setActiveFilterMenu(prev =>
              prev && prev.tableName === tableName && prev.columnName === field && prev.countKey === cacheKey
                ? null
                : { tableName, columnName: field, countKey: cacheKey }
            )
          }}
          style={{
            border: 'none',
            background: menuOpen || columnActive ? '#1976D2' : '#f0f0f0',
            color: menuOpen || columnActive ? 'white' : '#333',
            borderRadius: '50%',
            width: '20px',
            height: '20px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '0.75rem',
            cursor: 'pointer',
            lineHeight: 1
          }}
          title="Filter values"
        >
          ⚲
        </button>
      </>
    )

    const displayHistogram = getDisplayHistogram(menuHistogram, menuStats)
    const binsForPlot = displayHistogram.length > 0 ? displayHistogram : menuHistogram

    const xValues = binsForPlot.map(bin => (bin.bin_start + bin.bin_end) / 2)
    const yValues = binsForPlot.map(baselineBin => {
      let totalCount = 0
      rawHistogram.forEach(filteredBin => {
        const overlapStart = Math.max(baselineBin.bin_start, filteredBin.bin_start)
        const overlapEnd = Math.min(baselineBin.bin_end, filteredBin.bin_end)
        if (overlapStart < overlapEnd) {
          const filteredBinWidth = filteredBin.bin_end - filteredBin.bin_start
          const overlapWidth = overlapEnd - overlapStart
          const overlapFraction = overlapWidth / filteredBinWidth
          totalCount += filteredBin.count * overlapFraction
        }
      })
      return totalCount
    })
    const totalMetricCount = aggregation.total_rows ?? rawHistogram.reduce((sum, bin) => sum + bin.count, 0)
    const sumY = yValues.reduce((sum, val) => sum + val, 0)
    const scalingFactor = sumY > 0 && totalMetricCount > 0 ? totalMetricCount / sumY : 1
    const adjustedYValues = scalingFactor < 1 ? yValues.map(val => val * scalingFactor) : yValues
    const roundedYValues = adjustedYValues.map(val => Math.max(0, val))
    const binWidth = binsForPlot[0] ? binsForPlot[0].bin_end - binsForPlot[0].bin_start : 1
    const totalCount = totalMetricCount > 0 ? totalMetricCount : roundedYValues.reduce((sum, val) => sum + val, 0)
    const percentTexts = roundedYValues.map(val =>
      totalCount > 0 ? `${((val / totalCount) * 100).toFixed(1)}%` : '0%'
    )
    const countTexts = roundedYValues.map(val =>
      val >= 1000 ? Math.round(val).toLocaleString() : Math.round(val).toString()
    )

    const containerStyle: React.CSSProperties = {
      position: 'relative',
      background: 'white',
      padding: '0.5rem',
      borderRadius: '8px',
      boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
      width: '358px',
      minHeight: '175px',
      boxSizing: 'border-box',
      flexShrink: 0,
      display: 'flex',
      flexDirection: 'column',
      border: tableColor ? `2px solid ${tableColor}20` : undefined
    }

    const countIndicator = countIndicatorOverride ?? renderTableCountIndicator(tableName, field, cacheKey)

    return (
      <div style={containerStyle}>
        {renderChartHeader({
          title: metadata?.display_name || title,
          tooltip: tooltipText,
          countIndicator,
          actions: actionButtons
        })}
        <Plot
          data={[{
            type: 'bar',
            x: xValues,
            y: roundedYValues,
            width: binWidth * 0.9,
            marker: {
              color: binsForPlot.map(bin =>
                isRangeFiltered(tableName, field, bin.bin_start, bin.bin_end, cacheKey) ? '#2E7D32' : '#4CAF50'
              ),
              line: {
                color: binsForPlot.map(bin =>
                  isRangeFiltered(tableName, field, bin.bin_start, bin.bin_end, cacheKey) ? '#000' : undefined
                ),
                width: binsForPlot.map(bin =>
                  isRangeFiltered(tableName, field, bin.bin_start, bin.bin_end, cacheKey) ? 2 : 0
                )
              }
            },
            hovertemplate: `${[
              'Range: [%{customdata[0]:.2f}, %{customdata[1]:.2f}]',
              `Count (${metricLabels.short}): %{y}`,
              'Percent of total: %{customdata[2]}'
            ]
              .concat(pathLabel ? [pathLabel] : [])
              .join('<br>')}<extra></extra>`,
            customdata: binsForPlot.map((bin, idx) => [bin.bin_start, bin.bin_end, percentTexts[idx]]),
            text: showPercentageLabels ? percentTexts : countTexts,
            textposition: 'auto'
          }]}
          layout={{
            height: 135,
            margin: { t: 5, b: 30, l: 30, r: 5 },
            xaxis: { title: field, automargin: true, tickfont: { size: 9 }, titlefont: { size: 10 } },
            yaxis: { title: metricLabels.long, automargin: true, tickfont: { size: 9 }, titlefont: { size: 10 } },
            paper_bgcolor: 'transparent',
            plot_bgcolor: 'transparent',
            bargap: 0.1,
            dragmode: 'select',
            selectdirection: 'h'
          }}
          config={{
            displayModeBar: false,
            responsive: true,
            staticPlot: false,
            scrollZoom: false
          }}
          style={{ width: '348px', height: '135px', cursor: 'pointer' }}
          onClick={(event: PlotMouseEvent) => {
            const point = event.points?.[0]
            if (!point) return
            const pointIndex = point.pointIndex
            if (typeof pointIndex === 'number' && pointIndex >= 0 && pointIndex < binsForPlot.length) {
              const bin = binsForPlot[pointIndex]
              toggleRangeFilter(tableName, field, bin.bin_start, bin.bin_end, cacheKey)
            }
          }}
          onSelected={(event: PlotSelectionEvent) => {
            const rangeX = event?.range?.x
            if (!rangeX || rangeX.length < 2) return
            const [minX, maxX] = rangeX
            updateColumnRanges(tableName, field, prev => {
              const nextRange = { start: minX, end: maxX }
              const existingIndex = prev.findIndex(range => rangesEqual(range, nextRange))
              if (existingIndex >= 0) return prev
              return [...prev, nextRange]
            }, cacheKey)
          }}
        />
        {renderNumericFilterMenu(tableName, field, displayHistogram, menuStats, cacheKey)}
      </div>
    )
  }

  const renderSurvivalChart = (
    title: string,
    tableName: string,
    field: string,
    tableColor?: string,
    aggregationOverride?: ColumnAggregation,
    cacheKeyOverride?: string,
    countIndicatorOverride?: React.ReactNode,
    extraActions?: React.ReactNode,
    showHistogram: boolean = true
  ) => {
    const cacheKey = cacheKeyOverride ?? getEffectiveCacheKeyForChart(tableName, field)
    const aggregation =
      aggregationOverride && (!cacheKeyOverride || cacheKeyOverride === cacheKey)
        ? aggregationOverride
        : getAggregation(tableName, field, cacheKey)
    const table = dataset?.tables.find(t => t.name === tableName)
    if (!aggregation || !table) return null

    const statusColumn = findSurvivalStatusColumn(tableName, field)
    if (statusColumn) {
      ensureSurvivalCurve(table, field, statusColumn, cacheKey)
    }
    const curve = statusColumn ? getSurvivalCurve(tableName, field, statusColumn, cacheKey) : undefined

    const metadata = getColumnMetadata(tableName, field)
    const tableDisplayName = getTableDisplayNameByName(tableName) || tableName
    const tooltipParts = [
      metadata?.display_name || title,
      `ID: ${field}`,
      metadata?.description || '',
      `Table: ${tableDisplayName}`
    ]
    const pathLabel = formatMetricPath(aggregation)
    if (pathLabel) tooltipParts.push(pathLabel)
    const tooltipText = tooltipParts.filter(Boolean).join('\n')

    const menuOpen =
      activeFilterMenu?.tableName === tableName &&
      activeFilterMenu.columnName === field &&
      activeFilterMenu.countKey === cacheKey
    const columnActive = hasColumnFilter(field, cacheKey)

    const actionButtons = (
      <>
        {extraActions}
        <button
          type="button"
          onClick={event => {
            event.stopPropagation()
            toggleDashboard(tableName, field)
          }}
          style={{
            border: 'none',
            background: isOnDashboard(tableName, field) ? '#4CAF50' : '#f0f0f0',
            color: isOnDashboard(tableName, field) ? 'white' : '#333',
            borderRadius: '50%',
            width: '20px',
            height: '20px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '0.7rem',
            cursor: 'pointer',
            lineHeight: 1
          }}
          title={isOnDashboard(tableName, field) ? 'Remove from dashboard' : 'Add to dashboard'}
        >
          {isOnDashboard(tableName, field) ? '✓' : '+'}
        </button>
        <button
          type="button"
          onClick={event => {
            event.stopPropagation()
            setActiveFilterMenu(prev =>
              prev && prev.tableName === tableName && prev.columnName === field && prev.countKey === cacheKey
                ? null
                : { tableName, columnName: field, countKey: cacheKey }
            )
          }}
          style={{
            border: 'none',
            background: menuOpen || columnActive ? '#1976D2' : '#f0f0f0',
            color: menuOpen || columnActive ? 'white' : '#333',
            borderRadius: '50%',
            width: '20px',
            height: '20px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '0.75rem',
            cursor: 'pointer',
            lineHeight: 1
          }}
          title="Filter values"
        >
          ⚲
        </button>
      </>
    )

    const countIndicator = countIndicatorOverride ?? renderTableCountIndicator(tableName, field, cacheKey)
    const displayHistogram = getDisplayHistogram(aggregation.histogram ?? [], aggregation.numeric_stats)
    const binsForPlot = displayHistogram.length > 0 ? displayHistogram : aggregation.histogram || []
    const histogramPlot =
      binsForPlot.length > 0 ? (
        <Plot
          data={[{
            type: 'bar',
            x: binsForPlot.map(bin => (bin.bin_start + bin.bin_end) / 2),
            y: binsForPlot.map(bin => bin.count),
            width: binsForPlot.map(bin => bin.bin_end - bin.bin_start),
            marker: { color: tableColor || '#2196F3', opacity: 0.7 }
          }]}
          layout={{
            height: 180,
            margin: { t: 20, b: 40, l: 50, r: 10 },
            xaxis: { title: metadata?.display_name || title, tickfont: { size: 9 } },
            yaxis: { title: getMetricLabels(aggregation).long, tickfont: { size: 9 } },
            paper_bgcolor: 'transparent',
            plot_bgcolor: 'transparent',
            bargap: 0
          }}
          config={{
            displayModeBar: false,
            responsive: true,
            staticPlot: false,
            scrollZoom: false
          }}
          style={{ width: '100%', height: '180px' }}
        />
      ) : (
        <div style={{ padding: '1rem', color: '#777', fontSize: '0.85rem' }}>
          No histogram data available
        </div>
      )

    const survivalPlot = statusColumn
      ? curve && curve.length > 0 ? (
        <Plot
          data={[{
            type: 'scatter',
            mode: 'lines',
            line: { shape: 'hv', color: tableColor || '#1976D2', width: 2 },
            x: curve.map(p => p.time),
            y: curve.map(p => p.survival),
            customdata: curve.map(p => [p.atRisk, p.events, p.censored]),
            hovertemplate: [
              'Time: %{x}',
              'Survival: %{y:.3f}',
              'At risk: %{customdata[0]}',
              'Events: %{customdata[1]}',
              'Censored: %{customdata[2]}'
            ].join('<br>') + '<extra></extra>'
          }]}
          layout={{
            height: 260,
            margin: { t: 20, b: 40, l: 50, r: 10 },
            xaxis: { title: metadata?.display_name || title, tickfont: { size: 10 } },
            yaxis: { title: 'Survival probability', range: [0, 1], tickfont: { size: 10 } },
            paper_bgcolor: 'transparent',
            plot_bgcolor: 'transparent',
            hovermode: 'closest'
          }}
          config={{
            displayModeBar: false,
            responsive: true,
            staticPlot: false,
            scrollZoom: false
          }}
          style={{ width: '100%', height: '260px' }}
        />
      ) : (
        <div style={{ padding: '1rem', color: '#777', fontSize: '0.85rem' }}>
          {curve ? 'No survival data for current filters' : 'Loading survival curve…'}
        </div>
      )
      : (
        <div style={{ padding: '1rem', color: '#777', fontSize: '0.85rem' }}>
          Add a survival status column to plot a Kaplan–Meier curve.
        </div>
      )

    const containerStyle: React.CSSProperties = {
      position: 'relative',
      background: 'white',
      padding: '0.5rem',
      borderRadius: '8px',
      boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
      width: '100%',
      minHeight: showHistogram ? '420px' : '320px',
      boxSizing: 'border-box',
      flexShrink: 0,
      display: 'flex',
      flexDirection: 'column',
      border: tableColor ? `2px solid ${tableColor}20` : undefined
    }

    const menuStats = aggregation.numeric_stats
    const displayHistogramForMenu = getDisplayHistogram(aggregation.histogram ?? [], aggregation.numeric_stats)

    return (
      <div style={containerStyle}>
        {renderChartHeader({
          title: metadata?.display_name || title,
          tooltip: tooltipText,
          countIndicator,
          actions: actionButtons
        })}
        <div style={{ display: 'grid', gridTemplateColumns: showHistogram ? '2fr 1fr' : '1fr', gap: '0.5rem', flex: 1 }}>
          <div style={{ background: '#fafafa', borderRadius: '6px', padding: '0.35rem' }}>
            {survivalPlot}
          </div>
          {showHistogram && (
            <div style={{ background: '#fafafa', borderRadius: '6px', padding: '0.35rem' }}>
              {histogramPlot}
            </div>
          )}
        </div>
        {renderNumericFilterMenu(tableName, field, displayHistogramForMenu, menuStats, cacheKey)}
      </div>
    )
  }

  const renderMapChart = (
    title: string,
    tableName: string,
    field: string,
    tableColor?: string,
    aggregationOverride?: ColumnAggregation,
    cacheKeyOverride?: string,
    countIndicatorOverride?: React.ReactNode
  ) => {
    const cacheKey = cacheKeyOverride ?? getEffectiveCacheKeyForChart(tableName, field)
    const aggregation =
      aggregationOverride && (!cacheKeyOverride || cacheKeyOverride === cacheKey)
        ? aggregationOverride
        : getAggregation(tableName, field, cacheKey)

    if (!aggregation?.categories) return null

    const metadata = getColumnMetadata(tableName, field)
    const tableDisplayName = getTableDisplayNameByName(tableName) || tableName
    const tooltipParts = [
      metadata?.display_name || title,
      `ID: ${field}`,
      metadata?.description || '',
      `Table: ${tableDisplayName}`
    ]
    if (aggregation) {
      const pathLabel = formatMetricPath(aggregation)
      if (pathLabel) tooltipParts.push(pathLabel)
    }
    const tooltipText = tooltipParts.filter(Boolean).join('\n')

    const menuOpen =
      activeFilterMenu?.tableName === tableName &&
      activeFilterMenu.columnName === field &&
      activeFilterMenu.countKey === cacheKey
    const columnActive = hasColumnFilter(field, cacheKey)

    const actionButtons = (
      <>
        {extraActions}
        <button
          type="button"
          onClick={event => {
            event.stopPropagation()
            toggleDashboard(tableName, field)
          }}
          style={{
            border: 'none',
            background: isOnDashboard(tableName, field) ? '#4CAF50' : '#f0f0f0',
            color: isOnDashboard(tableName, field) ? 'white' : '#333',
            borderRadius: '50%',
            width: '20px',
            height: '20px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '0.7rem',
            cursor: 'pointer',
            lineHeight: 1
          }}
          title={isOnDashboard(tableName, field) ? 'Remove from dashboard' : 'Add to dashboard'}
        >
          {isOnDashboard(tableName, field) ? '✓' : '+'}
        </button>
        <button
          type="button"
          onClick={event => {
            event.stopPropagation()
            setActiveFilterMenu(prev =>
              prev && prev.tableName === tableName && prev.columnName === field && prev.countKey === cacheKey
                ? null
                : { tableName, columnName: field, countKey: cacheKey }
            )
          }}
          style={{
            border: 'none',
            background: menuOpen || columnActive ? '#1976D2' : '#f0f0f0',
            color: menuOpen || columnActive ? 'white' : '#333',
            borderRadius: '50%',
            width: '20px',
            height: '20px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '0.7rem',
            cursor: 'pointer',
            lineHeight: 1
          }}
          title={columnActive ? 'Active filter' : 'Filter'}
        >
          ≡
        </button>
      </>
    )

    const containerStyle: React.CSSProperties = {
      position: 'relative',
      background: 'white',
      padding: '0.5rem',
      borderRadius: '8px',
      boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
      width: '708px',
      minHeight: '400px',
      boxSizing: 'border-box',
      flexShrink: 0,
      display: 'flex',
      flexDirection: 'column',
      border: tableColor ? `2px solid ${tableColor}20` : undefined
    }

    const countIndicator = countIndicatorOverride ?? renderTableCountIndicator(tableName, field, cacheKey)

    const metricLabels = getMetricLabels(aggregation)
    const pathLabel = formatMetricPath(aggregation)

    // Map state values to codes for Plotly choropleth
    // Aggregate counts by state code (to handle both "CA" and "California")
    const stateMap = new Map<string, { count: number, name: string, originalValues: any[] }>()

    aggregation.categories.forEach(category => {
      const stateValue = category.value === '' ? '(Empty)' : String(category.value)
      const normalizedName = normalizeStateName(stateValue)
      const stateCode = getStateCode(normalizedName)

      if (stateCode) {
        const existing = stateMap.get(stateCode)
        if (existing) {
          existing.count += category.count
          existing.originalValues.push(normalizeFilterValue(category.value))
        } else {
          stateMap.set(stateCode, {
            count: category.count,
            name: normalizedName,
            originalValues: [normalizeFilterValue(category.value)]
          })
        }
      }
    })

    const locationCodes: string[] = []
    const zValues: number[] = []
    const hoverTexts: string[] = []
    const filterValues: any[][] = []

    stateMap.forEach((data, code) => {
      locationCodes.push(code)
      zValues.push(data.count)
      hoverTexts.push(data.name)
      filterValues.push(data.originalValues)
    })

    if (locationCodes.length === 0) {
      // No valid US state data - fall back to categorical table view
      return renderTableView(title, tableName, field, tableColor, aggregationOverride, cacheKeyOverride, countIndicatorOverride)
    }

    const totalCount = aggregation.total_rows ?? zValues.reduce((sum, val) => sum + val, 0)

    const baselineAggregation = getBaselineAggregation(tableName, field)
    const categoriesForMenu =
      metricsMatch(baselineAggregation, aggregation) && baselineAggregation?.categories?.length
        ? baselineAggregation.categories
        : aggregation.categories

    return (
      <div style={containerStyle}>
        {renderChartHeader({
          title: metadata?.display_name || title,
          tooltip: tooltipText,
          countIndicator,
          actions: actionButtons
        })}
        <Plot
          data={[{
            type: 'choropleth',
            locationmode: 'USA-states',
            locations: locationCodes,
            z: zValues,
            text: hoverTexts,
            hovertemplate: `${['%{text}', `Count (${metricLabels.short}): %{z}`, 'Percent: %{customdata}%']
              .concat(pathLabel ? [pathLabel] : [])
              .join('<br>')}<extra></extra>`,
            customdata: zValues.map(val =>
              totalCount > 0 ? ((val / totalCount) * 100).toFixed(1) : '0'
            ),
            colorscale: [
              [0, tableColor ? `${tableColor}40` : '#E3F2FD'],
              [1, tableColor || '#2196F3']
            ],
            marker: {
              line: {
                color: locationCodes.map((_code, idx) =>
                  filterValues[idx].some(v => isValueFiltered(field, v, cacheKey)) ? '#000' : 'white'
                ),
                width: locationCodes.map((_code, idx) =>
                  filterValues[idx].some(v => isValueFiltered(field, v, cacheKey)) ? 3 : 1
                )
              }
            },
            showscale: true,
            colorbar: {
              title: metricLabels.short,
              titleside: 'right',
              tickfont: { size: 10 },
              len: 0.7
            }
          }]}
          layout={{
            geo: {
              scope: 'usa',
              projection: { type: 'albers usa' },
              showlakes: true,
              lakecolor: 'rgb(255, 255, 255)'
            },
            height: 380,
            margin: { t: 5, b: 5, l: 5, r: 5 },
            paper_bgcolor: 'transparent',
            dragmode: false
          }}
          config={{
            displayModeBar: false,
            responsive: true,
            staticPlot: false,
            scrollZoom: false
          }}
          style={{ width: '698px', height: '380px', cursor: 'pointer' }}
          onClick={(event: PlotMouseEvent) => {
            const point = event.points?.[0]
            if (!point) return

            const pointIndex = point.pointIndex
            if (typeof pointIndex === 'number' && pointIndex >= 0 && pointIndex < filterValues.length) {
              const stateValues = filterValues[pointIndex]
              // If state has multiple representations (e.g., "CA" and "California"), handle as multi-value
              if (stateValues.length === 1) {
                toggleFilter(field, stateValues[0], tableName, cacheKey)
              } else {
                // Create/toggle filter with all state representations
                setFilters(prev => [
                  ...removeColumnFilters(prev, field, cacheKey),
                  { column: field, operator: 'in', value: stateValues, tableName, countByKey: cacheKey }
                ])
              }
            }
          }}
          onSelected={(event: PlotSelectionEvent) => {
            if (!event?.points || event.points.length === 0) return
            const selectedValues = event.points
              .map(p => p.pointIndex)
              .filter((idx): idx is number => typeof idx === 'number' && idx >= 0 && idx < filterValues.length)
              .flatMap(idx => filterValues[idx]) // Flatten all state value arrays

            if (selectedValues.length > 0) {
              setFilters(prev => [
                ...removeColumnFilters(prev, field, cacheKey),
                { column: field, operator: 'in', value: selectedValues, tableName, countByKey: cacheKey }
              ])
            }
          }}
        />
        {renderFilterMenu(tableName, field, categoriesForMenu, cacheKey)}
      </div>
    )
  }


  if (loading) return <p>Loading explorer...</p>
  if (!dataset) return <p>Dataset not found</p>

  return (
    <div>
      {/* Header */}
      <div style={{ marginBottom: '2rem', background: 'white', padding: '1.5rem', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)', position: 'relative' }}>
        <button
          onClick={() => navigate(`/datasets/${id}/manage`)}
          style={{
            position: 'absolute',
            top: '1.5rem',
            right: '1.5rem',
            padding: '0.5rem',
            background: '#757575',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '1.2rem',
            lineHeight: '1',
            width: '32px',
            height: '32px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center'
          }}
          title="Manage dataset"
        >
          ✎
        </button>

        <h2 style={{ marginTop: 0, paddingRight: '3rem' }}>{dataset.name}</h2>
        {dataset.description && (
          <SafeHtml
            html={dataset.description}
            style={{ color: '#666', margin: '0.5rem 0', display: 'block' }}
          />
        )}

        <div style={{ display: 'flex', gap: '2rem', marginTop: '1rem', fontSize: '0.875rem' }}>
          <div>
            <strong>Tables:</strong> {dataset.tables.length}
          </div>
        </div>
      </div>

      <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: '-1rem', marginBottom: '0.5rem' }}>
        <div style={{ position: 'relative', display: 'inline-block' }}>
          <button
            ref={settingsButtonRef}
            onClick={() => setShowSettingsMenu(prev => !prev)}
            style={{
              border: 'none',
              borderRadius: '4px',
              padding: '0.3rem 0.6rem',
              background: '#ECEFF1',
              color: '#333',
              cursor: 'pointer',
              fontSize: '0.75rem',
              display: 'flex',
              alignItems: 'center',
              gap: '0.35rem'
            }}
            aria-label="Chart settings"
          >
            <span role="img" aria-hidden="true">⚙</span>
            Chart settings
          </button>
          {showSettingsMenu && (
            <div
              ref={settingsMenuRef}
              style={{
                position: 'absolute',
                top: 'calc(100% + 8px)',
                right: 0,
                background: 'white',
                borderRadius: '8px',
                boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                padding: '0.75rem 1rem',
                width: '220px',
                zIndex: 5
              }}
            >
              <div style={{ fontWeight: 600, fontSize: '0.85rem', marginBottom: '0.5rem' }}>Chart settings</div>
              <div style={{ fontSize: '0.75rem', marginBottom: '0.25rem', color: '#444' }}>Chart labels</div>
              <div style={{ display: 'flex', gap: '0.35rem' }}>
                <button
                  type="button"
                  onClick={() => {
                    setShowPercentageLabels(false)
                    setShowSettingsMenu(false)
                  }}
                  style={{
                    border: 'none',
                    borderRadius: '999px',
                    padding: '0.2rem 0.9rem',
                    fontSize: '0.75rem',
                    cursor: 'pointer',
                    background: showPercentageLabels ? '#ECEFF1' : '#1976D2',
                    color: showPercentageLabels ? '#333' : 'white'
                  }}
                >
                  Counts
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setShowPercentageLabels(true)
                    setShowSettingsMenu(false)
                  }}
                  style={{
                    border: 'none',
                    borderRadius: '999px',
                    padding: '0.2rem 0.9rem',
                    fontSize: '0.75rem',
                    cursor: 'pointer',
                    background: showPercentageLabels ? '#1976D2' : '#ECEFF1',
                    color: showPercentageLabels ? 'white' : '#333'
                  }}
                >
                  Percentages
                </button>
              </div>
              <div style={{ fontSize: '0.7rem', color: '#777', marginTop: '0.4rem' }}>
                {showPercentageLabels ? 'Percentages may exceed 100% when parents overlap.' : 'Switch to percentages when needed.'}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Saved Filters Bar - Always visible when presets exist */}
      {presets.length > 0 && (
        <div style={{
          marginBottom: '1rem',
          background: '#E3F2FD',
          padding: '0.75rem 1rem',
          borderRadius: '8px',
          border: '1px solid #90CAF9',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between'
        }}>
          <div style={{ fontSize: '0.875rem', color: '#1976D2', fontWeight: 500 }}>
            {presets.length} saved filter{presets.length !== 1 ? 's' : ''}
          </div>
          <div style={{ display: 'flex', gap: '0.5rem' }}>
            <div style={{ position: 'relative' }}>
              <button
                onClick={() => setShowPresetsDropdown(!showPresetsDropdown)}
                style={{
                  padding: '0.25rem 0.75rem',
                  background: '#2196F3',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontSize: '0.75rem'
                }}
                title="Load a saved filter"
              >
                Load Filter
              </button>
            </div>
            <button
              onClick={() => setShowManagePresetsDialog(true)}
              style={{
                padding: '0.25rem 0.75rem',
                background: '#FF9800',
                color: 'white',
                border: 'none',
                borderRadius: '4px',
                cursor: 'pointer',
                fontSize: '0.75rem'
              }}
              title="Manage saved filters"
            >
              Manage
            </button>
          </div>
        </div>
      )}


      {/* Active Filters */}
      {filters.length > 0 && (
        <div style={{
          marginBottom: '1rem',
          background: '#F5F5F5',
          padding: '1rem',
          borderRadius: '8px',
          border: '1px solid #E0E0E0'
        }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.5rem' }}>
            <strong style={{ fontSize: '0.875rem' }}>Active Filters:</strong>
            <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
              <button
                onClick={() => setShowSavePresetDialog(true)}
                style={{
                  padding: '0.25rem 0.75rem',
                  background: '#4CAF50',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontSize: '0.75rem'
                }}
                title="Save current filters"
              >
                Save Filter
              </button>
              <button
                onClick={clearFilters}
                style={{
                  padding: '0.25rem 0.75rem',
                  background: '#f44336',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontSize: '0.75rem'
                }}
              >
                Clear All
              </button>
            </div>
          </div>
          <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap', alignItems: 'center' }}>
            {filters.map((filter, idx) => {
              const actualFilter = unwrapNot(filter)
              if (!actualFilter) return null
              const isNot = !!filter.not

              const columnName = getFilterColumn(actualFilter)
              const tableName = getFilterTableNameForCacheKey(actualFilter)
              const tableColor = tableName ? getTableColor(tableName) : '#9E9E9E'
              const table = dataset?.tables.find(t => t.name === tableName)

              // Extract count-by table for border color
              const countByTable = targetFromCacheKey(filter.countByKey)
              const countByColor = countByTable ? getTableColor(countByTable) : tableColor

              let displayValue = String(actualFilter.value)
              let logicType = '' // For tooltip

              // Remove handler - uses actualFilter's column/table regardless of NOT wrapper
              const removeHandler = () => {
                if (tableName && columnName) {
                  clearColumnFilter(tableName, columnName, getFilterCountKey(filter))
                } else {
                  setFilters(filters.filter((_, i) => i !== idx))
                }
              }

              // Toggle NOT wrapper
              const toggleNot = () => {
                setFilters(prev => {
                  const updated = [...prev]
                  if (isNot) {
                    // Remove NOT wrapper
                    updated[idx] = actualFilter
                  } else {
                    // Add NOT wrapper
                    updated[idx] = { not: actualFilter }
                  }
                  return updated
                })
              }

              if (actualFilter.operator === 'between' && Array.isArray(actualFilter.value)) {
                displayValue = `[${typeof actualFilter.value[0] === 'number' ? actualFilter.value[0].toFixed(2) : actualFilter.value[0]}, ${typeof actualFilter.value[1] === 'number' ? actualFilter.value[1].toFixed(2) : actualFilter.value[1]}]`
                logicType = 'Range'
              } else if (actualFilter.operator === 'in' && Array.isArray(actualFilter.value)) {
                const displayVals = actualFilter.value.map(v => {
                  if (v === '') return '(Empty)'
                  if (v === ' ') return '(Space)'
                  return v
                })
                // Show OR for multi-value selections
                if (actualFilter.value.length > 1) {
                  displayValue = displayVals.slice(0, 3).join(' OR ')
                  if (actualFilter.value.length > 3) {
                    displayValue += ` OR ${actualFilter.value.length - 3} more...`
                  }
                } else {
                  displayValue = displayVals[0] || ''
                }
                logicType = actualFilter.value.length > 1 ? `OR (${actualFilter.value.length} values)` : 'Single value'
              } else if (actualFilter.operator === 'eq') {
                if (actualFilter.value === '') displayValue = '(Empty)'
                else if (actualFilter.value === ' ') displayValue = '(Space)'
                else displayValue = String(actualFilter.value)
                logicType = 'Equals'
              } else if (actualFilter.or && Array.isArray(actualFilter.or)) {
                const ranges = actualFilter.or
                  .map(rangeFilter => rangeFilter as Filter)
                  .filter(rangeFilter => rangeFilter.column === actualFilter.column && rangeFilter.operator === 'between' && Array.isArray(rangeFilter.value))
                  .map(rangeFilter => {
                    const [start, end] = rangeFilter.value
                    const startLabel = typeof start === 'number' ? formatRangeValue(start) : String(start)
                    const endLabel = typeof end === 'number' ? formatRangeValue(end) : String(end)
                    return `${startLabel}–${endLabel}`
                  })

                displayValue = ranges.join(' OR ')
                logicType = `OR (${ranges.length} ranges)`
              }
              const columnLabel = columnName ?? '(Column)'
              const notPrefix = isNot ? 'NOT: ' : ''
              const tooltipText = tableName
                ? `${table?.displayName || tableName}.${columnLabel}\n${notPrefix}${logicType}\nValue: ${displayValue}`
                : columnLabel

              const showAndSeparator = idx > 0

              return (
                <React.Fragment key={idx}>
                  {showAndSeparator && (
                    <div style={{
                      color: '#666',
                      fontSize: '0.75rem',
                      fontWeight: 600,
                      padding: '0 0.25rem',
                      userSelect: 'none'
                    }}>
                      AND
                    </div>
                  )}
                  <div
                    style={{
                      background: isNot ? `linear-gradient(135deg, ${tableColor}DD, ${tableColor}BB)` : tableColor,
                      padding: '0.25rem 0.75rem',
                      borderRadius: '4px',
                      fontSize: '0.875rem',
                      display: 'flex',
                      alignItems: 'center',
                      gap: '0.5rem',
                      outline: `4px solid ${countByColor}`,
                      outlineOffset: '2px',
                      border: isNot ? `2px dashed rgba(255,255,255,0.6)` : 'none',
                      color: 'white',
                      fontWeight: 500,
                      opacity: isNot ? 0.9 : 1
                    }}
                    title={tooltipText}
                  >
                    {isNot && (
                      <span style={{
                        background: 'rgba(0,0,0,0.3)',
                        padding: '0.1rem 0.35rem',
                        borderRadius: '3px',
                        fontSize: '0.7rem',
                        fontWeight: 700,
                        marginRight: '0.1rem'
                      }}>
                        NOT
                      </span>
                    )}
                    <span style={{ textDecoration: isNot ? 'line-through' : 'none' }}>
                      <strong>{columnLabel}:</strong> {displayValue}
                    </span>
                    <button
                      onClick={toggleNot}
                      style={{
                        background: isNot ? 'rgba(0,0,0,0.3)' : 'rgba(255,255,255,0.3)',
                        border: 'none',
                        color: 'white',
                        cursor: 'pointer',
                        padding: '0 0.3rem',
                        fontSize: '0.75rem',
                        lineHeight: '1',
                        borderRadius: '3px',
                        fontWeight: 'bold'
                      }}
                      title={isNot ? 'Remove NOT' : 'Add NOT'}
                    >
                      ¬
                    </button>
                    <button
                      onClick={removeHandler}
                      style={{
                        background: 'rgba(255,255,255,0.3)',
                        border: 'none',
                        color: 'white',
                        cursor: 'pointer',
                        padding: '0 0.25rem',
                        fontSize: '1rem',
                        lineHeight: '1',
                        borderRadius: '3px',
                        fontWeight: 'bold'
                      }}
                    >
                      ×
                    </button>
                  </div>
                </React.Fragment>
              )
            })}
          </div>
        </div>
      )}

      {/* Save Filter Dialog */}
      {showSavePresetDialog && (
        <div
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: 'rgba(0,0,0,0.5)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 1000
          }}
          onClick={() => setShowSavePresetDialog(false)}
        >
          <div
            style={{
              background: 'white',
              padding: '1.5rem',
              borderRadius: '8px',
              minWidth: '400px',
              boxShadow: '0 4px 12px rgba(0,0,0,0.2)'
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <h3 style={{ marginTop: 0 }}>Save Filter</h3>
            <input
              type="text"
              value={presetNameInput}
              onChange={(e) => setPresetNameInput(e.target.value)}
              placeholder="Enter filter name..."
              onKeyDown={(e) => {
                if (e.key === 'Enter') savePreset()
                if (e.key === 'Escape') setShowSavePresetDialog(false)
              }}
              autoFocus
              style={{
                width: '100%',
                padding: '0.5rem',
                marginBottom: '1rem',
                border: '1px solid #ddd',
                borderRadius: '4px',
                fontSize: '0.875rem'
              }}
            />
            <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }}>
              <button
                onClick={() => setShowSavePresetDialog(false)}
                style={{
                  padding: '0.5rem 1rem',
                  background: '#f0f0f0',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontSize: '0.875rem'
                }}
              >
                Cancel
              </button>
              <button
                onClick={savePreset}
                disabled={!presetNameInput.trim()}
                style={{
                  padding: '0.5rem 1rem',
                  background: presetNameInput.trim() ? '#4CAF50' : '#ccc',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: presetNameInput.trim() ? 'pointer' : 'not-allowed',
                  fontSize: '0.875rem'
                }}
              >
                Save
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Load Filter Dropdown */}
      {showPresetsDropdown && (
        <div
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            zIndex: 999
          }}
          onClick={() => setShowPresetsDropdown(false)}
        >
          <div
            style={{
              position: 'absolute',
              top: '120px',
              right: '20px',
              background: 'white',
              border: '1px solid #ddd',
              borderRadius: '8px',
              boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
              padding: '0.5rem',
              minWidth: '300px',
              maxHeight: '400px',
              overflowY: 'auto'
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div style={{ padding: '0.5rem', borderBottom: '1px solid #eee', marginBottom: '0.5rem' }}>
              <strong style={{ fontSize: '0.875rem' }}>Select Filter</strong>
            </div>
            {presets.map((preset) => (
              <div
                key={preset.id}
                onClick={() => applyPreset(preset)}
                style={{
                  padding: '0.75rem',
                  cursor: 'pointer',
                  borderRadius: '4px',
                  marginBottom: '0.25rem',
                  border: '1px solid #eee',
                  transition: 'background 0.2s'
                }}
                onMouseEnter={(e) => e.currentTarget.style.background = '#f0f0f0'}
                onMouseLeave={(e) => e.currentTarget.style.background = 'white'}
              >
                <div style={{ fontWeight: 500, fontSize: '0.875rem', marginBottom: '0.25rem' }}>
                  {preset.name}
                </div>
                <div style={{ fontSize: '0.7rem', color: '#666' }}>
                  {preset.filters.length} filter{preset.filters.length !== 1 ? 's' : ''} · {new Date(preset.createdAt).toLocaleDateString()}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Manage Filters Dialog */}
      {showManagePresetsDialog && (
        <div
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: 'rgba(0,0,0,0.5)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 1000
          }}
          onClick={() => setShowManagePresetsDialog(false)}
        >
          <div
            style={{
              background: 'white',
              padding: '1.5rem',
              borderRadius: '8px',
              minWidth: '500px',
              maxHeight: '600px',
              overflowY: 'auto',
              boxShadow: '0 4px 12px rgba(0,0,0,0.2)'
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
              <h3 style={{ margin: 0 }}>Manage Saved Filters</h3>
              <div style={{ display: 'flex', gap: '0.5rem' }}>
                <button
                  onClick={exportPresets}
                  style={{
                    padding: '0.4rem 0.75rem',
                    background: '#2196F3',
                    color: 'white',
                    border: 'none',
                    borderRadius: '4px',
                    cursor: 'pointer',
                    fontSize: '0.75rem'
                  }}
                >
                  Export
                </button>
                <label style={{
                  padding: '0.4rem 0.75rem',
                  background: '#FF9800',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontSize: '0.75rem'
                }}>
                  Import
                  <input
                    type="file"
                    accept=".json"
                    onChange={importPresets}
                    style={{ display: 'none' }}
                  />
                </label>
              </div>
            </div>
            {presets.length === 0 ? (
              <p style={{ color: '#999', textAlign: 'center', padding: '2rem' }}>
                No filters saved yet.
              </p>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                {presets.map((preset) => (
                  <div
                    key={preset.id}
                    style={{
                      border: '1px solid #ddd',
                      borderRadius: '6px',
                      padding: '0.75rem'
                    }}
                  >
                    {editingPresetId === preset.id ? (
                      <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '0.5rem' }}>
                        <input
                          type="text"
                          defaultValue={preset.name}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter') renamePreset(preset.id, e.currentTarget.value)
                            if (e.key === 'Escape') setEditingPresetId(null)
                          }}
                          autoFocus
                          style={{
                            flex: 1,
                            padding: '0.25rem 0.5rem',
                            border: '1px solid #2196F3',
                            borderRadius: '4px',
                            fontSize: '0.875rem'
                          }}
                        />
                        <button
                          onClick={(e) => renamePreset(preset.id, e.currentTarget.previousElementSibling?.['value'] || '')}
                          style={{
                            padding: '0.25rem 0.5rem',
                            background: '#4CAF50',
                            color: 'white',
                            border: 'none',
                            borderRadius: '4px',
                            cursor: 'pointer',
                            fontSize: '0.75rem'
                          }}
                        >
                          Save
                        </button>
                        <button
                          onClick={() => setEditingPresetId(null)}
                          style={{
                            padding: '0.25rem 0.5rem',
                            background: '#f0f0f0',
                            border: 'none',
                            borderRadius: '4px',
                            cursor: 'pointer',
                            fontSize: '0.75rem'
                          }}
                        >
                          Cancel
                        </button>
                      </div>
                    ) : (
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                        <strong style={{ fontSize: '0.875rem' }}>{preset.name}</strong>
                        <div style={{ display: 'flex', gap: '0.5rem' }}>
                          <button
                            onClick={() => setEditingPresetId(preset.id)}
                            style={{
                              padding: '0.25rem 0.5rem',
                              background: '#2196F3',
                              color: 'white',
                              border: 'none',
                              borderRadius: '4px',
                              cursor: 'pointer',
                              fontSize: '0.75rem'
                            }}
                          >
                            Rename
                          </button>
                          <button
                            onClick={() => {
                              if (window.confirm(`Delete saved filter "${preset.name}"?`)) {
                                deletePreset(preset.id)
                              }
                            }}
                            style={{
                              padding: '0.25rem 0.5rem',
                              background: '#f44336',
                              color: 'white',
                              border: 'none',
                              borderRadius: '4px',
                              cursor: 'pointer',
                              fontSize: '0.75rem'
                            }}
                          >
                            Delete
                          </button>
                        </div>
                      </div>
                    )}
                    <div style={{ fontSize: '0.75rem', color: '#666' }}>
                      {preset.filters.length} filter{preset.filters.length !== 1 ? 's' : ''} · Created {new Date(preset.createdAt).toLocaleDateString()}
                    </div>
                  </div>
                ))}
              </div>
            )}
            <div style={{ marginTop: '1rem', display: 'flex', justifyContent: 'flex-end' }}>
              <button
                onClick={() => setShowManagePresetsDialog(false)}
                style={{
                  padding: '0.5rem 1rem',
                  background: '#f0f0f0',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontSize: '0.875rem'
                }}
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Save Dashboard Dialog */}
      {showSaveDashboardDialog && (
        <div
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: 'rgba(0,0,0,0.5)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 1000
          }}
          onClick={() => setShowSaveDashboardDialog(false)}
        >
          <div
            style={{
              background: 'white',
              padding: '1.5rem',
              borderRadius: '8px',
              minWidth: '400px',
              boxShadow: '0 4px 12px rgba(0,0,0,0.2)'
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <h3 style={{ marginTop: 0 }}>Save Dashboard</h3>
            <input
              type="text"
              value={newDashboardName}
              onChange={(e) => setNewDashboardName(e.target.value)}
              placeholder="Enter dashboard name..."
              onKeyDown={(e) => {
                if (e.key === 'Enter' && newDashboardName.trim()) saveDashboard(newDashboardName.trim())
                if (e.key === 'Escape') setShowSaveDashboardDialog(false)
              }}
              autoFocus
              style={{
                width: '100%',
                padding: '0.5rem',
                marginBottom: '1rem',
                border: '1px solid #ddd',
                borderRadius: '4px',
                fontSize: '0.875rem'
              }}
            />
            <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }}>
              <button
                onClick={() => {
                  setShowSaveDashboardDialog(false)
                  setNewDashboardName('')
                }}
                style={{
                  padding: '0.5rem 1rem',
                  background: '#f0f0f0',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontSize: '0.875rem'
                }}
              >
                Cancel
              </button>
              <button
                onClick={() => saveDashboard(newDashboardName.trim())}
                disabled={!newDashboardName.trim()}
                style={{
                  padding: '0.5rem 1rem',
                  background: newDashboardName.trim() ? '#4CAF50' : '#ccc',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: newDashboardName.trim() ? 'pointer' : 'not-allowed',
                  fontSize: '0.875rem'
                }}
              >
                Save
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Load Dashboard Dialog */}
      {showLoadDashboardDialog && (
        <div
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: 'rgba(0,0,0,0.5)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 1000
          }}
          onClick={() => setShowLoadDashboardDialog(false)}
        >
          <div
            style={{
              background: 'white',
              padding: '1.5rem',
              borderRadius: '8px',
              minWidth: '400px',
              maxWidth: '500px',
              maxHeight: '600px',
              overflowY: 'auto',
              boxShadow: '0 4px 12px rgba(0,0,0,0.2)'
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <h3 style={{ marginTop: 0 }}>Load Dashboard</h3>
            {savedDashboards.length === 0 ? (
              <p style={{ color: '#999', textAlign: 'center', padding: '2rem' }}>
                No saved dashboards yet.
              </p>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                {savedDashboards.map(dashboard => (
                  <div
                    key={dashboard.id}
                    onClick={() => {
                      loadDashboard(dashboard.id)
                      setShowLoadDashboardDialog(false)
                    }}
                    style={{
                      padding: '0.75rem',
                      border: activeDashboardId === dashboard.id ? '2px solid #2196F3' : '1px solid #ddd',
                      borderRadius: '6px',
                      cursor: 'pointer',
                      transition: 'background 0.2s, border-color 0.2s',
                      background: activeDashboardId === dashboard.id ? '#E3F2FD' : 'white'
                    }}
                    onMouseEnter={(e) => {
                      if (activeDashboardId !== dashboard.id) {
                        e.currentTarget.style.background = '#f5f5f5'
                        e.currentTarget.style.borderColor = '#2196F3'
                      }
                    }}
                    onMouseLeave={(e) => {
                      if (activeDashboardId !== dashboard.id) {
                        e.currentTarget.style.background = 'white'
                        e.currentTarget.style.borderColor = '#ddd'
                      }
                    }}
                  >
                    <div style={{ fontWeight: 500, fontSize: '0.875rem', marginBottom: '0.25rem' }}>
                      {dashboard.name}
                      {activeDashboardId === dashboard.id && (
                        <span style={{ marginLeft: '0.5rem', color: '#2196F3', fontSize: '0.75rem' }}>(Most Recently Loaded)</span>
                      )}
                    </div>
                    <div style={{ fontSize: '0.75rem', color: '#666' }}>
                      {dashboard.charts.length} chart{dashboard.charts.length !== 1 ? 's' : ''} · Created {new Date(dashboard.createdAt).toLocaleDateString()}
                    </div>
                  </div>
                ))}
              </div>
            )}
            <div style={{ marginTop: '1rem', display: 'flex', justifyContent: 'flex-end' }}>
              <button
                onClick={() => setShowLoadDashboardDialog(false)}
                style={{
                  padding: '0.5rem 1rem',
                  background: '#f0f0f0',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontSize: '0.875rem'
                }}
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Manage Dashboards Dialog */}
      {showManageDashboardsDialog && (
        <div
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: 'rgba(0,0,0,0.5)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 1000
          }}
          onClick={() => setShowManageDashboardsDialog(false)}
        >
          <div
            style={{
              background: 'white',
              padding: '1.5rem',
              borderRadius: '8px',
              minWidth: '500px',
              maxHeight: '600px',
              overflowY: 'auto',
              boxShadow: '0 4px 12px rgba(0,0,0,0.2)'
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <h3 style={{ margin: '0 0 1rem 0' }}>Manage Saved Dashboards</h3>
            {savedDashboards.length === 0 ? (
              <p style={{ color: '#999', textAlign: 'center', padding: '2rem' }}>
                No dashboards saved yet.
              </p>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                {savedDashboards.map((dashboard) => (
                  <div
                    key={dashboard.id}
                    style={{
                      border: '1px solid #ddd',
                      borderRadius: '6px',
                      padding: '0.75rem'
                    }}
                  >
                    {editingDashboardId === dashboard.id ? (
                      <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '0.5rem' }}>
                        <input
                          type="text"
                          defaultValue={dashboard.name}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter') renameDashboard(dashboard.id, e.currentTarget.value)
                            if (e.key === 'Escape') setEditingDashboardId(null)
                          }}
                          autoFocus
                          style={{
                            flex: 1,
                            padding: '0.25rem 0.5rem',
                            border: '1px solid #2196F3',
                            borderRadius: '4px',
                            fontSize: '0.875rem'
                          }}
                        />
                        <button
                          onClick={(e) => {
                            const input = e.currentTarget.previousElementSibling as HTMLInputElement
                            renameDashboard(dashboard.id, input?.value || '')
                          }}
                          style={{
                            padding: '0.25rem 0.5rem',
                            background: '#4CAF50',
                            color: 'white',
                            border: 'none',
                            borderRadius: '4px',
                            cursor: 'pointer',
                            fontSize: '0.75rem'
                          }}
                        >
                          Save
                        </button>
                        <button
                          onClick={() => setEditingDashboardId(null)}
                          style={{
                            padding: '0.25rem 0.5rem',
                            background: '#f0f0f0',
                            border: 'none',
                            borderRadius: '4px',
                            cursor: 'pointer',
                            fontSize: '0.75rem'
                          }}
                        >
                          Cancel
                        </button>
                      </div>
                    ) : (
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                        <strong style={{ fontSize: '0.875rem' }}>{dashboard.name}</strong>
                        <div style={{ display: 'flex', gap: '0.5rem' }}>
                          <button
                            onClick={() => {
                              setEditingDashboardId(dashboard.id)
                              setEditingDashboardName(dashboard.name)
                            }}
                            style={{
                              padding: '0.25rem 0.5rem',
                              background: '#2196F3',
                              color: 'white',
                              border: 'none',
                              borderRadius: '4px',
                              cursor: 'pointer',
                              fontSize: '0.75rem'
                            }}
                          >
                            Rename
                          </button>
                          <button
                            onClick={() => {
                              if (window.confirm(`Delete dashboard "${dashboard.name}"?`)) {
                                deleteDashboard(dashboard.id)
                              }
                            }}
                            style={{
                              padding: '0.25rem 0.5rem',
                              background: '#f44336',
                              color: 'white',
                              border: 'none',
                              borderRadius: '4px',
                              cursor: 'pointer',
                              fontSize: '0.75rem'
                            }}
                          >
                            Delete
                          </button>
                        </div>
                      </div>
                    )}
                    <div style={{ fontSize: '0.75rem', color: '#666' }}>
                      {dashboard.charts.length} chart{dashboard.charts.length !== 1 ? 's' : ''} · Created {new Date(dashboard.createdAt).toLocaleDateString()}
                    </div>
                  </div>
                ))}
              </div>
            )}
            <div style={{ marginTop: '1rem', display: 'flex', justifyContent: 'flex-end' }}>
              <button
                onClick={() => setShowManageDashboardsDialog(false)}
                style={{
                  padding: '0.5rem 1rem',
                  background: '#f0f0f0',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontSize: '0.875rem'
                }}
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Tab Navigation */}
      <div style={{
        marginBottom: '1.5rem',
        background: 'white',
        padding: '0.5rem',
        borderRadius: '8px',
        boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
        display: 'flex',
        gap: '0.5rem',
        flexWrap: 'wrap'
      }}>
        {/* Dashboard Tab */}
        <button
          onClick={() => setActiveTab('dashboard')}
          style={{
            padding: '0.75rem 1.5rem',
            background: activeTab === 'dashboard' ? '#607D8B' : 'transparent',
            color: activeTab === 'dashboard' ? 'white' : '#333',
            border: `2px solid ${activeTab === 'dashboard' ? '#607D8B' : '#E0E0E0'}`,
            borderRadius: '6px',
            cursor: 'pointer',
            fontSize: '0.875rem',
            fontWeight: activeTab === 'dashboard' ? 600 : 400,
            transition: 'all 0.2s ease',
            display: 'flex',
            alignItems: 'center',
            gap: '0.5rem'
          }}
          onMouseEnter={(e) => {
            if (activeTab !== 'dashboard') {
              e.currentTarget.style.borderColor = '#607D8B'
              e.currentTarget.style.color = '#607D8B'
            }
          }}
          onMouseLeave={(e) => {
            if (activeTab !== 'dashboard') {
              e.currentTarget.style.borderColor = '#E0E0E0'
              e.currentTarget.style.color = '#333'
            }
          }}
        >
          <div style={{
            width: '8px',
            height: '20px',
            borderRadius: '2px',
            background: activeTab === 'dashboard' ? 'white' : '#607D8B'
          }} />
          Dashboard {dashboardCharts.length > 0 && `(${dashboardCharts.length})`}
        </button>

        {/* Table Tabs */}
        {dataset.tables.map(table => {
          const tableColor = getTableColor(table.name)
          const isActive = activeTab === table.name
          const chartCount = getTableChartCount(table.name)

          return (
            <button
              key={table.name}
              onClick={() => setActiveTab(table.name)}
              style={{
                padding: '0.75rem 1.5rem',
                background: isActive ? tableColor : 'transparent',
                color: isActive ? 'white' : '#333',
                border: `2px solid ${isActive ? tableColor : '#E0E0E0'}`,
                borderRadius: '6px',
                cursor: 'pointer',
                fontSize: '0.875rem',
                fontWeight: isActive ? 600 : 400,
                transition: 'all 0.2s ease',
                display: 'flex',
                alignItems: 'center',
                gap: '0.5rem'
              }}
              onMouseEnter={(e) => {
                if (!isActive) {
                  e.currentTarget.style.borderColor = tableColor
                  e.currentTarget.style.color = tableColor
                }
              }}
              onMouseLeave={(e) => {
                if (!isActive) {
                  e.currentTarget.style.borderColor = '#E0E0E0'
                  e.currentTarget.style.color = '#333'
                }
              }}
            >
              <div style={{
                width: '8px',
                height: '20px',
                borderRadius: '2px',
                background: isActive ? 'white' : tableColor
              }} />
              {table.displayName || table.name} {chartCount > 0 && `(${chartCount})`}
            </button>
          )
        })}
      </div>

      {/* Dashboard View */}
      {activeTab === 'dashboard' && (
        <div>
          {/* Dashboard Controls - always visible */}
          <div style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            marginBottom: '1rem',
            gap: '1rem'
          }}>
            <h3 style={{ margin: 0 }}>
              {activeDashboardId
                ? `Dashboard: ${savedDashboards.find(d => d.id === activeDashboardId)?.name || 'Unknown'}`
                : `Dashboard (${dashboardCharts.length} charts)`}
            </h3>
            <div style={{ display: 'flex', gap: '0.5rem' }}>
              <button
                onClick={() => setShowLoadDashboardDialog(true)}
                disabled={savedDashboards.length === 0}
                style={{
                  padding: '0.5rem 1rem',
                  background: savedDashboards.length > 0 ? '#4CAF50' : '#ccc',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: savedDashboards.length > 0 ? 'pointer' : 'not-allowed',
                  fontSize: '0.875rem'
                }}
              >
                Load Dashboard
              </button>
              <button
                onClick={() => setShowSaveDashboardDialog(true)}
                disabled={dashboardCharts.length === 0}
                style={{
                  padding: '0.5rem 1rem',
                  background: dashboardCharts.length > 0 ? '#2196F3' : '#ccc',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: dashboardCharts.length > 0 ? 'pointer' : 'not-allowed',
                  fontSize: '0.875rem'
                }}
              >
                Save Dashboard
              </button>
              <button
                onClick={() => setShowManageDashboardsDialog(true)}
                disabled={savedDashboards.length === 0}
                style={{
                  padding: '0.5rem 1rem',
                  background: savedDashboards.length > 0 ? '#FF9800' : '#ccc',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: savedDashboards.length > 0 ? 'pointer' : 'not-allowed',
                  fontSize: '0.875rem'
                }}
              >
                Manage
              </button>
              <button
                onClick={() => {
                  if (window.confirm('Clear all charts from dashboard?')) {
                    setDashboardCharts([])
                    setActiveDashboardId(null)
                  }
                }}
                disabled={dashboardCharts.length === 0}
                style={{
                  padding: '0.5rem 1rem',
                  background: dashboardCharts.length > 0 ? '#f44336' : '#ccc',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: dashboardCharts.length > 0 ? 'pointer' : 'not-allowed',
                  fontSize: '0.875rem'
                }}
              >
                Clear
              </button>
            </div>
          </div>

          {/* Dashboard Content */}
          {dashboardCharts.length === 0 ? (
            <div style={{
              background: 'white',
              padding: '3rem',
              borderRadius: '8px',
              boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
              textAlign: 'center',
              color: '#666'
            }}>
              <div style={{ fontSize: '3rem', marginBottom: '1rem' }}>📊</div>
              <h3 style={{ margin: '0 0 0.5rem 0', color: '#333' }}>Your Dashboard is Empty</h3>
              <p style={{ margin: 0 }}>
                Click on the <strong>+ Add to Dashboard</strong> button on any chart in the table tabs to pin it here.
                {savedDashboards.length > 0 && <><br />Or use the <strong>Load Dashboard</strong> button above to load a saved dashboard.</>}
              </p>
            </div>
          ) : (
            <div>

              <div style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fill, 175px)',
                gridAutoRows: '175px',
                gap: '0.5rem',
                gridAutoFlow: 'dense'
              }}>
                {dashboardCharts.map((chart, chartIndex) => {
                  const { tableName, columnName, countByTarget } = chart
                  const overrideKey = countByTarget ? `parent:${countByTarget}` : ROW_COUNT_KEY
                  const cardKey = getDashboardChartKey(chart)
                  const cardRef = registerDashboardCard(cardKey)
                  const aggregation = getAggregation(tableName, columnName, overrideKey)
                  const tableColor = getTableColor(tableName)
                  const displayTitle = getDisplayTitle(tableName, columnName)
                  const table = dataset.tables.find(t => t.name === tableName)
                  const indicatorNode = renderDashboardCountIndicator(chartIndex, tableName, columnName, overrideKey)
                  const columnMeta = getColumnMetadata(tableName, columnName)
                  const metaDisplayType = columnMeta?.display_type
                  const normalizedDisplayType =
                    aggregation?.normalized_display_type || aggregation?.display_type || metaDisplayType || ''

                  if (!aggregation) {
                    return (
                      <div
                        key={cardKey}
                        ref={cardRef}
                        data-dashboard-key={cardKey}
                        style={{
                          gridColumn: 'span 2',
                          minHeight: '175px',
                          background: 'white',
                          borderRadius: '8px',
                          boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
                          padding: '0.75rem',
                          display: 'flex',
                          flexDirection: 'column',
                          justifyContent: 'center',
                          alignItems: 'center',
                          border: tableColor ? `2px solid ${tableColor}15` : undefined
                        }}
                      >
                        {renderChartHeader({
                          title: displayTitle,
                          tooltip: `${displayTitle} is loading…`,
                          countIndicator: indicatorNode
                        })}
                        <div style={{ fontSize: '0.8rem', color: '#999', textAlign: 'center' }}>
                          Loading {displayTitle}…
                        </div>
                      </div>
                    )
                  }

                  if ((normalizedDisplayType === 'categorical' || metaDisplayType === 'survival_status') && aggregation.categories) {
                    const categoryCount = aggregation.categories.length
                    const viewPref = getViewPreference(tableName, columnName, categoryCount)
                    const allowPie = categoryCount <= MAX_PIE_CATEGORIES

                    if (viewPref === 'table') {
                      return (
                        <div
                          key={cardKey}
                          ref={cardRef}
                          data-dashboard-key={cardKey}
                          style={{ gridColumn: 'span 2', gridRow: 'span 2' }}
                        >
                          {renderTableView(
                            displayTitle,
                            tableName,
                            columnName,
                            tableColor,
                            aggregation,
                            overrideKey,
                            indicatorNode
                          )}
                        </div>
                      )
                    }

                    if (allowPie) {
                      return (
                        <div key={cardKey} ref={cardRef} data-dashboard-key={cardKey}>
                          {renderPieChart(
                            displayTitle,
                            tableName,
                            columnName,
                            tableColor,
                            aggregation,
                            overrideKey,
                            indicatorNode
                          )}
                        </div>
                      )
                    }

                    return (
                      <div key={cardKey} ref={cardRef} data-dashboard-key={cardKey} style={{ gridColumn: 'span 2' }}>
                        {renderBarChart(
                          displayTitle,
                          tableName,
                          columnName,
                          tableColor,
                          aggregation,
                          overrideKey,
                          indicatorNode
                        )}
                      </div>
                    )
                  } else if (metaDisplayType === 'survival_time') {
                    const view = getSurvivalViewPreference(tableName, columnName)
                    const toggleButton = (
                      <button
                        type="button"
                        onClick={event => {
                          event.stopPropagation()
                          toggleSurvivalViewPreference(tableName, columnName)
                        }}
                        style={{
                          border: 'none',
                          background: '#f0f0f0',
                          color: '#333',
                          borderRadius: '50%',
                          width: '20px',
                          height: '20px',
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          fontSize: '0.8rem',
                          fontWeight: 700,
                          cursor: 'pointer',
                          lineHeight: 1
                        }}
                        title={view === 'km' ? 'Show histogram' : 'Show survival curve'}
                      >
                        {view === 'km' ? '📊' : '┐'}
                      </button>
                    )

                    if (view === 'km') {
                      return (
                        <div key={cardKey} ref={cardRef} data-dashboard-key={cardKey} style={{ gridColumn: 'span 2', gridRow: 'span 2' }}>
                          {renderSurvivalChart(
                            displayTitle,
                            tableName,
                            columnName,
                            tableColor,
                            aggregation,
                            overrideKey,
                            indicatorNode,
                            toggleButton,
                            false
                          )}
                        </div>
                      )
                    }

                    return (
                      <div key={cardKey} ref={cardRef} data-dashboard-key={cardKey} style={{ gridColumn: 'span 2' }}>
                        {renderHistogram(
                          displayTitle,
                          tableName,
                          columnName,
                          tableColor,
                          aggregation,
                          overrideKey,
                          indicatorNode,
                          toggleButton
                        )}
                      </div>
                    )
                  } else if (normalizedDisplayType === 'numeric' && aggregation.histogram) {
                    return (
                      <div key={cardKey} ref={cardRef} data-dashboard-key={cardKey} style={{ gridColumn: 'span 2' }}>
                        {renderHistogram(
                          displayTitle,
                          tableName,
                          columnName,
                          tableColor,
                          aggregation,
                          overrideKey,
                          indicatorNode
                        )}
                      </div>
                    )
                  } else if (aggregation.display_type === 'geographic' && aggregation.categories) {
                    return (
                      <div key={cardKey} ref={cardRef} data-dashboard-key={cardKey} style={{ gridColumn: 'span 4' }}>
                        {renderMapChart(
                          displayTitle,
                          tableName,
                          columnName,
                          tableColor,
                          aggregation,
                          overrideKey,
                          indicatorNode
                        )}
                      </div>
                    )
                  }
                  return null
                })}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Chart Grid - Grouped by Table */}
      {dataset.tables
        .filter(table => table.name === activeTab)
        .map(table => {
        const tableAggregations = getAggregationsForTable(table.name)
        if (!tableAggregations) return null

        // Sort aggregations by display priority (if available from metadata)
        const sortedAggregations = [...tableAggregations].sort((a, b) => {
          const metaA = getColumnMetadata(table.name, a.column_name)
          const metaB = getColumnMetadata(table.name, b.column_name)
          const priorityA = metaA?.display_priority || 0
          const priorityB = metaB?.display_priority || 0
          return priorityB - priorityA
        })

        // Filter out hidden columns
        const visibleAggregations = sortedAggregations.filter(agg => {
          const metadata = getColumnMetadata(table.name, agg.column_name)
          return !metadata?.is_hidden
        })

        if (visibleAggregations.length === 0) return null

        const tableColor = getTableColor(table.name)
        const primaryAggregation = visibleAggregations[0]
        const tableRowCount = primaryAggregation?.total_rows ?? table.rowCount ?? 0

        // Get baseline (unfiltered) row count for this table
        const baselineTableAggs = baselineAggregations[table.name] || []
        const baselineSample = baselineTableAggs.length > 0 ? baselineTableAggs[0] : undefined
        const baselineMatches = metricsMatch(baselineSample, primaryAggregation)
        const baselineRowCount = baselineMatches
          ? baselineSample?.total_rows ?? tableRowCount
          : null

        // Get filter counts for this table
        const effectiveFilters = getAllEffectiveFilters()
        const tableFilters = effectiveFilters[table.name] || { direct: [], propagated: [] }
        const directFilterCount = tableFilters.direct.length
        const propagatedFilterCount = tableFilters.propagated.length
        const hasTableFilters = directFilterCount > 0 || propagatedFilterCount > 0

        // Calculate maximum path length for transitive relationships (2+ hops only)
        let maxPathLength = 0
        if (propagatedFilterCount > 0 && dataset?.tables) {
          for (const filter of tableFilters.propagated) {
            if (filter.tableName) {
              const path = findRelationshipPath(table.name, filter.tableName, dataset.tables)
              if (path && path.length > 1) {
                const pathLength = path.length - 1 // Number of hops
                // Only track paths with 2+ hops (truly transitive)
                if (pathLength >= 2) {
                  maxPathLength = Math.max(maxPathLength, pathLength)
                }
              }
            }
          }
        }

        const metricLabels = getMetricLabels(primaryAggregation)
        const parentOptions = ancestorOptions[table.name] || []
        const countByValue = getCountByValueForTable(table.name)

        return (
          <div key={table.name} style={{ marginBottom: '2.5rem' }}>
            {/* Table Section Header */}
            <div style={{
              background: `linear-gradient(135deg, ${tableColor}15, ${tableColor}05)`,
              border: `2px solid ${tableColor}40`,
              borderRadius: '8px',
              padding: '0.75rem 1.25rem',
              marginBottom: '1rem',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              gap: '1rem'
            }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                {parentOptions.length > 0 ? renderTabCountIndicator(table.name, countByValue) : (
                  <div style={{
                    background: tableColor,
                    color: 'white',
                    width: '8px',
                    height: '40px',
                    borderRadius: '4px'
                  }} />
                )}
                <div>
                  <h3 style={{
                    margin: 0,
                    fontSize: '1.1rem',
                    fontWeight: 600,
                    color: '#333'
                  }}>
                    {table.displayName || table.name}
                  </h3>
                  <div style={{
                    fontSize: '0.8rem',
                    color: '#666',
                    marginTop: '0.2rem'
                  }}>
                    {hasTableFilters && baselineRowCount !== null ? (
                      <>
                        <span style={{ color: '#E65100', fontWeight: 600 }}>
                          {tableRowCount.toLocaleString()}
                        </span>
                        <span style={{ color: '#999' }}> / </span>
                        <span>{baselineRowCount.toLocaleString()}</span>
                        <span style={{
                          marginLeft: '0.3rem',
                          padding: '0.1rem 0.4rem',
                          background: '#FF9800',
                          color: 'white',
                          borderRadius: '8px',
                          fontSize: '0.7rem',
                          fontWeight: 600
                        }}>
                          {baselineRowCount > 0 ? ((tableRowCount / baselineRowCount) * 100).toFixed(1) : '0'}%
                        </span>
                        <span> {metricLabels.short} · {visibleAggregations.length} columns</span>
                        <span style={{ color: '#999', fontSize: '0.75rem' }}> (by {getCountByLabelFromCacheKey(table.name, countByValue)})</span>
                      </>
                    ) : (
                      <>
                        {tableRowCount.toLocaleString()} {metricLabels.short} · {visibleAggregations.length} columns
                        <span style={{ color: '#999', fontSize: '0.75rem' }}> (by {getCountByLabelFromCacheKey(table.name, countByValue)})</span>
                      </>
                    )}
                  </div>
                </div>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                {/* Filter badges */}
                {directFilterCount > 0 && (
                  <div
                    style={{
                      background: '#1976D2',
                      color: 'white',
                      fontSize: '0.7rem',
                      padding: '0.3rem 0.6rem',
                      borderRadius: '4px',
                      fontWeight: 600
                    }}
                    title={`${directFilterCount} direct filter${directFilterCount > 1 ? 's' : ''} applied`}
                  >
                    {directFilterCount} filter{directFilterCount > 1 ? 's' : ''}
                  </div>
                )}
                {propagatedFilterCount > 0 && (
                  <div
                    style={{
                      background: '#64B5F6',
                      color: 'white',
                      fontSize: '0.7rem',
                      padding: '0.3rem 0.6rem',
                      borderRadius: '4px',
                      fontWeight: 600,
                      fontStyle: 'italic'
                    }}
                    title={`${propagatedFilterCount} filter${propagatedFilterCount > 1 ? 's' : ''} propagated from related tables${maxPathLength > 0 ? ` (max ${maxPathLength} hop${maxPathLength > 1 ? 's' : ''})` : ''}`}
                  >
                    +{propagatedFilterCount} linked{maxPathLength > 0 ? ` (${maxPathLength}-hop)` : ''}
                  </div>
                )}
                {/* Add All Charts button */}
                <button
                  onClick={() => {
                    addAllChartsToTable(table.name)
                  }}
                  style={{
                    padding: '0.3rem 0.6rem',
                    background: '#4CAF50',
                    color: 'white',
                    border: 'none',
                    borderRadius: '4px',
                    cursor: 'pointer',
                    fontSize: '0.7rem',
                    fontWeight: 600,
                    transition: 'background 0.2s'
                  }}
                  onMouseEnter={(e) => {
                    e.currentTarget.style.background = '#45a049'
                  }}
                  onMouseLeave={(e) => {
                    e.currentTarget.style.background = '#4CAF50'
                  }}
                  title="Add all charts from this table to dashboard"
                >
                  + Add All
                </button>
                <div style={{
                  background: tableColor,
                  color: 'white',
                  fontSize: '0.7rem',
                  padding: '0.3rem 0.6rem',
                  borderRadius: '4px',
                  fontWeight: 600
                }}>
                  {table.name}
                </div>
              </div>
            </div>

            {/* Table Charts */}
            <div style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fill, 175px)',
              gridAutoRows: '175px',
              gap: '0.5rem',
              gridAutoFlow: 'dense'
            }}>
              {visibleAggregations.map(agg => {
                const displayTitle = getDisplayTitle(table.name, agg.column_name)
                const cacheKey = getEffectiveCacheKeyForChart(table.name, agg.column_name)
                const defaultKey = getCountByCacheKey(table.name)
                const aggregationForChart = cacheKey === defaultKey ? agg : undefined
                const columnMeta = getColumnMetadata(table.name, agg.column_name)
                const metaDisplayType = columnMeta?.display_type
                const normalizedDisplayType =
                  agg?.normalized_display_type || agg?.display_type || metaDisplayType || ''

                if ((normalizedDisplayType === 'categorical' || metaDisplayType === 'survival_status') && agg.categories) {
                  const categoryCount = agg.categories.length
                  const viewPref = getViewPreference(table.name, agg.column_name, categoryCount)
                  const allowPie = categoryCount <= MAX_PIE_CATEGORIES

                  if (viewPref === 'table') {
                    return (
                      <div key={`${table.name}_${agg.column_name}`} style={{ gridColumn: 'span 2', gridRow: 'span 2' }}>
                        {renderTableView(displayTitle, table.name, agg.column_name, tableColor, aggregationForChart, cacheKey)}
                      </div>
                    )
                  }

                  if (allowPie) {
                    return (
                      <div key={`${table.name}_${agg.column_name}`}>
                        {renderPieChart(displayTitle, table.name, agg.column_name, tableColor, aggregationForChart, cacheKey)}
                      </div>
                    )
                  }

                  return (
                    <div key={`${table.name}_${agg.column_name}`} style={{ gridColumn: 'span 2' }}>
                      {renderBarChart(displayTitle, table.name, agg.column_name, tableColor, aggregationForChart, cacheKey)}
                    </div>
                  )
                } else if (metaDisplayType === 'survival_time') {
                  const view = getSurvivalViewPreference(table.name, agg.column_name)
                  const toggleButton = (
                    <button
                      type="button"
                      onClick={event => {
                        event.stopPropagation()
                        toggleSurvivalViewPreference(table.name, agg.column_name)
                      }}
                      style={{
                        border: 'none',
                        background: '#f0f0f0',
                        color: '#333',
                        borderRadius: '50%',
                        width: '20px',
                        height: '20px',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        fontSize: '0.8rem',
                        fontWeight: 700,
                        cursor: 'pointer',
                        lineHeight: 1
                      }}
                      title={view === 'km' ? 'Show histogram' : 'Show survival curve'}
                    >
                      {view === 'km' ? '📊' : '┐'}
                    </button>
                  )

                  if (view === 'km') {
                    return (
                      <div key={`${table.name}_${agg.column_name}_km`} style={{ gridColumn: 'span 2', gridRow: 'span 2' }}>
                        {renderSurvivalChart(displayTitle, table.name, agg.column_name, tableColor, aggregationForChart, cacheKey, undefined, toggleButton, false)}
                      </div>
                    )
                  }

                  return (
                    <div key={`${table.name}_${agg.column_name}_hist`} style={{ gridColumn: 'span 2' }}>
                      {renderHistogram(displayTitle, table.name, agg.column_name, tableColor, aggregationForChart, cacheKey, undefined, toggleButton)}
                    </div>
                  )
                } else if (normalizedDisplayType === 'numeric' && agg.histogram) {
                  return (
                    <div key={`${table.name}_${agg.column_name}`} style={{ gridColumn: 'span 2' }}>
                      {renderHistogram(displayTitle, table.name, agg.column_name, tableColor, aggregationForChart, cacheKey)}
                    </div>
                  )
                } else if (agg.display_type === 'geographic' && agg.categories) {
                  return (
                    <div key={`${table.name}_${agg.column_name}`} style={{ gridColumn: 'span 4' }}>
                      {renderMapChart(displayTitle, table.name, agg.column_name, tableColor, aggregationForChart, cacheKey)}
                    </div>
                  )
                }
                return null
              })}
            </div>
          </div>
        )
      })}
    </div>
  )
}

export default DatasetExplorer
