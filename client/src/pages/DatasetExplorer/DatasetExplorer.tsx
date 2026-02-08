import React, { useState, useEffect, useRef, useMemo } from 'react'
import { useParams, useNavigate, useLocation } from 'react-router-dom'
import SafeHtml from '../../components/SafeHtml'
import api from '../../services/api'
import type { MetricPathSegment } from '../../types'
import {
  findRelationshipPath,
  type Filter,
  ROW_COUNT_KEY,
  unwrapNot,
  rangeKey,
  rangesEqual,
  getFilterCountKey,
  filterContainsColumn,
  migrateFiltersToCurrentSchema,
} from '../../utils/filterHelpers'
import { encodeState, decodeState } from '../../utils/urlHelpers'
import {
  type CountBySelection
} from '../../utils/presetHelpers'
import { useFilterPresets } from './hooks/useFilterPresets'
import { useViewPreferences } from './hooks/useViewPreferences'
import { useDashboard } from './hooks/useDashboard'
import { ChartProvider } from './components/ChartContext'
import { PieChart } from './components/PieChart'
import { BarChart } from './components/BarChart'
import { TableViewChart } from './components/TableViewChart'
import { HistogramChart } from './components/HistogramChart'
import { SurvivalChart } from './components/SurvivalChart'
import { MapChart } from './components/MapChart'
import { ActiveFilters } from './components/ActiveFilters'
import { ChartSettingsMenu } from './components/ChartSettingsMenu'
import { SavedFiltersBar } from './components/SavedFiltersBar'
import { PresetDialogs } from './components/PresetDialogs'
import { DashboardDialogs } from './components/DashboardDialogs'
import {
  MAX_PIE_CATEGORIES,
  DASHBOARD_SCOPE_KEY,
  CACHE_TTL_MS,
  CACHE_MAX_ENTRIES_PER_TABLE,
  MAX_ANCESTOR_DEPTH,
  persistChartOverrides,
  loadChartOverrides,
  type ColumnMetadata,
  type NumericStats,
  type HistogramBin,
  type SurvivalCurvePoint,
  type ColumnAggregation,
  type Table,
  type Dataset,
  type AncestorOption,
  type AggregationCacheEntry,
  type SurvivalCacheEntry,
} from './types'

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
  const [error, setError] = useState<string | null>(null)
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

  // Filter preset management (extracted hook)
  const {
    presets,
    showSavePresetDialog, setShowSavePresetDialog,
    showManagePresetsDialog, setShowManagePresetsDialog,
    showPresetsDropdown, setShowPresetsDropdown,
    presetNameInput, setPresetNameInput,
    editingPresetId, setEditingPresetId,
    savePreset, applyPreset, deletePreset, renamePreset,
    exportPresets, importPresets,
  } = useFilterPresets({
    identifier,
    filters,
    countBySelections,
    setFilters,
    setCountBySelections,
  })

  // View preferences (extracted hook)
  const {
    showPercentageLabels, setShowPercentageLabels,
    showSettingsMenu, setShowSettingsMenu,
    settingsButtonRef, settingsMenuRef,
    getViewPreference, toggleViewPreference,
    getSurvivalViewPreference, toggleSurvivalViewPreference,
  } = useViewPreferences({ identifier })

  // Tab navigation state: track which table tab is currently active
  const [activeTab, setActiveTab] = useState<string | null>(null)

  // Dashboard management (extracted hook)
  const {
    dashboardCharts, setDashboardCharts,
    visibleDashboardKeys,
    savedDashboards,
    activeDashboardId, setActiveDashboardId,
    showSaveDashboardDialog, setShowSaveDashboardDialog,
    showLoadDashboardDialog, setShowLoadDashboardDialog,
    showManageDashboardsDialog, setShowManageDashboardsDialog,
    newDashboardName, setNewDashboardName,
    editingDashboardId, setEditingDashboardId,
    setEditingDashboardName,
    getDashboardChartKey, registerDashboardCard,
    saveDashboard, loadDashboard, deleteDashboard, renameDashboard,
  } = useDashboard({ identifier })

  const [chartCountOverrides, setChartCountOverrides] = useState<Record<string, string>>({})
  const [activeCountMenuKey, setActiveCountMenuKey] = useState<string | null>(null)
  const [ancestorOptions, setAncestorOptions] = useState<Record<string, AncestorOption[]>>({})
  const survivalRequests = useRef<Set<string>>(new Set())

  // Track if filters have been initialized from URL to prevent overwriting
  const filtersInitialized = useRef(false)
  const isUpdatingURL = useRef(false)
  const countByInitialized = useRef(false)
  const previousCountByRef = useRef<Record<string, CountBySelection>>({})
  const chartOverridesInitialized = useRef(false)

  // Helper functions for URL persistence
  const serializeFilters = (filters: Filter[]): string => {
    return encodeState(filters)
  }

  const deserializeFilters = (encoded: string): Filter[] | null => {
    return decodeState<Filter[]>(encoded)
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
    return encodeState(selections)
  }

  const deserializeCountBySelections = (encoded: string): Record<string, CountBySelection> | null => {
    return decodeState<Record<string, CountBySelection>>(encoded)
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
    if (!identifier) return
    try {
      persistChartOverrides(localStorage, identifier, overrides)
    } catch (error) {
      console.error('Failed to persist chart overrides:', error)
    }
  }

  const loadChartOverridesFromLocalStorage = (): Record<string, string> | null => {
    if (!identifier) return null
    try {
      return loadChartOverrides(localStorage, identifier)
    } catch (error) {
      console.error('Failed to load chart overrides from localStorage:', error)
      return null
    }
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


  useEffect(() => {
    if (!countByReady) return
    loadDataset()
  }, [id, database, countByReady])


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
      setError(null)
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
      setError(error instanceof Error ? error.message : 'Failed to load dataset')
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
      setError(error instanceof Error ? error.message : 'Failed to load table aggregations')
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
      setError(error instanceof Error ? error.message : 'Failed to load column metadata')
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

  const getCountByLabelFromCacheKey = (_tableName: string, cacheKey: string): string => {
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

  const getCountIndicatorColor = (tableName: string, _cacheKey: string): string => {
    // Color bar represents the data source table
    return getTableColor(tableName)
  }

  const getCountByTableColor = (_tableName: string, cacheKey: string): string | null => {
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
    actions,
    isListColumn
  }: {
    title: string
    tooltip?: string
    countIndicator: React.ReactNode
    actions?: React.ReactNode
    isListColumn?: boolean
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
          {isListColumn && (
            <span
              style={{
                marginLeft: '0.25rem',
                fontSize: '0.65rem',
                opacity: 0.7
              }}
              title="List column - items can appear in multiple rows"
            >
              📋
            </span>
          )}
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

  /**
   * Determine which table a filter applies to for cache lookups / backend requests.
   *
   * Note: {@link Filter.countByKey} is purely client-side metadata that scopes UI controls.
   * The backend only inspects {@link Filter.tableName} to build JOIN paths, so we never rewrite
   * tableName when users select different ancestor count targets.
   */
  const getFilterTableNameForCacheKey = (filter: Filter): string | undefined => filter.tableName

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
      const orFilters = nextRanges.map(range => ({ column: columnName, operator: 'between' as const, value: [range.start, range.end] }))
      return [
        ...without,
        { column: columnName, or: orFilters, tableName, countByKey: countKey }
      ]
    })
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
    if (!activeCountMenuKey) return
    const handleClick = () => setActiveCountMenuKey(null)
    document.addEventListener('click', handleClick)
    return () => document.removeEventListener('click', handleClick)
  }, [activeCountMenuKey])

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

  const chartContextValue = {
    getEffectiveCacheKeyForChart,
    getAggregation,
    getBaselineAggregation,
    getMetricLabels,
    metricsMatch,
    getColumnMetadata,
    getTableDisplayNameByName,
    formatMetricPath,
    getCountByOptions,
    getCountByCacheKey,
    handleCountByChange,
    handleChartCountOverrideChange,
    activeCountMenuKey,
    setActiveCountMenuKey,
    getTableColor,
    getCountIndicatorColor,
    getCountByTableColor,
    getCountByLabelFromCacheKey,
    activeFilterMenu,
    setActiveFilterMenu,
    filters,
    setFilters,
    hasColumnFilter,
    isValueFiltered,
    normalizeFilterValue,
    toggleFilter,
    clearColumnFilter,
    removeColumnFilters,
    rangeSelections,
    customRangeInputs,
    toggleRangeFilter,
    isRangeFiltered,
    handleCustomRangeChange,
    applyCustomRange,
    updateColumnRanges,
    formatRangeValue,
    getDisplayHistogram,
    showPercentageLabels,
    toggleViewPreference,
    getSurvivalViewPreference,
    toggleSurvivalViewPreference,
    findTable: (tableName: string) => dataset?.tables.find(t => t.name === tableName),
    findSurvivalStatusColumn,
    ensureSurvivalCurve,
    getSurvivalCurve,
    isOnDashboard,
    toggleDashboard,
  }

  if (loading) return <p>Loading explorer...</p>
  if (error) return <div role="alert" style={{ padding: '2rem', color: 'red' }}>Error: {error}</div>
  if (!dataset) return <p>Dataset not found</p>

  return (
    <ChartProvider value={chartContextValue}>
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
            <ChartSettingsMenu
              showPercentageLabels={showPercentageLabels}
              setShowPercentageLabels={setShowPercentageLabels}
              onClose={() => setShowSettingsMenu(false)}
              identifier={identifier}
              menuRef={settingsMenuRef}
            />
          )}
        </div>
      </div>

      <SavedFiltersBar
        presets={presets}
        showPresetsDropdown={showPresetsDropdown}
        setShowPresetsDropdown={setShowPresetsDropdown}
        setShowManagePresetsDialog={setShowManagePresetsDialog}
      />

      <ActiveFilters
        filters={filters}
        setFilters={setFilters}
        onSaveFilter={() => setShowSavePresetDialog(true)}
        onClearFilters={clearFilters}
        getTableColor={getTableColor}
        getFilterTableNameForCacheKey={getFilterTableNameForCacheKey}
        targetFromCacheKey={targetFromCacheKey}
        formatRangeValue={formatRangeValue}
        clearColumnFilter={clearColumnFilter}
        datasetTables={dataset?.tables}
      />

      <PresetDialogs
        presets={presets}
        showSavePresetDialog={showSavePresetDialog}
        setShowSavePresetDialog={setShowSavePresetDialog}
        showPresetsDropdown={showPresetsDropdown}
        setShowPresetsDropdown={setShowPresetsDropdown}
        showManagePresetsDialog={showManagePresetsDialog}
        setShowManagePresetsDialog={setShowManagePresetsDialog}
        presetNameInput={presetNameInput}
        setPresetNameInput={setPresetNameInput}
        editingPresetId={editingPresetId}
        setEditingPresetId={setEditingPresetId}
        savePreset={savePreset}
        applyPreset={applyPreset}
        deletePreset={deletePreset}
        renamePreset={renamePreset}
        exportPresets={exportPresets}
        importPresets={importPresets}
      />


      <DashboardDialogs
        savedDashboards={savedDashboards}
        activeDashboardId={activeDashboardId}
        showSaveDashboardDialog={showSaveDashboardDialog}
        setShowSaveDashboardDialog={setShowSaveDashboardDialog}
        showLoadDashboardDialog={showLoadDashboardDialog}
        setShowLoadDashboardDialog={setShowLoadDashboardDialog}
        showManageDashboardsDialog={showManageDashboardsDialog}
        setShowManageDashboardsDialog={setShowManageDashboardsDialog}
        newDashboardName={newDashboardName}
        setNewDashboardName={setNewDashboardName}
        editingDashboardId={editingDashboardId}
        setEditingDashboardId={setEditingDashboardId}
        setEditingDashboardName={setEditingDashboardName}
        saveDashboard={saveDashboard}
        loadDashboard={loadDashboard}
        deleteDashboard={deleteDashboard}
        renameDashboard={renameDashboard}
      />

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
                        <div key={cardKey} ref={cardRef} data-dashboard-key={cardKey} style={{ gridColumn: 'span 2', gridRow: 'span 2' }}>
                          <TableViewChart title={displayTitle} tableName={tableName} field={columnName} tableColor={tableColor} aggregationOverride={aggregation} cacheKeyOverride={overrideKey} countIndicatorOverride={indicatorNode} />
                        </div>
                      )
                    }

                    if (allowPie) {
                      return (
                        <div key={cardKey} ref={cardRef} data-dashboard-key={cardKey}>
                          <PieChart title={displayTitle} tableName={tableName} field={columnName} tableColor={tableColor} aggregationOverride={aggregation} cacheKeyOverride={overrideKey} countIndicatorOverride={indicatorNode} />
                        </div>
                      )
                    }

                    return (
                      <div key={cardKey} ref={cardRef} data-dashboard-key={cardKey} style={{ gridColumn: 'span 2' }}>
                        <BarChart title={displayTitle} tableName={tableName} field={columnName} tableColor={tableColor} aggregationOverride={aggregation} cacheKeyOverride={overrideKey} countIndicatorOverride={indicatorNode} />
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
                          <SurvivalChart title={displayTitle} tableName={tableName} field={columnName} tableColor={tableColor} aggregationOverride={aggregation} cacheKeyOverride={overrideKey} countIndicatorOverride={indicatorNode} extraActions={toggleButton} showHistogram={false} />
                        </div>
                      )
                    }

                    return (
                      <div key={cardKey} ref={cardRef} data-dashboard-key={cardKey} style={{ gridColumn: 'span 2' }}>
                        <HistogramChart title={displayTitle} tableName={tableName} field={columnName} tableColor={tableColor} aggregationOverride={aggregation} cacheKeyOverride={overrideKey} countIndicatorOverride={indicatorNode} extraActions={toggleButton} />
                      </div>
                    )
                  } else if (normalizedDisplayType === 'numeric' && aggregation.histogram) {
                    return (
                      <div key={cardKey} ref={cardRef} data-dashboard-key={cardKey} style={{ gridColumn: 'span 2' }}>
                        <HistogramChart title={displayTitle} tableName={tableName} field={columnName} tableColor={tableColor} aggregationOverride={aggregation} cacheKeyOverride={overrideKey} countIndicatorOverride={indicatorNode} />
                      </div>
                    )
                  } else if (aggregation.display_type === 'geographic' && aggregation.categories) {
                    return (
                      <div key={cardKey} ref={cardRef} data-dashboard-key={cardKey} style={{ gridColumn: 'span 4' }}>
                        <MapChart title={displayTitle} tableName={tableName} field={columnName} tableColor={tableColor} aggregationOverride={aggregation} cacheKeyOverride={overrideKey} countIndicatorOverride={indicatorNode} />
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
                    <div
                      data-testid={`row-count-${table.name}`}
                      style={{
                        fontSize: '0.8rem',
                        color: '#666',
                        marginTop: '0.2rem'
                      }}>
                      {hasTableFilters && baselineRowCount !== null ? (
                        <>
                          <span data-testid={`filtered-count-${table.name}`} style={{ color: '#E65100', fontWeight: 600 }}>
                            {tableRowCount.toLocaleString()}
                          </span>
                          <span style={{ color: '#999' }}> / </span>
                          <span data-testid={`total-count-${table.name}`}>{baselineRowCount.toLocaleString()}</span>
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
                          <span data-testid={`total-count-${table.name}`}>{tableRowCount.toLocaleString()}</span> {metricLabels.short} · {visibleAggregations.length} columns
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
                          <TableViewChart title={displayTitle} tableName={table.name} field={agg.column_name} tableColor={tableColor} aggregationOverride={aggregationForChart} cacheKeyOverride={cacheKey} />
                        </div>
                      )
                    }

                    if (allowPie) {
                      return (
                        <div key={`${table.name}_${agg.column_name}`}>
                          <PieChart title={displayTitle} tableName={table.name} field={agg.column_name} tableColor={tableColor} aggregationOverride={aggregationForChart} cacheKeyOverride={cacheKey} />
                        </div>
                      )
                    }

                    return (
                      <div key={`${table.name}_${agg.column_name}`} style={{ gridColumn: 'span 2' }}>
                        <BarChart title={displayTitle} tableName={table.name} field={agg.column_name} tableColor={tableColor} aggregationOverride={aggregationForChart} cacheKeyOverride={cacheKey} />
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
                          <SurvivalChart title={displayTitle} tableName={table.name} field={agg.column_name} tableColor={tableColor} aggregationOverride={aggregationForChart} cacheKeyOverride={cacheKey} extraActions={toggleButton} showHistogram={false} />
                        </div>
                      )
                    }

                    return (
                      <div key={`${table.name}_${agg.column_name}_hist`} style={{ gridColumn: 'span 2' }}>
                        <HistogramChart title={displayTitle} tableName={table.name} field={agg.column_name} tableColor={tableColor} aggregationOverride={aggregationForChart} cacheKeyOverride={cacheKey} extraActions={toggleButton} />
                      </div>
                    )
                  } else if (normalizedDisplayType === 'numeric' && agg.histogram) {
                    return (
                      <div key={`${table.name}_${agg.column_name}`} style={{ gridColumn: 'span 2' }}>
                        <HistogramChart title={displayTitle} tableName={table.name} field={agg.column_name} tableColor={tableColor} aggregationOverride={aggregationForChart} cacheKeyOverride={cacheKey} />
                      </div>
                    )
                  } else if (agg.display_type === 'geographic' && agg.categories) {
                    return (
                      <div key={`${table.name}_${agg.column_name}`} style={{ gridColumn: 'span 4' }}>
                        <MapChart title={displayTitle} tableName={table.name} field={agg.column_name} tableColor={tableColor} aggregationOverride={aggregationForChart} cacheKeyOverride={cacheKey} />
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
    </ChartProvider>
  )
}

export default DatasetExplorer
