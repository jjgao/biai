import { useState, useEffect, useRef, useCallback } from 'react'
import api from '../../../services/api'
import { type Filter, ROW_COUNT_KEY, findRelationshipPath } from '../../../utils/filterHelpers'
import type { CountBySelection } from '../../../utils/presetHelpers'
import type { DashboardChart } from './useDashboard'
import {
  buildFiltersKey,
  buildAncestorOptions,
} from '../utils'
import {
  CACHE_TTL_MS,
  CACHE_MAX_ENTRIES_PER_TABLE,
  type ColumnMetadata,
  type ColumnAggregation,
  type SurvivalCurvePoint,
  type Table,
  type Dataset,
  type AncestorOption,
  type AggregationCacheEntry,
  type SurvivalCacheEntry,
} from '../types'

interface UseDatasetLoaderArgs {
  id: string | undefined
  database: string | undefined
  identifier: string | undefined
  isDatabaseMode: boolean
  filters: Filter[]
  currentFiltersKey: string
  countBySelections: Record<string, CountBySelection>
  countByReady: boolean
  getCountByCacheKey: (tableName: string, override?: string) => string
  chartCountOverrides: Record<string, string>
  dashboardCharts: DashboardChart[]
  getDashboardChartKey: (chart: DashboardChart) => string
  visibleDashboardKeys: Record<string, boolean>
  previousCountByRef: React.MutableRefObject<Record<string, CountBySelection>>
  onDatasetLoaded: () => void
  onAncestorOptionsChanged?: (options: Record<string, AncestorOption[]>) => void
}

export function useDatasetLoader({
  id,
  database,
  identifier,
  isDatabaseMode,
  filters,
  currentFiltersKey,
  countBySelections,
  countByReady,
  getCountByCacheKey,
  chartCountOverrides,
  dashboardCharts,
  getDashboardChartKey,
  visibleDashboardKeys,
  previousCountByRef,
  onDatasetLoaded,
  onAncestorOptionsChanged,
}: UseDatasetLoaderArgs) {
  const [dataset, setDataset] = useState<Dataset | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [columnMetadata, setColumnMetadata] = useState<Record<string, ColumnMetadata[]>>({})
  const [aggregations, setAggregations] = useState<Record<string, Record<string, AggregationCacheEntry>>>({})
  const [survivalCurves, setSurvivalCurves] = useState<Record<string, Record<string, SurvivalCacheEntry>>>({})
  const [baselineAggregations, setBaselineAggregations] = useState<Record<string, ColumnAggregation[]>>({})
  const [ancestorOptions, setAncestorOptions] = useState<Record<string, AncestorOption[]>>({})

  const survivalRequests = useRef<Set<string>>(new Set())

  // Derived values
  const usesDatabaseAPI = isDatabaseMode ? true : dataset?.database_type === 'connected'
  const databaseIdentifier = isDatabaseMode ? identifier : dataset?.database_name
  const datasetIdentifier = dataset?.id

  // ── Cache helpers ─────────────────────────────────────────────────

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

  const getBaselineAggregation = (tableName: string, columnName: string): ColumnAggregation | undefined => {
    const tableAggs = baselineAggregations[tableName]
    if (!tableAggs) return undefined
    return tableAggs.find(agg => agg.column_name === columnName)
  }

  const getAggregation = (tableName: string, columnName: string, overrideKey?: string): ColumnAggregation | undefined => {
    const tableAggs = getAggregationsForTable(tableName, overrideKey)
    if (!tableAggs) return undefined
    return tableAggs.find(agg => agg.column_name === columnName)
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

  // ── Display helpers ───────────────────────────────────────────────

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

  const getTableColor = (tableName: string): string => {
    if (!dataset?.tables) return '#9E9E9E'
    const tableIndex = dataset.tables.findIndex(t => t.name === tableName)
    const colors = ['#2196F3', '#4CAF50', '#FF9800', '#9C27B0', '#F44336', '#00BCD4', '#FFC107', '#E91E63']
    return tableIndex >= 0 ? colors[tableIndex % colors.length] : '#9E9E9E'
  }

  const getTableChartCount = (tableName: string): number => {
    const tableAggs = baselineAggregations[tableName] || []
    if (tableAggs.length === 0) return 0
    return tableAggs.filter(agg => {
      const meta = getColumnMetadata(tableName, agg.column_name)
      return !meta?.is_hidden
    }).length
  }

  // ── Filter helpers (need dataset) ─────────────────────────────────

  const getAllEffectiveFilters = (): Record<string, { direct: Filter[]; propagated: Filter[] }> => {
    if (!dataset) return {}
    const result: Record<string, { direct: Filter[]; propagated: Filter[] }> = {}
    for (const table of dataset.tables) {
      result[table.name] = { direct: [], propagated: [] }
    }
    for (const filter of filters) {
      const filterTableName = filter.tableName
      if (!filterTableName) continue
      for (const table of dataset.tables) {
        if (table.name === filterTableName) {
          result[table.name].direct.push(filter)
        } else {
          const path = findRelationshipPath(table.name, filterTableName, dataset.tables)
          if (path !== null) {
            result[table.name].propagated.push(filter)
          }
        }
      }
    }
    return result
  }

  // ── Survival curve helpers ────────────────────────────────────────

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
      const params: Record<string, any> = { timeColumn, statusColumn }
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
          [table.name]: { ...tableCache, [entryKey]: nextEntry }
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

  // ── Loading functions ─────────────────────────────────────────────

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
      const activeFilters = options?.tableFilters !== undefined ? options.tableFilters : filters
      const requestFiltersKey = buildFiltersKey(activeFilters)
      const params: Record<string, any> = activeFilters.length > 0 ? { filters: JSON.stringify(activeFilters) } : {}
      const shouldUseDbAPI = options?.useDbAPI !== undefined ? options.useDbAPI : usesDatabaseAPI
      const dbId = options?.dbName || databaseIdentifier
      const datasetParam = options?.datasetId || datasetIdentifier

      const apiPath = shouldUseDbAPI
        ? `/databases/${dbId}/tables/${tableId}/aggregations`
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

        return { ...prev, [tableName]: nextTableCache }
      })
      if (options?.storeBaseline && cacheKey === ROW_COUNT_KEY) {
        setBaselineAggregations(prev => ({ ...prev, [tableName]: response.data.aggregations }))
      }
    } catch (err) {
      console.error('Failed to load table aggregations:', err)
      setError(err instanceof Error ? err.message : 'Failed to load table aggregations')
    }
  }

  const loadColumnMetadata = async (
    tableId: string,
    tableName: string,
    options?: { useDbAPI?: boolean; dbName?: string; datasetId?: string }
  ) => {
    try {
      const shouldUseDbAPI = options?.useDbAPI !== undefined ? options.useDbAPI : usesDatabaseAPI
      const dbId = options?.dbName || databaseIdentifier
      const datasetParam = options?.datasetId || datasetIdentifier

      const apiPath = shouldUseDbAPI
        ? `/databases/${dbId}/tables/${tableId}/columns`
        : `/datasets/${identifier}/tables/${tableId}/columns`
      const response = await api.get(apiPath, {
        params: shouldUseDbAPI && datasetParam ? { datasetId: datasetParam } : undefined
      })
      setColumnMetadata(prev => ({ ...prev, [tableName]: response.data.columns }))
    } catch (err) {
      console.error('Failed to load column metadata:', err)
      setError(err instanceof Error ? err.message : 'Failed to load column metadata')
    }
  }

  const reloadAggregations = useCallback(async () => {
    if (!dataset || !countByReady) return
    const shouldUseDbAPI = isDatabaseMode || dataset.database_type === 'connected'
    const dbId = isDatabaseMode ? identifier : dataset.database_name

    for (const table of dataset.tables) {
      const selection = countBySelections[table.name] ?? null
      const cacheKey = getCountByCacheKey(table.name)
      await loadTableAggregations(table.id, table.name, {
        useDbAPI: shouldUseDbAPI,
        dbName: dbId,
        datasetId: dataset.id,
        tableFilters: filters,
        cacheKey,
        selectionOverride: selection
      })
    }
  }, [dataset, countByReady, isDatabaseMode, identifier, filters, countBySelections])

  const ensureAggregationForCacheKey = (tableName: string, cacheKey: string) => {
    const cachedEntry = aggregations[tableName]?.[cacheKey]
    if (isCacheEntryFresh(cachedEntry, currentFiltersKey)) return
    if (!dataset) return

    const shouldUseDbAPI = isDatabaseMode || dataset.database_type === 'connected'
    const dbId = isDatabaseMode ? identifier : dataset.database_name
    const table = dataset.tables.find(t => t.name === tableName)
    if (!table) return

    const selection = cacheKey.startsWith('parent:')
      ? { mode: 'parent' as const, targetTable: cacheKey.slice('parent:'.length) }
      : null
    loadTableAggregations(table.id, table.name, {
      useDbAPI: shouldUseDbAPI,
      dbName: dbId,
      datasetId: dataset.id,
      cacheKey,
      selectionOverride: selection
    })
  }

  const loadDataset = async () => {
    try {
      setLoading(true)
      setError(null)
      setAggregations({})
      setBaselineAggregations({})

      const apiPath = isDatabaseMode ? `/databases/${identifier}` : `/datasets/${identifier}`
      const response = await api.get(apiPath)

      const loadedDataset = response.data.dataset
      setDataset(loadedDataset)
      setBaselineAggregations({})
      onDatasetLoaded()

      const shouldUseDbAPI = isDatabaseMode || loadedDataset.database_type === 'connected'
      const dbId = isDatabaseMode ? identifier : loadedDataset.database_name

      for (const table of loadedDataset.tables) {
        await loadTableAggregations(table.id, table.name, {
          storeBaseline: true,
          useDbAPI: shouldUseDbAPI,
          dbName: dbId,
          datasetId: loadedDataset.id,
          cacheKey: ROW_COUNT_KEY
        })
        await loadColumnMetadata(table.id, table.name, {
          useDbAPI: shouldUseDbAPI,
          dbName: dbId,
          datasetId: loadedDataset.id
        })
      }
    } catch (err) {
      console.error('Failed to load dataset:', err)
      setError(err instanceof Error ? err.message : 'Failed to load dataset')
    } finally {
      setLoading(false)
    }
  }

  // ── Effects ───────────────────────────────────────────────────────

  // Load dataset when countBy is ready
  useEffect(() => {
    if (!countByReady) return
    loadDataset()
  }, [id, database, countByReady])

  // Build ancestor options when dataset changes
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
    })
    return () => { cancelled = true }
  }, [dataset])

  // Normalize countBy selections when ancestor options change (via callback)
  useEffect(() => {
    if (Object.keys(ancestorOptions).length === 0) return
    onAncestorOptionsChanged?.(ancestorOptions)
  }, [ancestorOptions])

  // Reload aggregations when filters change
  useEffect(() => {
    if (dataset && countByReady) {
      reloadAggregations()
    }
  }, [filters, dataset, countByReady])

  // Reload when countBy selections change
  useEffect(() => {
    if (!dataset || !countByReady) return

    const shouldUseDbAPI = isDatabaseMode || dataset.database_type === 'connected'
    const dbId = isDatabaseMode ? identifier : dataset.database_name

    dataset.tables.forEach(table => {
      const previousKey = previousCountByRef.current[table.name]
        ? `parent:${previousCountByRef.current[table.name].targetTable}`
        : 'rows'
      const currentSelection = countBySelections[table.name]
      const currentKey = currentSelection ? `parent:${currentSelection.targetTable}` : 'rows'

      if (previousKey !== currentKey) {
        const cachedEntry = aggregations[table.name]?.[currentKey]
        if (isCacheEntryFresh(cachedEntry, currentFiltersKey)) return
        loadTableAggregations(table.id, table.name, {
          useDbAPI: shouldUseDbAPI,
          dbName: dbId,
          datasetId: dataset.id,
          cacheKey: currentKey
        })
      }
    })

    previousCountByRef.current = countBySelections
  }, [countBySelections, dataset, isDatabaseMode, identifier, aggregations, currentFiltersKey])

  // Ensure aggregations for chart count overrides
  useEffect(() => {
    if (!dataset || !countByReady) return
    Object.entries(chartCountOverrides).forEach(([key, cacheKey]) => {
      const [tableName, columnName] = key.split('.')
      if (tableName && columnName) {
        ensureAggregationForCacheKey(tableName, cacheKey)
      }
    })
  }, [chartCountOverrides, dataset, countByReady])

  // Reload for visible dashboard charts
  useEffect(() => {
    if (!dataset || !countByReady) return
    const shouldUseDbAPI = isDatabaseMode || dataset.database_type === 'connected'
    const dbId = isDatabaseMode ? identifier : dataset.database_name

    dashboardCharts.forEach(chart => {
      const key = getDashboardChartKey(chart)
      if (!visibleDashboardKeys[key]) return
      const table = dataset.tables.find(t => t.name === chart.tableName)
      if (!table) return
      const cacheKey = chart.countByTarget ? `parent:${chart.countByTarget}` : ROW_COUNT_KEY
      const cachedEntry = aggregations[chart.tableName]?.[cacheKey]
      if (isCacheEntryFresh(cachedEntry, currentFiltersKey)) return
      loadTableAggregations(table.id, table.name, {
        useDbAPI: shouldUseDbAPI,
        dbName: dbId,
        datasetId: dataset.id,
        cacheKey,
        selectionOverride: chart.countByTarget ? { mode: 'parent', targetTable: chart.countByTarget } : null
      })
    })
  }, [dashboardCharts, dataset, countByReady, aggregations, isDatabaseMode, identifier, currentFiltersKey, visibleDashboardKeys])

  return {
    dataset,
    loading,
    error,
    columnMetadata,
    aggregations,
    baselineAggregations,
    ancestorOptions,
    isCacheEntryFresh,
    getAggregationsForTable,
    getBaselineAggregation,
    getAggregation,
    getColumnMetadata,
    getDisplayTitle,
    getTableDisplayNameByName,
    getMetricLabels,
    formatMetricPath,
    getTableColor,
    getTableChartCount,
    getAllEffectiveFilters,
    getSurvivalCurve,
    ensureSurvivalCurve,
    findSurvivalStatusColumn,
    ensureAggregationForCacheKey,
  }
}
