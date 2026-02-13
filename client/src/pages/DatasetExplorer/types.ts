import type {
  SurvivalCurvePoint,
  ColumnAggregation,
  TableRelationship,
  MetricPathSegment,
} from 'shared'

// Re-export shared types so existing imports from this module continue to work
export type {
  CategoryCount,
  NumericStats,
  HistogramBin,
  SurvivalCurvePoint,
  ColumnAggregation,
  TableRelationship,
  MetricPathSegment,
} from 'shared'

// Small categorical sets render better as pie charts; beyond this use bars.
export const MAX_PIE_CATEGORIES = 8

export const CHART_LABEL_STORAGE_PREFIX = 'chartLabels_'
export const CHART_OVERRIDE_STORAGE_PREFIX = 'chartOverrides_'
export const TABLE_SCOPE_KEY = 'table'
export const DASHBOARD_SCOPE_KEY = 'dashboard'
export const CACHE_TTL_MS = 5 * 60 * 1000 // 5 minutes
export const CACHE_MAX_ENTRIES_PER_TABLE = 5
export const MAX_ANCESTOR_DEPTH = 4

export const chartOverrideStorageKey = (identifier: string) => `${CHART_OVERRIDE_STORAGE_PREFIX}${identifier}`

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

export interface Column {
  name: string
  type: string
  nullable: boolean
}

export interface ColumnMetadata {
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
  is_list_column?: boolean
  list_syntax?: string
}



export interface SavedDashboard {
  id: string
  name: string
  charts: Array<{ tableName: string; columnName: string; addedAt: string }>
  createdAt: string
  updatedAt: string
}

export interface Table {
  id: string
  name: string
  displayName: string
  rowCount: number
  columns: Column[]
  primaryKey?: string
  relationships?: TableRelationship[]
}

export interface Dataset {
  id: string
  name: string
  database_name?: string
  database_type?: 'created' | 'connected'
  description: string
  tags?: string[]
  tables: Table[]
}

export type AncestorOption = {
  targetTable: string
  label: string
  key: string
  path: MetricPathSegment[]
}

export type AggregationCacheEntry = {
  data: ColumnAggregation[]
  filtersKey: string
  timestamp: number
}

export type SurvivalCacheEntry = {
  data: SurvivalCurvePoint[]
  filtersKey: string
  countByKey: string
  statusColumn: string
  timestamp: number
}
