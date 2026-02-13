/**
 * Aggregation result types shared between client and server.
 */

export interface CategoryCount {
  value: string
  display_value: string
  count: number
  percentage: number
}

export interface NumericStats {
  min: number
  max: number
  mean: number
  median: number
  stddev: number
  q25: number
  q75: number
}

export interface HistogramBin {
  bin_start: number
  bin_end: number
  count: number
  percentage: number
}

export interface MetricPathSegment {
  from_table: string
  via_column: string
  to_table: string
  referenced_column?: string
}

export type MetricType = 'rows' | 'parent'

export interface ColumnAggregation {
  column_name: string
  display_type: string
  normalized_display_type?: string
  total_rows: number
  null_count: number
  unique_count: number
  categories?: CategoryCount[]
  numeric_stats?: NumericStats
  histogram?: HistogramBin[]
  metric_type?: MetricType
  metric_parent_table?: string
  metric_parent_column?: string
  metric_path?: MetricPathSegment[]
}

export interface SurvivalCurvePoint {
  time: number
  atRisk: number
  events: number
  censored: number
  survival: number
}

/**
 * Configuration describing how a table should aggregate counts.
 * - `rows` (default) counts raw rows
 * - `parent` counts distinct values from an upstream table
 */
export interface CountByConfig {
  mode: MetricType
  target_table?: string
}
