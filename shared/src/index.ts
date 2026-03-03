/**
 * Shared TypeScript types for the BIAI project.
 *
 * This package provides the single source of truth for types used by both
 * the client and server packages, eliminating duplication and preventing drift.
 */

// Filter & relationship types
export type { Filter, TableRelationship } from './filters'

// Aggregation result types
export type {
  CategoryCount,
  NumericStats,
  HistogramBin,
  MetricPathSegment,
  MetricType,
  ColumnAggregation,
  SurvivalCurvePoint,
  CountByConfig,
  BivariateDataPoint,
  BivariateAggregation,
} from './aggregation'

// Dashboard types
export type { DashboardChart } from './dashboard'

// Import/spreadsheet types
export type { DetectedRelationship } from './import'
