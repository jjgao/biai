import clickhouseClient from '../config/clickhouse.js'
import { looksLikeList, parseListValue, hasNestedLists } from '../utils/listParser.js'

const qualifyTableName = (tableName: string): string =>
  tableName.includes('.') ? tableName : `biai.${tableName}`

export interface ColumnAnalysis {
  display_type: 'categorical' | 'numeric' | 'datetime' | 'survival_time' | 'survival_status' | 'id' | 'text' | 'geographic'
  unique_value_count: number
  null_count: number
  min_value: string | null
  max_value: string | null
  suggested_chart: 'pie' | 'bar' | 'histogram' | 'survival' | 'none' | 'map'
  display_priority: number
  is_hidden: boolean
}

interface ColumnStats {
  unique_count: number
  null_count: number
  total_count: number
  sample_values: any[]
  min_value: any
  max_value: any
}

export async function analyzeColumn(
  tableName: string,
  columnName: string,
  columnType: string
): Promise<ColumnAnalysis> {
  // Get column statistics
  const stats = await getColumnStats(tableName, columnName, columnType)

  // Detect display type
  const displayType = detectDisplayType(columnName, columnType, stats)

  // Suggest chart type
  const suggestedChart = suggestChartType(displayType, stats)

  // Calculate priority (higher = more important)
  const priority = calculatePriority(columnName, displayType, stats)

  // Determine if should be hidden
  const isHidden = shouldHideColumn(displayType, stats)

  return {
    display_type: displayType,
    unique_value_count: stats.unique_count,
    null_count: stats.null_count,
    min_value: stats.min_value ? String(stats.min_value) : null,
    max_value: stats.max_value ? String(stats.max_value) : null,
    suggested_chart: suggestedChart,
    display_priority: priority,
    is_hidden: isHidden
  }
}

async function getColumnStats(
  tableName: string,
  columnName: string,
  columnType: string
): Promise<ColumnStats> {
  // Handle nullable types
  const isNumericType = columnType.includes('Int') || columnType.includes('Float') || columnType.includes('Decimal')
  const qualifiedTableName = qualifyTableName(tableName)

  // Get unique count and null count
  const nullCondition = isNumericType
    ? `isNull(${columnName})`
    : `isNull(${columnName}) OR ${columnName} = ''`

  const countQuery = `
    SELECT
      uniqExact(${columnName}) as unique_count,
      countIf(${nullCondition}) as null_count,
      count() as total_count
    FROM ${qualifiedTableName}
  `

  const countResult = await clickhouseClient.query({
    query: countQuery,
    format: 'JSONEachRow'
  })
  const countData = await countResult.json<{
    unique_count: number
    null_count: number
    total_count: number
  }>()
  const counts =
    countData && countData.length > 0
      ? countData[0]
      : { unique_count: 0, null_count: 0, total_count: 0 }

  // Get sample values (up to 100)
  const whereCondition = isNumericType
    ? `${columnName} IS NOT NULL`
    : `${columnName} IS NOT NULL AND ${columnName} != ''`

  const sampleQuery = `
    SELECT DISTINCT ${columnName}
    FROM ${qualifiedTableName}
    WHERE ${whereCondition}
    LIMIT 100
  `

  const sampleResult = await clickhouseClient.query({
    query: sampleQuery,
    format: 'JSONEachRow'
  })
  const sampleData = await sampleResult.json<Record<string, unknown>>()
  const sampleValues = sampleData.map(row => row[columnName] as any)

  // Get min/max for numeric columns
  let minValue = null
  let maxValue = null

  if (columnType.includes('Int') || columnType.includes('Float')) {
    const minMaxQuery = `
      SELECT
        min(${columnName}) as min_val,
        max(${columnName}) as max_val
      FROM ${qualifiedTableName}
      WHERE ${columnName} IS NOT NULL
    `

    const minMaxResult = await clickhouseClient.query({
      query: minMaxQuery,
      format: 'JSONEachRow'
    })
    const minMaxData = await minMaxResult.json<{ min_val: number | null; max_val: number | null }>()
    if (minMaxData && minMaxData.length > 0 && minMaxData[0]) {
      minValue = minMaxData[0].min_val
      maxValue = minMaxData[0].max_val
    }
  }

  return {
    unique_count: counts.unique_count,
    null_count: counts.null_count,
    total_count: counts.total_count,
    sample_values: sampleValues,
    min_value: minValue,
    max_value: maxValue
  }
}

function detectDisplayType(
  columnName: string,
  columnType: string,
  stats: ColumnStats
): ColumnAnalysis['display_type'] {
  const nameLower = columnName.toLowerCase()

  // ID columns
  if (nameLower.includes('_id') || nameLower === 'id' ||
      nameLower.endsWith('identifier') ||
      stats.unique_count === stats.total_count) {
    return 'id'
  }

  // Survival time/status columns (keep semantic detection)
  const nameTokens = nameLower.split(/[^a-z0-9]+/).filter(Boolean)
  const hasSurvivalToken = nameTokens.some(token => ['survival', 'os', 'pfs', 'dfs', 'dss'].includes(token))
  const looksLikeTime = nameLower.includes('months') || nameLower.includes('days') || nameLower.includes('time')
  if (hasSurvivalToken && looksLikeTime) {
    return 'survival_time'
  }
  if (hasSurvivalToken && nameLower.includes('status')) {
    return 'survival_status'
  }

  // Geographic columns - use exact matches or word boundaries to avoid false positives
  // (e.g., don't match "prostate_status" or "estate_value")
  const geographicPatterns = [
    /^state$/,           // exact: "state"
    /^state_/,           // prefix: "state_code", "state_name"
    /_state$/,           // suffix: "patient_state", "billing_state"
    /^county$/,          // exact: "county"
    /^county_/,          // prefix: "county_name", "county_code"
    /_county$/,          // suffix: "residence_county"
    /^country$/,         // exact: "country"
    /^country_/,         // prefix: "country_code", "country_name"
    /_country$/,         // suffix: "birth_country"
    /^region$/,          // exact: "region"
    /^region_/,          // prefix: "region_name", "region_code"
    /_region$/,          // suffix: "geographic_region"
    /^province$/,        // exact: "province"
    /^province_/,        // prefix: "province_name"
    /_province$/,        // suffix: "birth_province"
    /^territory$/,       // exact: "territory"
    /^territory_/,       // prefix: "territory_name"
    /_territory$/        // suffix: "home_territory"
  ]
  if (geographicPatterns.some(pattern => pattern.test(nameLower))) {
    return 'geographic'
  }

  // Datetime columns
  if (columnType.includes('Date') || nameLower.includes('date') || nameLower.includes('time')) {
    return 'datetime'
  }

  // Numeric columns
  if (columnType.includes('Int') || columnType.includes('Float') || columnType.includes('Decimal')) {
    // Check if it's really numeric by sampling values
    const isNumeric = stats.sample_values.every(v => !isNaN(Number(v)))
    if (isNumeric) {
      return 'numeric'
    }
  }

  // High cardinality categorical (was: text)
  // Keep as categorical instead of text so they remain visible
  if (stats.unique_count > 100) {
    return 'categorical'
  }

  // Default to categorical
  return 'categorical'
}

function suggestChartType(
  displayType: ColumnAnalysis['display_type'],
  stats: ColumnStats
): ColumnAnalysis['suggested_chart'] {
  // Don't chart ID, text, or hidden columns
  if (displayType === 'id' || displayType === 'text') {
    return 'none'
  }

  // Treat survival columns like their base types so they still render
  if (displayType === 'survival_time') {
    return stats.unique_count >= 10 ? 'histogram' : 'none'
  }
  if (displayType === 'survival_status') {
    return 'bar'
  }

  // Geographic columns - render as maps
  if (displayType === 'geographic') {
    return 'map'
  }

  // Datetime - could be timeline, but skip for now
  if (displayType === 'datetime') {
    return 'none'
  }

  // Numeric - histogram
  if (displayType === 'numeric') {
    // Only histogram if there's good variation
    if (stats.unique_count >= 10) {
      return 'histogram'
    }
    return 'none'
  }

  // Categorical
  if (displayType === 'categorical') {
    // Pie chart for few categories (2-8)
    if (stats.unique_count >= 2 && stats.unique_count <= 8) {
      return 'pie'
    }
    // Bar chart for medium categories (9-50), but will default to table in UI
    if (stats.unique_count > 8 && stats.unique_count <= 50) {
      return 'bar'
    }
    // Bar chart for high cardinality too (will default to table in UI)
    // Changed from 'none' to 'bar' so UI can toggle between chart and table
    if (stats.unique_count > 50) {
      return 'bar'
    }
    return 'none'
  }

  return 'none'
}

function calculatePriority(
  columnName: string,
  displayType: ColumnAnalysis['display_type'],
  stats: ColumnStats
): number {
  const nameLower = columnName.toLowerCase()
  let priority = 0

  // Common demographic/clinical fields
  const highPriorityFields = ['sex', 'gender', 'age', 'race', 'ethnicity', 'status', 'type', 'stage', 'grade']
  if (highPriorityFields.some(field => nameLower.includes(field))) {
    priority += 500
  }

  // Categorical with good distribution (not too sparse)
  if (displayType === 'categorical') {
    const nonNullPercent = ((stats.total_count - stats.null_count) / stats.total_count) * 100
    if (nonNullPercent > 50) {
      priority += 200
    }
  }

  // Numeric with variation
  if (displayType === 'numeric' && stats.unique_count >= 10) {
    priority += 100
  }

  // Penalize high null percentage
  const nullPercent = (stats.null_count / stats.total_count) * 100
  if (nullPercent > 80) {
    priority -= 300
  }

  return priority
}

function shouldHideColumn(
  displayType: ColumnAnalysis['display_type'],
  stats: ColumnStats
): boolean {
  // Hide ID columns
  if (displayType === 'id') {
    return true
  }

  // Don't hide text columns anymore - they'll be shown as tables
  // (Removed text column hiding)

  // Hide if mostly null (>90%)
  const nullPercent = (stats.null_count / stats.total_count) * 100
  if (nullPercent > 90) {
    return true
  }

  // Hide if only one unique value
  if (stats.unique_count <= 1) {
    return true
  }

  return false
}

/**
 * Result of list detection for a column
 */
export interface ListDetectionResult {
  columnName: string
  columnIndex: number
  confidence: 'high' | 'medium' | 'low'
  sampleCount: number
  listSyntax: 'python' | 'json' | 'mixed'
  avgItemCount: number
  uniqueItemCount: number
  hasNestedLists: boolean
}

/**
 * Detect columns that likely contain list values.
 * Analyzes sample values to identify list patterns and calculate confidence.
 *
 * @param columns - Array of column metadata
 * @param rows - Array of data rows
 * @param sampleSize - Number of rows to analyze (default: 100)
 * @returns Array of list detection results with high/medium/low confidence
 */
export function detectListColumns(
  columns: { name: string; index: number }[],
  rows: any[][],
  sampleSize: number = 100
): ListDetectionResult[] {
  const results: ListDetectionResult[] = []
  const samplesToAnalyze = Math.min(sampleSize, rows.length)

  if (samplesToAnalyze === 0) {
    return results
  }

  for (const column of columns) {
    const columnIndex = column.index

    // Get sample values for this column
    const sampleValues = rows.slice(0, samplesToAnalyze).map(row => row[columnIndex])

    // Filter out null/empty values
    const nonEmptyValues = sampleValues.filter(
      v => v !== null && v !== undefined && String(v).trim() !== ''
    )

    if (nonEmptyValues.length === 0) {
      continue
    }

    // Count how many values look like lists
    let pythonCount = 0
    let jsonCount = 0
    let listLikeCount = 0
    let hasNested = false
    const allParsedItems: string[] = []

    for (const value of nonEmptyValues) {
      const strValue = String(value)

      if (looksLikeList(strValue)) {
        listLikeCount++

        // Try to parse to determine syntax and collect items
        const result = parseListValue(strValue, 'auto')

        if (result.success) {
          // Detect syntax by checking quotes
          const trimmed = strValue.trim()
          if (trimmed.includes('"')) {
            jsonCount++
          } else {
            pythonCount++
          }

          // Collect items for uniqueness analysis
          allParsedItems.push(...result.items)

          // Check for nested lists
          if (hasNestedLists(strValue)) {
            hasNested = true
          }
        }
      }
    }

    // Calculate confidence based on percentage of list-like values
    const listPercentage = (listLikeCount / nonEmptyValues.length) * 100

    let confidence: 'high' | 'medium' | 'low'
    if (listPercentage >= 90) {
      confidence = 'high'
    } else if (listPercentage >= 70) {
      confidence = 'medium'
    } else if (listPercentage >= 50) {
      confidence = 'low'
    } else {
      // Not enough list-like values, skip
      continue
    }

    // Determine syntax
    let listSyntax: 'python' | 'json' | 'mixed'
    if (jsonCount > pythonCount * 2) {
      listSyntax = 'json'
    } else if (pythonCount > jsonCount * 2) {
      listSyntax = 'python'
    } else if (jsonCount > 0 && pythonCount > 0) {
      listSyntax = 'mixed'
    } else {
      // Default to python if unclear
      listSyntax = 'python'
    }

    // Calculate average items per list
    const avgItemCount = listLikeCount > 0 ? allParsedItems.length / listLikeCount : 0

    // Count unique items
    const uniqueItemCount = new Set(allParsedItems).size

    results.push({
      columnName: column.name,
      columnIndex: column.index,
      confidence,
      sampleCount: nonEmptyValues.length,
      listSyntax,
      avgItemCount: Math.round(avgItemCount * 10) / 10, // Round to 1 decimal
      uniqueItemCount,
      hasNestedLists: hasNested
    })
  }

  return results
}
