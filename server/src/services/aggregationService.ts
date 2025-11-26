import clickhouseClient from '../config/clickhouse.js'

const BASE_TABLE_ALIAS = 'base_table'

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

export interface ColumnAggregation {
  column_name: string
  display_type: string
  normalized_display_type?: string
  total_rows: number
  null_count: number
  unique_count: number

  // For categorical columns
  categories?: CategoryCount[]

  // For numeric columns
  numeric_stats?: NumericStats
  histogram?: HistogramBin[]

  metric_type?: MetricType
  metric_parent_table?: string
  metric_parent_column?: string
  metric_path?: MetricPathSegment[]
}

export interface Filter {
  // Simple filter (leaf node)
  column?: string
  operator?: 'eq' | 'in' | 'gt' | 'lt' | 'gte' | 'lte' | 'between'
    | 'temporal_before' | 'temporal_after' | 'temporal_within' | 'temporal_overlaps' | 'temporal_duration'
  value?: any

  // Logical operators (internal nodes)
  and?: Filter[]
  or?: Filter[]
  not?: Filter

  // Cross-table metadata (optional)
  tableName?: string

  // Temporal-specific fields
  temporal_reference_column?: string
  temporal_reference_table?: string
  temporal_window_days?: number
}

export interface TableRelationship {
  foreign_key: string
  referenced_table: string
  referenced_column: string
  type?: string
}

export interface TableMetadata {
  table_name: string
  clickhouse_table_name: string
  relationships?: TableRelationship[]
}

export type MetricType = 'rows' | 'parent'

/**
 * Configuration describing how a table should aggregate counts.
 * - `rows` (default) counts raw rows
 * - `parent` counts distinct values from an upstream table
 */
export interface CountByConfig {
  mode: MetricType
  target_table?: string
}

interface MetricJoin {
  alias: string
  table: string
  on: string
}

export interface MetricPathSegment {
  from_table: string
  via_column: string
  to_table: string
  referenced_column?: string
}

interface MetricContext {
  type: MetricType
  parentTable?: string
  parentColumn?: string
  joins?: MetricJoin[]
  ancestorExpression?: string
  pathSegments?: MetricPathSegment[]
  aliasByTable?: Record<string, string>
  parentAlias?: string
}

export interface SurvivalCurvePoint {
  time: number
  atRisk: number
  events: number
  censored: number
  survival: number
}

const badRequest = (message: string): Error => {
  const error: any = new Error(message)
  error.status = 400
  return error
}

class AggregationService {
  private columnRef(column: string, alias: string = BASE_TABLE_ALIAS): string {
    return `${alias}.${column}`
  }

  private buildFromClause(qualifiedTableName: string, metricContext: MetricContext): string {
    const joins = metricContext.joins?.map(join => `ANY LEFT JOIN ${join.table} AS ${join.alias} ON ${join.on}`).join('\n') ?? ''
    const base = `${qualifiedTableName} AS ${BASE_TABLE_ALIAS}`
    return joins ? `${base}\n${joins}` : base
  }

  /**
   * Find a relationship path between two tables (supports multi-hop transitive relationships).
   *
   * Uses BFS to find the shortest path between tables through foreign key relationships.
   *
   * @param fromTable - Starting table name
   * @param toTable - Target table name
   * @param allTablesMetadata - Metadata for all tables including relationships
   * @returns Array of relationship steps, or null if no path exists
   *
   * @example
   * // Direct relationship: mutations → samples
   * // Returns: [{ from: 'mutations', to: 'samples', fk: 'sample_id', refCol: 'sample_id' }]
   *
   * // Transitive relationship: mutations → samples → patients
   * // Returns: [
   * //   { from: 'mutations', to: 'samples', fk: 'sample_id', refCol: 'sample_id' },
   * //   { from: 'samples', to: 'patients', fk: 'patient_id', refCol: 'patient_id' }
   * // ]
   */
  private findRelationshipPath(
    fromTable: string,
    toTable: string,
    allTablesMetadata: TableMetadata[]
  ): Array<{ from: string; to: string; fk: string; refCol: string; direction: 'forward' | 'backward' }> | null {
    if (fromTable === toTable) return null

    // BFS to find shortest path
    const queue: Array<{
      table: string
      path: Array<{ from: string; to: string; fk: string; refCol: string; direction: 'forward' | 'backward' }>
    }> = [{ table: fromTable, path: [] }]
    const visited = new Set<string>([fromTable])

    while (queue.length > 0) {
      const { table: currentTable, path } = queue.shift()!

      // Get current table metadata
      const currentTableMeta = allTablesMetadata.find(t => t.table_name === currentTable)
      if (!currentTableMeta) continue

      // Check forward relationships (current table references other tables)
      for (const rel of currentTableMeta.relationships || []) {
        const nextTable = rel.referenced_table
        if (visited.has(nextTable)) continue

        const newPath = [
          ...path,
          {
            from: currentTable,
            to: nextTable,
            fk: rel.foreign_key,
            refCol: rel.referenced_column,
            direction: 'forward' as const
          }
        ]

        if (nextTable === toTable) {
          return newPath
        }

        visited.add(nextTable)
        queue.push({ table: nextTable, path: newPath })
      }

      // Check backward relationships (other tables reference current table)
      for (const otherTableMeta of allTablesMetadata) {
        if (otherTableMeta.table_name === currentTable) continue

        for (const rel of otherTableMeta.relationships || []) {
          if (rel.referenced_table !== currentTable) continue

          const nextTable = otherTableMeta.table_name
          if (visited.has(nextTable)) continue

          const newPath = [
            ...path,
            {
              from: currentTable,
              to: nextTable,
              fk: rel.foreign_key,
              refCol: rel.referenced_column,
              direction: 'backward' as const
            }
          ]

          if (nextTable === toTable) {
            return newPath
          }

          visited.add(nextTable)
          queue.push({ table: nextTable, path: newPath })
        }
      }
    }

    return null // No path found
  }

  /**
   * Build a subquery for cross-table filtering.
   *
   * Generates SQL subqueries to filter a table based on filters from related tables
   * through foreign key relationships. Supports bidirectional and transitive (multi-hop) relationships.
   *
   * @param currentTableName - The table being filtered
   * @param filter - The filter to apply (must have tableName property for cross-table)
   * @param allTablesMetadata - Metadata for all tables including relationships
   * @returns SQL subquery string or null if not a cross-table filter or no relationship exists
   *
   * @example
   * // Direct: Filtering samples by patient attributes:
   * // WHERE samples.patient_id IN (SELECT patient_id FROM patients WHERE radiation_therapy = 'Yes')
   *
   * // Transitive: Filtering mutations by patient attributes (mutations → samples → patients):
   * // WHERE mutations.sample_id IN (
   * //   SELECT sample_id FROM samples WHERE patient_id IN (
   * //     SELECT patient_id FROM patients WHERE radiation_therapy = 'Yes'
   * //   )
   * // )
   */
  private buildCrossTableSubquery(
    currentTableName: string,
    filter: Filter,
    allTablesMetadata: TableMetadata[]
  ): string | null {
    // Unwrap NOT to access the actual filter's tableName
    const hasNot = !!(filter as any).not
    const actualFilter = hasNot ? (filter as any).not : filter

    const filterTableName = actualFilter.tableName
    if (!filterTableName || filterTableName === currentTableName) {
      return null // Not a cross-table filter
    }

    // Find the filter's table metadata
    const filterTable = allTablesMetadata.find(t => t.table_name === filterTableName)
    if (!filterTable) {
      console.warn(`Cross-table filter references unknown table: ${filterTableName}`)
      return null
    }

    // Find relationship path (supports transitive/multi-hop relationships)
    const path = this.findRelationshipPath(currentTableName, filterTableName, allTablesMetadata)
    if (!path || path.length === 0) {
      console.warn(`No relationship path found between ${currentTableName} and ${filterTableName}`)
      return null
    }

    // Build the filter condition for the target table (use unwrapped filter)
    const filterCondition = this.buildFilterCondition(actualFilter, null)
    if (!filterCondition) return null

    // Build nested IN subqueries for ClickHouse (no JOINs, better performance)
    // Example path: mutations → samples → patients
    // Build: mutations.sample_id IN (
    //          SELECT sample_id FROM samples WHERE patient_id IN (
    //            SELECT patient_id FROM patients WHERE condition
    //          )
    //        )

    // Start with the innermost query (filter table) and wrap outward
    const lastStep = path[path.length - 1]
    const lastTable = allTablesMetadata.find(t => t.table_name === lastStep.to)
    if (!lastTable) return null

    const qualifiedLastTable = this.qualifyTableName(lastTable.clickhouse_table_name)

    // Innermost: SELECT key FROM filter_table WHERE condition
    let subquery = lastStep.direction === 'forward'
      ? `SELECT ${lastStep.refCol} FROM ${qualifiedLastTable} WHERE ${filterCondition}`
      : `SELECT ${lastStep.fk} FROM ${qualifiedLastTable} WHERE ${filterCondition}`

    // Work backwards through the path, wrapping each level
    for (let i = path.length - 2; i >= 0; i--) {
      const step = path[i]
      const nextStep = path[i + 1]
      const stepTable = allTablesMetadata.find(t => t.table_name === step.to)
      if (!stepTable) return null

      const qualifiedTable = this.qualifyTableName(stepTable.clickhouse_table_name)

      // Determine SELECT column (what current table needs)
      const selectCol = step.direction === 'forward' ? step.refCol : step.fk

      // Determine WHERE column (what links to next table)
      const whereCol = nextStep.direction === 'forward' ? nextStep.fk : nextStep.refCol

      subquery = `SELECT ${selectCol} FROM ${qualifiedTable} WHERE ${whereCol} IN (${subquery})`
    }

    // Final wrap: current_table.column IN (subquery) or NOT IN if filter was negated
    const firstStep = path[0]
    const finalColumn = firstStep.direction === 'forward' ? firstStep.fk : firstStep.refCol
    const columnRef = this.columnRef(finalColumn)
    const operator = hasNot ? 'NOT IN' : 'IN'

    // When using NOT IN, add NULL guard to preserve orphaned rows
    // (NULL NOT IN (...) evaluates to UNKNOWN, which filters out rows incorrectly)
    if (hasNot) {
      subquery = `(${columnRef} ${operator} (${subquery}) OR ${columnRef} IS NULL)`
    } else {
      subquery = `${columnRef} ${operator} (${subquery})`
    }

    return subquery
  }

  /**
   * Ensure numeric filter values are safe for interpolation
   */
  private ensureNumeric(value: unknown, operator: string): number {
    if (typeof value === 'number') {
      if (Number.isFinite(value)) {
        return value
      }
      throw new Error(`Invalid numeric value provided for ${operator} filter`)
    }

    if (typeof value === 'string') {
      const trimmed = value.trim()
      if (trimmed.length === 0) {
        throw new Error(`Invalid numeric value provided for ${operator} filter`)
      }
      const parsed = Number(trimmed)
      if (Number.isFinite(parsed)) {
        return parsed
      }
    }

    throw new Error(`Invalid numeric value provided for ${operator} filter`)
  }

  /**
   * Get all column names for a table
   */
  private async getTableColumns(clickhouseTableName: string): Promise<Set<string>> {
    try {
      const { database, table } = this.parseTableIdentifier(clickhouseTableName)
      const result = await clickhouseClient.query({
        query: `
          SELECT name
          FROM system.columns
          WHERE database = {database:String}
            AND table = {table:String}
        `,
        query_params: { database, table },
        format: 'JSONEachRow'
      })
      const columns = await result.json<{ name: string }>()
      return new Set(columns.map(c => c.name))
    } catch (error) {
      console.error('Error getting table columns:', error)
      return new Set()
    }
  }

  /**
   * Filter out columns that don't exist in the table
   */
  private filterExistingColumns(filter: Filter, validColumns: Set<string>): Filter | null {
    // Handle logical operators recursively
    if (filter.and && Array.isArray(filter.and)) {
      const filtered = filter.and
        .map(f => this.filterExistingColumns(f, validColumns))
        .filter(f => f !== null) as Filter[]
      if (filtered.length === 0) return null
      if (filtered.length === 1) return filtered[0]
      return { and: filtered }
    }

    if (filter.or && Array.isArray(filter.or)) {
      const filtered = filter.or
        .map(f => this.filterExistingColumns(f, validColumns))
        .filter(f => f !== null) as Filter[]
      if (filtered.length === 0) return null
      if (filtered.length === 1) return filtered[0]
      return { or: filtered }
    }

    if (filter.not) {
      const filtered = this.filterExistingColumns(filter.not, validColumns)
      if (!filtered) return null
      return { not: filtered }
    }

    // Handle simple filter - check if column exists
    if (filter.column) {
      if (!validColumns.has(filter.column)) {
        return null // Skip this filter
      }
    }

    return filter
  }

  /**
   * Build exclusion subquery for parent-table counting with NOT filters.
   *
   * When counting by a parent table with a NOT filter on a child column, we need to exclude
   * parents where ANY child matches the positive condition (not just filter row-by-row).
   *
   * Example: Samples filtered by NOT(sample_type = Primary), counting by Patients
   * Returns: parent_fk NOT IN (SELECT parent_fk FROM samples WHERE sample_type = 'Primary')
   *
   * This ensures parents are excluded if they have ANY child matching the condition.
   */
  private buildParentExclusionSubquery(
    filter: Filter,
    currentTableName: string,
    metricContext: MetricContext,
    currentTableClickhouseName: string
  ): string | null {
    // Extract the positive filter from NOT wrapper
    const positiveFilter = (filter as any).not
    if (!positiveFilter) return null

    // Build the positive condition
    const condition = this.buildFilterCondition(positiveFilter, BASE_TABLE_ALIAS)
    if (!condition) return null

    // Get the parent foreign key column
    // metricContext.joins[0] should have the foreign key from child to parent
    const parentFk = metricContext.joins?.[0]?.foreignKey
    if (!parentFk) return null

    const qualifiedTableName = this.qualifyTableName(currentTableClickhouseName)
    const columnRef = this.columnRef(parentFk, BASE_TABLE_ALIAS)

    // Build exclusion subquery
    // Example: parent_id NOT IN (SELECT parent_id FROM samples WHERE sample_type = 'Primary')
    return `${columnRef} NOT IN (SELECT ${parentFk} FROM ${qualifiedTableName} WHERE ${condition})`
  }

  /**
   * Build WHERE clause from filters.
   *
   * Supports logical operators (AND, OR, NOT) and cross-table filtering through
   * foreign key relationships. Filters can target columns in related tables.
   *
   * @param filters - Filter or array of filters to apply
   * @param validColumns - Set of valid column names for the current table (optional)
   * @param currentTableName - Name of the table being filtered (required for cross-table)
   * @param allTablesMetadata - Metadata for all tables with relationships (required for cross-table)
   * @param tableAliasResolver - Function to resolve table names to SQL aliases (for parent counting)
   * @param metricContext - Context for parent-table counting (to detect NOT filter semantics)
   * @param currentTableClickhouseName - ClickHouse table name (for parent exclusion subqueries)
   * @returns SQL WHERE clause string (includes 'AND' prefix) or empty string
   */
  private buildWhereClause(
    filters: Filter[] | Filter,
    validColumns?: Set<string>,
    currentTableName?: string,
    allTablesMetadata?: TableMetadata[],
    tableAliasResolver?: (tableName?: string) => string | undefined,
    metricContext?: MetricContext,
    currentTableClickhouseName?: string
  ): string {
    if (!filters) {
      return ''
    }

    const filterArray = Array.isArray(filters) ? filters : [filters]
    const localFilters: Filter[] = []
    const crossTableConditions: string[] = []
    const aliasFilters: Array<{ filter: Filter; alias: string }> = []

    // Separate local filters from cross-table filters
    for (const filter of filterArray) {
      const targetTable = this.getFilterTableName(filter)
      const aliasOverride = targetTable ? tableAliasResolver?.(targetTable) : undefined
      const isCrossTable = !aliasOverride && targetTable && currentTableName && allTablesMetadata && targetTable !== currentTableName

      // Special handling for NOT filters with parent-table counting
      // When counting by parent with a NOT filter on a local (child) column,
      // we need parent-level exclusion semantics, not row-level NOT
      const hasNot = !!(filter as any).not
      const isParentCounting = metricContext?.type === 'parent'
      const isLocalFilter = aliasOverride === BASE_TABLE_ALIAS || (!aliasOverride && !isCrossTable)

      if (hasNot && isParentCounting && isLocalFilter && metricContext && currentTableName && currentTableClickhouseName) {
        // Use parent-level exclusion: exclude parents where ANY child matches positive condition
        const subquery = this.buildParentExclusionSubquery(filter, currentTableName, metricContext, currentTableClickhouseName)
        if (subquery) {
          crossTableConditions.push(subquery)
        }
      } else if (aliasOverride && targetTable && aliasOverride !== BASE_TABLE_ALIAS) {
        aliasFilters.push({ filter, alias: aliasOverride })
      } else if (isCrossTable) {
        // This is a cross-table filter - build subquery
        const subquery = this.buildCrossTableSubquery(
          currentTableName,
          filter,
          allTablesMetadata!
        )
        if (subquery) {
          crossTableConditions.push(subquery)
        }
      } else {
        // Local filter
        localFilters.push(filter)
      }
    }

    // Filter out columns that don't exist in the table (for local filters only)
    let filteredLocalFilters = localFilters
    if (validColumns && localFilters.length > 0) {
      const filtered = localFilters
        .map(f => this.filterExistingColumns(f, validColumns))
        .filter(f => f !== null) as Filter[]
      filteredLocalFilters = filtered
    }

    // Build local filter condition
    let localCondition = ''
    if (filteredLocalFilters.length > 0) {
      const filterTree: Filter = filteredLocalFilters.length === 1 ? filteredLocalFilters[0] : { and: filteredLocalFilters }
      localCondition = this.buildFilterCondition(filterTree)
    }

    const aliasConditions = aliasFilters
      .map(({ filter, alias }) => this.buildFilterCondition(filter, alias))
      .filter(condition => condition !== '')

    // Combine local and cross-table conditions
    const allConditions = []
    if (localCondition) allConditions.push(localCondition)
    allConditions.push(...aliasConditions)
    allConditions.push(...crossTableConditions)

    if (allConditions.length === 0) return ''
    return `AND (${allConditions.join(' AND ')})`
  }

  /**
   * Recursively build filter condition from filter tree
   */
  private buildFilterCondition(filter: Filter, alias?: string | null): string {
    // Handle logical operators
    if (filter.and && Array.isArray(filter.and)) {
      const conditions = filter.and
        .map(f => this.buildFilterCondition(f, alias))
        .filter(c => c !== '')
      if (conditions.length === 0) return ''
      if (conditions.length === 1) return conditions[0]
      return `(${conditions.join(' AND ')})`
    }

    if (filter.or && Array.isArray(filter.or)) {
      const conditions = filter.or
        .map(f => this.buildFilterCondition(f, alias))
        .filter(c => c !== '')
      if (conditions.length === 0) return ''
      if (conditions.length === 1) return conditions[0]
      return `(${conditions.join(' OR ')})`
    }

    if (filter.not) {
      const condition = this.buildFilterCondition(filter.not, alias)
      if (!condition) return ''
      return `NOT (${condition})`
    }

    // Handle simple filter (leaf node)
    if (!filter.column || !filter.operator) {
      return ''
    }

    const col =
      alias === null
        ? filter.column
        : this.columnRef(filter.column, alias ?? BASE_TABLE_ALIAS)

    switch (filter.operator) {
      case 'eq':
        // Handle empty string case
        if (filter.value === '(Empty)' || filter.value === '') {
          return `(${col} = '' OR isNull(${col}))`
        }
        if (filter.value === '(N/A)') {
          return `${col} = 'N/A'`
        }
        if (filter.value === null) {
          return `isNull(${col})`
        }
        if (typeof filter.value === 'number') {
          if (!Number.isFinite(filter.value)) {
            throw new Error('Invalid value provided for eq filter')
          }
          return `${col} = ${filter.value}`
        }
        if (typeof filter.value === 'string') {
          return `${col} = '${filter.value.replace(/'/g, "''")}'`
        }
        throw new Error('Invalid value provided for eq filter')

      case 'in':
        const values = Array.isArray(filter.value) ? filter.value : [filter.value]
        let includesEmpty = false
        let includesNull = false
        const inValues = values
          .map(v => {
            if (v === '(Empty)' || v === '') {
              includesEmpty = true
              return null
            }
            if (v === '(N/A)') {
              return `'N/A'`
            }
            if (v === null) {
              includesNull = true
              return null
            }
            if (typeof v === 'number') {
              if (!Number.isFinite(v)) {
                throw new Error('Invalid numeric value provided for in filter')
              }
              return `${v}`
            }
            if (typeof v === 'string') {
              return `'${v.replace(/'/g, "''")}'`
            }
            throw new Error('Invalid value provided for in filter')
          })
          .filter((item): item is string => item !== null)
          .join(', ')

        const conditions: string[] = []
        if (inValues.length > 0) {
          conditions.push(`${col} IN (${inValues})`)
        }
        if (includesEmpty) {
          conditions.push(`${col} = ''`)
          conditions.push(`isNull(${col})`)
        } else if (includesNull) {
          conditions.push(`isNull(${col})`)
        }

        if (conditions.length === 0) {
          // No valid values provided; return a condition that always fails
          return '0'
        }

        if (conditions.length === 1) {
          return conditions[0]
        }

        return `(${conditions.join(' OR ')})`

      case 'gt':
        return `${col} > ${this.ensureNumeric(filter.value, 'gt')}`

      case 'lt':
        return `${col} < ${this.ensureNumeric(filter.value, 'lt')}`

      case 'gte':
        return `${col} >= ${this.ensureNumeric(filter.value, 'gte')}`

      case 'lte':
        return `${col} <= ${this.ensureNumeric(filter.value, 'lte')}`

      case 'between':
        if (!Array.isArray(filter.value) || filter.value.length !== 2) {
          throw new Error('Between filter requires an array with exactly two values')
        }
        const [start, end] = filter.value.map(v => this.ensureNumeric(v, 'between'))
        return `${col} BETWEEN ${start} AND ${end}`

      case 'temporal_before':
        return this.buildTemporalBeforeCondition(filter, alias)

      case 'temporal_after':
        return this.buildTemporalAfterCondition(filter, alias)

      case 'temporal_duration':
        return this.buildTemporalDurationCondition(filter, alias)

      case 'temporal_within':
        // To be implemented in Phase 4
        throw new Error('temporal_within operator not yet implemented')

      case 'temporal_overlaps':
        // To be implemented in Phase 4
        throw new Error('temporal_overlaps operator not yet implemented')

      default:
        return ''
    }
  }

  /**
   * Build SQL condition for temporal_before operator
   * Event A occurs before event B starts
   */
  private buildTemporalBeforeCondition(filter: Filter, alias?: string | null): string {
    if (!filter.column || !filter.temporal_reference_column) {
      throw new Error('temporal_before requires column and temporal_reference_column')
    }

    const thisCol =
      alias === null
        ? filter.column
        : this.columnRef(filter.column, alias ?? BASE_TABLE_ALIAS)
    const refCol = filter.temporal_reference_column

    // NULL handling: exclude rows with NULL temporal columns
    return `(${thisCol} IS NOT NULL AND ${thisCol} < ${refCol})`
  }

  /**
   * Build SQL condition for temporal_after operator
   * Event A occurs after event B ends
   */
  private buildTemporalAfterCondition(filter: Filter, alias?: string | null): string {
    if (!filter.column || !filter.temporal_reference_column) {
      throw new Error('temporal_after requires column and temporal_reference_column')
    }

    const thisCol =
      alias === null
        ? filter.column
        : this.columnRef(filter.column, alias ?? BASE_TABLE_ALIAS)
    const refCol = filter.temporal_reference_column

    // NULL handling: exclude rows with NULL temporal columns
    return `(${thisCol} IS NOT NULL AND ${thisCol} > ${refCol})`
  }

  /**
   * Build SQL condition for temporal_duration operator
   * Event duration meets threshold (stop - start >= value)
   */
  private buildTemporalDurationCondition(filter: Filter, alias?: string | null): string {
    if (!filter.column || !filter.temporal_reference_column || filter.value === undefined) {
      throw new Error('temporal_duration requires column (start), temporal_reference_column (stop), and value (threshold)')
    }

    const startCol =
      alias === null
        ? filter.column
        : this.columnRef(filter.column, alias ?? BASE_TABLE_ALIAS)
    const stopCol =
      alias === null
        ? filter.temporal_reference_column
        : this.columnRef(filter.temporal_reference_column, alias ?? BASE_TABLE_ALIAS)

    const threshold = this.ensureNumeric(filter.value, 'temporal_duration')

    // NULL handling: exclude rows with NULL temporal columns
    return `(${startCol} IS NOT NULL AND ${stopCol} IS NOT NULL AND (${stopCol} - ${startCol}) >= ${threshold})`
  }

  private getFilterTableName(filter: Filter): string | undefined {
    if ((filter as any).tableName) {
      return (filter as any).tableName
    }
    if (filter.not) {
      return this.getFilterTableName(filter.not)
    }
    return undefined
  }

  /**
   * Get aggregated data for a column based on its display type
   */
  async getColumnAggregation(
    datasetId: string,
    tableId: string,
    columnName: string,
    displayType: string,
    filters: Filter[] | Filter = [],
    currentTableName?: string,
    allTablesMetadata?: TableMetadata[],
    countBy?: CountByConfig
  ): Promise<ColumnAggregation> {
    // Get the ClickHouse table name
    const tableResult = await clickhouseClient.query({
      query: `
        SELECT table_name, clickhouse_table_name, row_count
        FROM biai.dataset_tables
        WHERE dataset_id = {datasetId:String}
          AND table_id = {tableId:String}
        LIMIT 1
      `,
      query_params: { datasetId, tableId },
      format: 'JSONEachRow'
    })

    const tables = await tableResult.json<{ table_name: string; clickhouse_table_name: string; row_count: number }>()
    if (tables.length === 0) {
      throw new Error('Table not found')
    }

    const clickhouseTableName = tables[0].clickhouse_table_name
    const qualifiedTableName = this.qualifyTableName(clickhouseTableName)
    const totalRows = tables[0].row_count
    let effectiveTableName = currentTableName || tables[0].table_name
    let tableMetadata = allTablesMetadata

    if (countBy && countBy.mode === 'parent' && (!effectiveTableName || !tableMetadata)) {
      const { metadata, idToNameMap } = await this.loadDatasetTablesMetadata(datasetId)
      tableMetadata = metadata
      if (!effectiveTableName) {
        effectiveTableName = idToNameMap.get(tableId) || tables[0].table_name
      }
    }

    const metricContext = this.resolveMetricContext(effectiveTableName, countBy, tableMetadata)

    // Get valid columns for this table
    const validColumns = await this.getTableColumns(clickhouseTableName)
    const aliasResolver = metricContext.aliasByTable
      ? (tableName?: string) => {
          if (!tableName) return undefined
          return metricContext.aliasByTable?.[tableName]
        }
      : undefined
    const whereClause = this.buildWhereClause(filters, validColumns, effectiveTableName, tableMetadata, aliasResolver, metricContext, clickhouseTableName)

    const fromClause = this.buildFromClause(qualifiedTableName, metricContext)
    const metricAggregation = this.getMetricAggregationExpression(metricContext)

    // Get filtered count for the selected metric
    let filteredTotalRows = totalRows
    const hasFilters = Array.isArray(filters) ? filters.length > 0 : (filters && Object.keys(filters).length > 0)
    const needsMetricCountQuery = metricContext.type === 'parent' || (hasFilters && whereClause)
    if (needsMetricCountQuery) {
      const countQuery = `
        SELECT ${metricAggregation} AS filtered_count
        FROM ${fromClause}
        WHERE 1=1 ${whereClause}
      `
      const countResult = await clickhouseClient.query({
        query: countQuery,
        format: 'JSONEachRow'
      })
      const countData = await countResult.json<{ filtered_count: number }>()
      const [countRow] = countData
      filteredTotalRows = countRow?.filtered_count ?? 0
    }

    // Get basic stats (null count, unique count)
    // Use uniq() instead of uniqExact() for memory efficiency on high-cardinality columns
    const nullCountExpression = this.getMetricAggregationExpression(metricContext, `isNull(${this.columnRef(columnName)})`)
    const basicStatsQuery = `
      SELECT
        ${nullCountExpression} AS null_count,
        uniq(${this.columnRef(columnName)}) AS unique_count
      FROM ${fromClause}
      WHERE 1=1 ${whereClause}
    `

    const basicStatsResult = await clickhouseClient.query({
      query: basicStatsQuery,
      format: 'JSONEachRow'
    })

    const basicStats = await basicStatsResult.json<{ null_count: number; unique_count: number }>()
    const { null_count, unique_count } = basicStats[0]

    const effectiveDisplayType =
      displayType === 'survival_time'
        ? 'numeric'
        : displayType === 'survival_status'
          ? 'categorical'
          : displayType

    const aggregation: ColumnAggregation = {
      column_name: columnName,
      display_type: displayType,
      normalized_display_type: effectiveDisplayType,
      total_rows: filteredTotalRows,
      null_count,
      unique_count,
      metric_type: metricContext.type,
      metric_parent_table: metricContext.parentTable,
      metric_parent_column: metricContext.parentColumn,
      metric_path: metricContext.pathSegments
    }

    // Get aggregation based on display type
    if (effectiveDisplayType === 'categorical' || effectiveDisplayType === 'id' || effectiveDisplayType === 'geographic') {
      // Use higher limit for geographic columns (to include all US states/territories)
      const categoryLimit = effectiveDisplayType === 'geographic' ? 100 : 50
      aggregation.categories = await this.getCategoricalAggregation(
        qualifiedTableName,
        columnName,
        filteredTotalRows,
        categoryLimit,
        whereClause,
        metricContext
      )
    } else if (effectiveDisplayType === 'numeric') {
      aggregation.numeric_stats = await this.getNumericStats(
        qualifiedTableName,
        columnName,
        whereClause,
        metricContext
      )
      aggregation.histogram = await this.getHistogram(
        qualifiedTableName,
        columnName,
        20,
        whereClause,
        metricContext,
        filteredTotalRows
      )
    }

    return aggregation
  }

  /**
   * Get category counts for categorical columns
   */
  private async getCategoricalAggregation(
    qualifiedTableName: string,
    columnName: string,
    totalRows: number,
    limit: number = 50,
    whereClause: string = '',
    metricContext: MetricContext
  ): Promise<CategoryCount[]> {
    const metricAggregation = this.getMetricAggregationExpression(metricContext)
    const columnExpr = this.columnRef(columnName)
    const fromClause = this.buildFromClause(qualifiedTableName, metricContext)
    const query = `
      SELECT
        multiIf(
          isNull(${columnExpr}) OR lengthUTF8(trimBoth(toString(${columnExpr}))) = 0, '',
          lowerUTF8(trimBoth(toString(${columnExpr}))) = 'n/a', 'N/A',
          trimBoth(toString(${columnExpr}))
        ) AS value,
        multiIf(
          isNull(${columnExpr}) OR lengthUTF8(trimBoth(toString(${columnExpr}))) = 0, '(Empty)',
          lowerUTF8(trimBoth(toString(${columnExpr}))) = 'n/a', '(N/A)',
          trimBoth(toString(${columnExpr}))
        ) AS display_value,
        ${metricAggregation} AS count,
        if(${totalRows} = 0, 0, ${metricAggregation} * 100.0 / ${totalRows}) AS percentage
      FROM ${fromClause}
      WHERE 1=1
        ${whereClause}
      GROUP BY value, display_value
      ORDER BY count DESC
      LIMIT ${limit}
    `

    const result = await clickhouseClient.query({
      query,
      format: 'JSONEachRow'
    })

    return await result.json<CategoryCount>()
  }

  /**
   * Get numeric statistics for numeric columns
   */
  private async getNumericStats(
    qualifiedTableName: string,
    columnName: string,
    whereClause: string = '',
    metricContext: MetricContext
  ): Promise<NumericStats> {
    const columnExpr = this.columnRef(columnName)
    const fromClause = this.buildFromClause(qualifiedTableName, metricContext)
    const query = `
      SELECT
        min(${columnExpr}) AS min,
        max(${columnExpr}) AS max,
        avg(${columnExpr}) AS mean,
        median(${columnExpr}) AS median,
        stddevPop(${columnExpr}) AS stddev,
        quantile(0.25)(${columnExpr}) AS q25,
        quantile(0.75)(${columnExpr}) AS q75
      FROM ${fromClause}
      WHERE ${columnExpr} IS NOT NULL
        ${whereClause}
    `

    const result = await clickhouseClient.query({
      query,
      format: 'JSONEachRow'
    })

    const stats = await result.json<NumericStats>()
    return stats[0]
  }

  /**
   * Get histogram data for numeric columns
   */
  private async getHistogram(
    qualifiedTableName: string,
    columnName: string,
    bins: number = 20,
    whereClause: string = '',
    metricContext: MetricContext,
    totalMetricCount: number
  ): Promise<HistogramBin[]> {
    // First get min and max to calculate bin width
    const columnExpr = this.columnRef(columnName)
    const fromClause = this.buildFromClause(qualifiedTableName, metricContext)
    const minMaxQuery = `
      SELECT
        min(${columnExpr}) AS min_val,
        max(${columnExpr}) AS max_val
      FROM ${fromClause}
      WHERE ${columnExpr} IS NOT NULL
        ${whereClause}
    `

    const minMaxResult = await clickhouseClient.query({
      query: minMaxQuery,
      format: 'JSONEachRow'
    })

    const minMaxData = await minMaxResult.json<{ min_val: number | null; max_val: number | null }>()
    if (minMaxData.length === 0) {
      return []
    }

    const { min_val, max_val } = minMaxData[0]
    if (min_val === null || max_val === null) {
      return []
    }

    if (min_val === max_val) {
      // All values are the same
      return [{
        bin_start: min_val,
        bin_end: min_val,
        count: totalMetricCount,
        percentage: 100
      }]
    }

    const binWidth = (max_val - min_val) / bins

    // Use ClickHouse's histogram function or manual binning
    const metricAggregation = this.getMetricAggregationExpression(metricContext)
    const histogramQuery = `
      SELECT
        floor((${columnExpr} - ${min_val}) / ${binWidth}) AS bin_index,
        ${min_val} + floor((${columnExpr} - ${min_val}) / ${binWidth}) * ${binWidth} AS bin_start,
        ${min_val} + (floor((${columnExpr} - ${min_val}) / ${binWidth}) + 1) * ${binWidth} AS bin_end,
        ${metricAggregation} AS count
      FROM ${fromClause}
      WHERE ${columnExpr} IS NOT NULL
        ${whereClause}
      GROUP BY bin_index, bin_start, bin_end
      ORDER BY bin_index
    `

    const result = await clickhouseClient.query({
      query: histogramQuery,
      format: 'JSONEachRow'
    })

    const histogram = await result.json<{ bin_start: number; bin_end: number; count: number }>()
    return histogram.map(bin => ({
      bin_start: bin.bin_start,
      bin_end: bin.bin_end,
      count: bin.count,
      percentage: totalMetricCount > 0 ? (bin.count / totalMetricCount) * 100 : 0
    }))
  }

  private qualifyTableName(tableName: string): string {
    return tableName.includes('.') ? tableName : `biai.${tableName}`
  }

  private async loadDatasetTablesMetadata(datasetId: string): Promise<{
    metadata: TableMetadata[]
    idToNameMap: Map<string, string>
  }> {
    const tablesResult = await clickhouseClient.query({
      query: `
        SELECT table_id, table_name, clickhouse_table_name
        FROM biai.dataset_tables
        WHERE dataset_id = {datasetId:String}
      `,
      query_params: { datasetId },
      format: 'JSONEachRow'
    })

    const tablesData = await tablesResult.json<{ table_id: string; table_name: string; clickhouse_table_name: string }>()

    const relationshipsResult = await clickhouseClient.query({
      query: `
        SELECT
          table_id,
          foreign_key,
          referenced_table,
          referenced_column,
          relationship_type
        FROM biai.table_relationships
        WHERE dataset_id = {datasetId:String}
      `,
      query_params: { datasetId },
      format: 'JSONEachRow'
    })

    const relationshipsData = await relationshipsResult.json<{
      table_id: string
      foreign_key: string
      referenced_table: string
      referenced_column: string
      relationship_type: string
    }>()

    const idToNameMap = new Map(tablesData.map(t => [t.table_id, t.table_name]))

    const metadata: TableMetadata[] = tablesData.map(table => {
      const tableRelationships = relationshipsData
        .filter(rel => rel.table_id === table.table_id)
        .map(rel => ({
          foreign_key: rel.foreign_key,
          referenced_table: rel.referenced_table,
          referenced_column: rel.referenced_column,
          type: rel.relationship_type
        }))

      return {
        table_name: table.table_name,
        clickhouse_table_name: table.clickhouse_table_name,
        relationships: tableRelationships
      }
    })

    return { metadata, idToNameMap }
  }

  private resolveMetricContext(
    currentTableName: string | undefined,
    countBy: CountByConfig | undefined,
    allTablesMetadata: TableMetadata[] | undefined
  ): MetricContext {
    if (!countBy || countBy.mode === 'rows' || !currentTableName) {
      return { type: 'rows' }
    }

    if (!allTablesMetadata) {
      throw badRequest('countBy requires table metadata')
    }

    if (!countBy.target_table) {
      throw badRequest('countBy target_table is required')
    }

    return this.buildParentMetricContext(currentTableName, countBy.target_table, allTablesMetadata)
  }

  private buildParentMetricContext(
    currentTableName: string,
    targetTable: string,
    allTablesMetadata: TableMetadata[]
  ): MetricContext {
    const path = this.findRelationshipPath(currentTableName, targetTable, allTablesMetadata)
    if (!path || path.length === 0) {
      throw badRequest(`No relationship from ${currentTableName} to ${targetTable}`)
    }

    if (path.some(step => step.direction !== 'forward')) {
      throw badRequest('countBy supports only parent (forward) relationships')
    }

    const tableMap = new Map(allTablesMetadata.map(t => [t.table_name, t]))
    const joins: MetricJoin[] = []
    const pathSegments: MetricPathSegment[] = []
    const aliasByTable = new Map<string, string>([[currentTableName, BASE_TABLE_ALIAS]])

    // Always create joins for each step so alias lookups work even for single-hop relationships
    path.forEach((step, index) => {
      pathSegments.push({
        from_table: step.from,
        via_column: step.fk,
        to_table: step.to,
        referenced_column: step.refCol
      })

      const fromAlias = aliasByTable.get(step.from)
      if (!fromAlias) {
        throw badRequest(`Unable to resolve relationship path for ${step.from}`)
      }

      const toMeta = tableMap.get(step.to)
      if (!toMeta) {
        throw badRequest(`Table metadata not found for ${step.to}`)
      }

      const joinAlias = `ancestor_${index}`
      joins.push({
        alias: joinAlias,
        table: this.qualifyTableName(toMeta.clickhouse_table_name),
        on: `${fromAlias}.${step.fk} = ${joinAlias}.${step.refCol}`
      })
      aliasByTable.set(step.to, joinAlias)
    })

    const lastStep = path[path.length - 1]
    const parentAlias = aliasByTable.get(targetTable)
    if (!parentAlias) {
      throw badRequest(`Unable to resolve alias for ancestor ${targetTable}`)
    }
    // Use the parent table's primary key when counting distinct parents
    // (referenced_column = parent PK, via_column = child FK)
    const ancestorExpression = this.columnRef(lastStep.refCol, parentAlias)

    return {
      type: 'parent',
      parentTable: targetTable,
      parentColumn: lastStep.refCol,
      joins,
      ancestorExpression,
      pathSegments,
      aliasByTable: Object.fromEntries(aliasByTable.entries()),
      parentAlias
    }
  }

  private getMetricAggregationExpression(metricContext: MetricContext, condition?: string): string {
    if (metricContext.type === 'parent') {
      const ancestorExpr = metricContext.ancestorExpression
        ?? (metricContext.parentColumn ? this.columnRef(metricContext.parentColumn) : null)
      if (!ancestorExpr) {
        throw new Error('Parent metric missing ancestor expression')
      }
      if (condition) {
        return `uniqIf(${ancestorExpr}, ${condition})`
      }
      return `uniq(${ancestorExpr})`
    }

    if (condition) {
      return `countIf(${condition})`
    }
    return 'count()'
  }

  private parseTableIdentifier(tableName: string): { database: string; table: string } {
    if (tableName.includes('.')) {
      const [database, table] = tableName.split('.', 2)
      return { database, table }
    }
    return { database: 'biai', table: tableName }
  }

  /**
   * Compute survival curve points (Kaplan–Meier style) for a time/status column pair
   */
  async getSurvivalCurve(
    datasetId: string,
    tableId: string,
    timeColumn: string,
    statusColumn: string,
    filters: Filter[] | Filter = [],
    countBy?: CountByConfig
  ): Promise<SurvivalCurvePoint[]> {
    const tableResult = await clickhouseClient.query({
      query: `
        SELECT table_name, clickhouse_table_name
        FROM biai.dataset_tables
        WHERE dataset_id = {datasetId:String}
          AND table_id = {tableId:String}
        LIMIT 1
      `,
      query_params: { datasetId, tableId },
      format: 'JSONEachRow'
    })

    const tables = await tableResult.json<{ table_name: string; clickhouse_table_name: string }>()
    if (tables.length === 0) {
      throw new Error('Table not found')
    }

    const clickhouseTableName = tables[0].clickhouse_table_name
    const qualifiedTableName = this.qualifyTableName(clickhouseTableName)
    let effectiveTableName = tables[0].table_name
    let tableMetadata: TableMetadata[] | undefined

    if (countBy && countBy.mode === 'parent') {
      const { metadata, idToNameMap } = await this.loadDatasetTablesMetadata(datasetId)
      tableMetadata = metadata
      effectiveTableName = idToNameMap.get(tableId) || effectiveTableName
    }

    const metricContext = this.resolveMetricContext(effectiveTableName, countBy, tableMetadata)
    const validColumns = await this.getTableColumns(clickhouseTableName)
    const aliasResolver = metricContext.aliasByTable
      ? (tableName?: string) => {
          if (!tableName) return undefined
          return metricContext.aliasByTable?.[tableName]
        }
      : undefined
    const whereClause = this.buildWhereClause(filters, validColumns, effectiveTableName, tableMetadata, aliasResolver, metricContext, clickhouseTableName)
    const fromClause = this.buildFromClause(qualifiedTableName, metricContext)

    const timeExpr = `toFloat64(${this.columnRef(timeColumn)})`
    const statusExpr = `lowerUTF8(trimBoth(toString(${this.columnRef(statusColumn)})))`
    const eventExpr = `
      coalesce(
        toInt64OrNull(${this.columnRef(statusColumn)}),
        multiIf(
          ${statusExpr} IN ('1','true','t','yes','y','dead','deceased','death','died','event','progressed','progression','relapse'), 1,
          startsWith(${statusExpr}, '1') OR startsWith(${statusExpr}, 'event') OR position(${statusExpr}, 'deceased') > 0 OR position(${statusExpr}, 'death') > 0 OR position(${statusExpr}, 'dead') > 0, 1,
          ${statusExpr} IN ('0','false','f','no','n','alive','living','censored','censor','none','ongoing'), 0,
          startsWith(${statusExpr}, '0') OR position(${statusExpr}, 'alive') > 0 OR position(${statusExpr}, 'living') > 0 OR position(${statusExpr}, 'censor') > 0, 0,
          null
        )
      )
    `

    const query = `
      SELECT
        time_val,
        sum(event_flag) AS events,
        count() - sum(event_flag) AS censored
      FROM (
        SELECT
          ${timeExpr} AS time_val,
          ${eventExpr} AS event_flag
        FROM ${fromClause}
        WHERE 1=1 ${whereClause}
          AND ${timeExpr} IS NOT NULL
          AND ${eventExpr} IS NOT NULL
      )
      GROUP BY time_val
      ORDER BY time_val
    `

    const result = await clickhouseClient.query({
      query,
      format: 'JSONEachRow'
    })

    const rows = await result.json<{ time_val: number; events: number; censored: number }>()
    if (!rows || rows.length === 0) return []

    // Compute KM-style step survival
    let atRisk = rows.reduce((sum, row) => sum + row.events + row.censored, 0)
    let survival = 1
    const curve: SurvivalCurvePoint[] = []

    for (const row of rows) {
      const atRiskBefore = atRisk
      const events = row.events || 0
      const censored = row.censored || 0

      if (atRiskBefore > 0) {
        const step = events > 0 ? 1 - events / atRiskBefore : 1
        survival = survival * step
      }

      curve.push({
        time: row.time_val,
        atRisk: atRiskBefore,
        events,
        censored,
        survival
      })

      atRisk = atRiskBefore - events - censored
      if (atRisk < 0) atRisk = 0
    }

    return curve
  }

  /**
   * Get aggregations for all visible columns in a table.
   *
   * Loads table metadata including foreign key relationships to support
   * cross-table filtering. Filters from related tables are automatically
   * propagated through relationship chains.
   *
   * @param datasetId - The dataset ID
   * @param tableId - The table ID
   * @param filters - Filters to apply (may include cross-table filters with tableName property)
   * @returns Array of column aggregations
   */
  async getTableAggregations(
    datasetId: string,
    tableId: string,
    filters: Filter[] | Filter = [],
    countBy?: CountByConfig
  ): Promise<ColumnAggregation[]> {
    const { metadata: allTablesMetadata, idToNameMap } = await this.loadDatasetTablesMetadata(datasetId)
    const currentTableName = idToNameMap.get(tableId)
    if (!currentTableName) {
      throw new Error('Table metadata not found')
    }

    // Get column metadata
    const columnsResult = await clickhouseClient.query({
      query: `
        SELECT
          column_name,
          display_type,
          is_hidden
        FROM biai.dataset_columns
        WHERE dataset_id = {datasetId:String}
          AND table_id = {tableId:String}
          AND is_hidden = false
        ORDER BY created_at DESC
      `,
      query_params: { datasetId, tableId },
      format: 'JSONEachRow'
    })

    const columns = await columnsResult.json<{ column_name: string; display_type: string; is_hidden: boolean }>()

    // Get aggregations for each column in parallel
    const aggregations = await Promise.all(
      columns.map(col =>
        this.getColumnAggregation(
          datasetId,
          tableId,
          col.column_name,
          col.display_type,
          filters,
          currentTableName,
          allTablesMetadata,
          countBy
        )
      )
    )

    return aggregations
  }
}

export default new AggregationService()
